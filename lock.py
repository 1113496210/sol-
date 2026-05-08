# -*- coding: utf-8 -*-
"""
Binance USDT 永续合约 · 融合监控系统（全市场扫描 +2500万确认版）
===================================================================
触发层：全市场山寨合约 3M HH 扫描（~300个币）
确认层：仅 24h 成交额 > 2500万 USDT 的币通过（多因子 + 硬因子门槛）
追踪层：HH 推进（≥4%）+ 回撤失效（5%）
位置分析：3M / 15M / 1H / 日线四层自动判断
通知：Telegram + QQ 邮箱（即时 + 每日 Excel 日报）
"""

import os
import time
import sqlite3
import logging
import smtplib
import requests
import pandas as pd
from threading import Lock
from collections import defaultdict
from datetime import datetime, timedelta, timezone, date
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.header import Header
from email.utils import formataddr

# ========================= 配置区 =========================
BINANCE_API = "https://fapi.binance.com"

# Telegram
TELEGRAM_TOKEN = "8557301222:AAHj1rSQ63zJGFXVxxuTniwRP2Y1tj3QsAs"          # 替换为真实 token
TELEGRAM_CHAT_ID = "5408890841"          # 替换为真实 chat_id

# QQ 邮箱
ENABLE_EMAIL = True
QQ_EMAIL = "1113496210@qq.com"                        # 替换为发件邮箱
QQ_AUTH_CODE = "hzshvazrbnyzfhdf"           # 替换为授权码
EMAIL_TO = "1113496210@qq.com"                  # 替换为收件邮箱
SMTP_HOST = "smtp.qq.com"
SMTP_PORT = 465

# 扫描节奏
SCAN_INTERVAL = 30                     # 30秒扫一轮
SYMBOL_REFRESH_INTERVAL = 3600
REQUEST_TIMEOUT = 10

# 成交额门槛（确认层）
MIN_24H_VOLUME_USDT = 25_000_000    # 2500万 USDT，只有超过这个才推送
ALT_BLACKLIST = ["BTCUSDT", "ETHUSDT"]

# 系统B（触发层）参数
HH_MIN_TOTAL_PCT = 1.2                # 启动结构最小涨幅（%）
DRAWDOWN_FAIL = 0.05                  # 回撤 5% 视为失效
MAX_PUSH = 3                          # 单轮最多推进次数
MAX_DAILY_ROUND = 2                   # 单币每日最多触发轮数
MIN_PUSH_PCT = 0.04                   # 推进至少涨 4% 才推送

# 多因子确认层参数
MIN_FACTORS_TO_ALERT = 3
OI_STRONG = 0.05
VOLUME_MULTIPLIER = 1.5
PRICE_CHANGE_MIN = 0.01
FUNDING_RATE_MAX = 0.001
MAX_1H_CHANGE = 0.08                  # 1h 涨跌幅 > 8% 直接否决

# 全市场噪音过滤
MARKET_NOISE_PCT = 1.2
MARKET_NOISE_RATIO = 0.6

# 去重
DEDUP_HOURS = 2

# 并发
MAX_WORKERS_TRIGGER = 30
MAX_WORKERS_CONFIRM = 15
MAX_CANDIDATES_PER_CYCLE = 30

LOG_FILE = "monitor.log"

# ===================== 日志 + 数据库 ======================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

db_lock = Lock()
db_conn = sqlite3.connect("signals.db", check_same_thread=False)
sqlite3.register_adapter(datetime, lambda dt: dt.isoformat())

with db_conn:
    db_conn.execute("""
        CREATE TABLE IF NOT EXISTS sent_log (
            symbol TEXT,
            direction TEXT,
            sent_at TIMESTAMP
        )
    """)
    db_conn.execute("""
        CREATE TABLE IF NOT EXISTS signals_history (
            symbol TEXT,
            direction TEXT,
            score INTEGER,
            price REAL,
            oi_change REAL,
            price_change REAL,
            volume_ratio REAL,
            funding REAL,
            sent_at TIMESTAMP
        )
    """)

def should_alert(symbol, direction):
    cutoff = datetime.now() - timedelta(hours=DEDUP_HOURS)
    with db_lock:
        cur = db_conn.execute(
            "SELECT 1 FROM sent_log WHERE symbol=? AND direction=? AND sent_at > ?",
            (symbol, direction, cutoff)
        )
        return cur.fetchone() is None

def mark_alerted(symbol, direction):
    with db_lock:
        db_conn.execute(
            "INSERT INTO sent_log VALUES (?,?,?)",
            (symbol, direction, datetime.now())
        )
        db_conn.commit()

def save_signal(sig):
    with db_lock:
        db_conn.execute("""
            INSERT INTO signals_history VALUES (?,?,?,?,?,?,?,?,?)
        """, (
            sig['symbol'], sig['direction'], sig['score'],
            sig['price'], sig['oi_change'], sig['price_change'],
            sig['volume_ratio'], sig['funding'], datetime.now()
        ))
        db_conn.commit()

# ===================== HTTP Session =====================
session = requests.Session()
adapter = HTTPAdapter(pool_connections=30, pool_maxsize=30)
session.mount("https://", adapter)
session.mount("http://", adapter)
_orig = session.request
def _patched(method, url, **kw):
    if 'timeout' not in kw:
        kw['timeout'] = REQUEST_TIMEOUT
    return _orig(method, url, **kw)
session.request = _patched

# ===================== Binance API =====================
# 全局变量：高成交额币种集合（确认层用）
high_volume_symbols = set()

def get_symbols():
    """返回全市场山寨合约列表，同时更新高成交额集合"""
    global high_volume_symbols
    try:
        info = session.get(f"{BINANCE_API}/fapi/v1/exchangeInfo").json()
        all_syms = [
            s['symbol'] for s in info['symbols']
            if s['contractType'] == 'PERPETUAL'
            and s['quoteAsset'] == 'USDT'
            and s['status'] == 'TRADING'
            and s['symbol'] not in ALT_BLACKLIST
        ]

        tickers = session.get(f"{BINANCE_API}/fapi/v1/ticker/24hr").json()
        hv = set()
        for t in tickers:
            try:
                if float(t.get('quoteVolume', 0)) > MIN_24H_VOLUME_USDT:
                    hv.add(t['symbol'])
            except:
                pass
        high_volume_symbols = hv

        logger.info(f"全部永续: {len(all_syms)} → 全部扫描 | 5000万以上: {len(hv)}")
        return all_syms
    except Exception as e:
        logger.error(f"获取币种失败: {e}")
        return []

def get_klines(symbol, interval, limit):
    r = session.get(
        f"{BINANCE_API}/fapi/v1/klines",
        params={"symbol": symbol, "interval": interval, "limit": limit}
    )
    r.raise_for_status()
    return r.json()

def get_oi_history(symbol, period="1h", limit=2):
    r = session.get(
        f"{BINANCE_API}/futures/data/openInterestHist",
        params={"symbol": symbol, "period": period, "limit": limit}
    )
    r.raise_for_status()
    return r.json()

def get_funding_rate(symbol):
    r = session.get(f"{BINANCE_API}/fapi/v1/premiumIndex", params={"symbol": symbol})
    r.raise_for_status()
    return float(r.json()['lastFundingRate'])

# ===================== 通知模块 =====================
def send_tg(text):
    try:
        session.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": text}
        )
    except Exception as e:
        logger.error(f"TG发送失败: {e}")

def send_email_text(subject, content):
    if not ENABLE_EMAIL:
        return
    try:
        msg = MIMEText(content, "plain", "utf-8")
        msg["From"] = formataddr((str(Header("盘面监控", "utf-8")), QQ_EMAIL))
        msg["To"] = EMAIL_TO
        msg["Subject"] = Header(subject, "utf-8").encode()
        server = smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, timeout=REQUEST_TIMEOUT)
        server.login(QQ_EMAIL, QQ_AUTH_CODE)
        server.sendmail(QQ_EMAIL, [e.strip() for e in EMAIL_TO.split(',')], msg.as_string())
        server.quit()
    except Exception as e:
        logger.error(f"邮件发送失败: {e}")

def send_email_attach(subject, body, filepath):
    if not ENABLE_EMAIL:
        return
    try:
        msg = MIMEMultipart()
        msg["From"] = formataddr((str(Header("盘面监控", "utf-8")), QQ_EMAIL))
        msg["To"] = EMAIL_TO
        msg["Subject"] = Header(subject, "utf-8").encode()
        msg.attach(MIMEText(body, "plain", "utf-8"))
        with open(filepath, "rb") as f:
            part = MIMEApplication(f.read(), Name=os.path.basename(filepath))
            part['Content-Disposition'] = f'attachment; filename="{os.path.basename(filepath)}"'
            msg.attach(part)
        server = smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, timeout=REQUEST_TIMEOUT)
        server.login(QQ_EMAIL, QQ_AUTH_CODE)
        server.sendmail(QQ_EMAIL, [e.strip() for e in EMAIL_TO.split(',')], msg.as_string())
        server.quit()
    except Exception as e:
        logger.error(f"日报邮件发送失败: {e}")

def notify_all(text):
    send_tg(text)
    send_email_text("实时监控提示", text)

# ===================== 位置分析（4层K线）=====================
def analyze_position(symbol):
    """3M / 15M / 1H / 日线 四层自动分析"""
    try:
        lines = []

        # ===== 3M 结构确认 =====
        k3m = get_klines(symbol, "3m", 6)
        highs_3m = [float(k[2]) for k in k3m]
        closes_3m = [float(k[4]) for k in k3m]
        opens_3m = [float(k[1]) for k in k3m]

        hh_count = 0
        for i in range(1, len(highs_3m)):
            if highs_3m[i] > highs_3m[i-1]:
                hh_count += 1

        has_wick = False
        last_high = float(k3m[-1][2])
        last_close = float(k3m[-1][4])
        last_open = float(k3m[-1][1])
        body = abs(last_close - last_open)
        upper_wick = last_high - max(last_close, last_open)
        if body > 0 and upper_wick > body * 1.5:
            has_wick = True

        if hh_count >= 4:
            lines.append(f"✅ 3M 结构：{hh_count}/5 根连续新高，HH 结构强")
        elif hh_count >= 2:
            lines.append(f"🟢 3M 结构：{hh_count}/5 根新高，结构成立")
        else:
            lines.append(f"⚠️ 3M 结构：仅 {hh_count}/5 根新高，结构弱")
        if has_wick:
            lines.append(f"🚨 3M 插针：最新K线长上影线，可能是假突破")

        # ===== 15M 量价确认 =====
        k15m = get_klines(symbol, "15m", 5)
        closes_15m = [float(k[4]) for k in k15m]
        opens_15m = [float(k[1]) for k in k15m]
        vols_15m = [float(k[5]) for k in k15m]

        bull_count_15m = sum(1 for i in range(len(closes_15m)) if closes_15m[i] > opens_15m[i])
        vol_trend = "放大" if vols_15m[-1] > sum(vols_15m[:-1]) / max(len(vols_15m)-1, 1) else "缩量"
        high_5h = max(float(k[2]) for k in k15m)
        at_high = closes_15m[-1] >= high_5h * 0.99

        if bull_count_15m >= 4:
            lines.append(f"✅ 15M：最近5根中 {bull_count_15m} 根阳线，多头控盘")
        elif bull_count_15m >= 3:
            lines.append(f"🟢 15M：最近5根中 {bull_count_15m} 根阳线")
        else:
            lines.append(f"⚠️ 15M：最近5根中仅 {bull_count_15m} 根阳线，多空拉锯")
        if vol_trend == "放大":
            lines.append(f"✅ 15M 成交量：最新一根放量，资金活跃")
        else:
            lines.append(f"⚠️ 15M 成交量：最新一根缩量，动能不足")
        if at_high:
            lines.append(f"🟢 15M：当前价接近5小时高点，突破中")

        # ===== 1H 位置判断 =====
        k1h = get_klines(symbol, "1h", 24)
        closes_1h = [float(k[4]) for k in k1h]
        opens_1h = [float(k[1]) for k in k1h]
        highs_1h = [float(k[2]) for k in k1h]
        lows_1h = [float(k[3]) for k in k1h]

        change_24h = (closes_1h[-1] - closes_1h[0]) / closes_1h[0] * 100

        high_24h = max(highs_1h)
        low_24h = min(lows_1h)
        if high_24h == low_24h:
            position_24h = 50
        else:
            position_24h = (closes_1h[-1] - low_24h) / (high_24h - low_24h) * 100

        consecutive_bull = 0
        for i in range(len(closes_1h) - 1, -1, -1):
            if closes_1h[i] > opens_1h[i]:
                consecutive_bull += 1
            else:
                break

        if consecutive_bull >= 6:
            lines.append(f"🚨 1H 连涨 {consecutive_bull} 根 — 极度危险，大概率是末端，不要追")
        elif consecutive_bull >= 4:
            lines.append(f"⚠️ 1H 连涨 {consecutive_bull} 根 — 趋势偏热，追高风险大")
        elif consecutive_bull >= 2:
            lines.append(f"🟢 1H 连涨 {consecutive_bull} 根 — 趋势刚启动")
        else:
            lines.append(f"🟡 1H 连涨 {consecutive_bull} 根 — 还没形成明显趋势")

        if abs(change_24h) > 15:
            lines.append(f"🚨 24h 涨跌 {change_24h:+.2f}% — 短期涨幅过大，回调风险极高")
        elif abs(change_24h) > 8:
            lines.append(f"⚠️ 24h 涨跌 {change_24h:+.2f}% — 已经涨了不少")
        elif abs(change_24h) > 3:
            lines.append(f"🟢 24h 涨跌 {change_24h:+.2f}% — 涨幅适中")
        else:
            lines.append(f"🟡 24h 涨跌 {change_24h:+.2f}% — 波动不大")

        if position_24h > 90:
            lines.append(f"🚨 24h区间位置 {position_24h:.0f}% — 在山顶")
        elif position_24h > 70:
            lines.append(f"⚠️ 24h区间位置 {position_24h:.0f}% — 偏高")
        elif position_24h > 40:
            lines.append(f"🟢 24h区间位置 {position_24h:.0f}% — 中间")
        else:
            lines.append(f"✅ 24h区间位置 {position_24h:.0f}% — 偏低，空间大")

        # ===== 日线（7天）=====
        k1d = get_klines(symbol, "1d", 7)
        closes_1d = [float(k[4]) for k in k1d]
        change_7d = (closes_1d[-1] - closes_1d[0]) / closes_1d[0] * 100

        if abs(change_7d) > 30:
            lines.append(f"🚨 7天涨跌 {change_7d:+.2f}% — 短期已暴涨，追高=接盘")
        elif abs(change_7d) > 15:
            lines.append(f"⚠️ 7天涨跌 {change_7d:+.2f}% — 近一周涨幅偏大")
        else:
            lines.append(f"🟢 7天涨跌 {change_7d:+.2f}%")

        # ===== 综合建议 =====
        danger_count = sum(1 for l in lines if l.startswith("🚨"))
        warn_count = sum(1 for l in lines if l.startswith("⚠️"))

        lines.append("")
        if danger_count >= 2:
            lines.append("🛑 位置建议：不要买！多个指标显示在高位，追高大概率亏钱")
        elif danger_count >= 1 or warn_count >= 2:
            lines.append("⚠️ 位置建议：谨慎，如果要买请轻仓，设好止损")
        elif warn_count >= 1:
            lines.append("🟡 位置建议：可以关注，但不要重仓")
        else:
            lines.append("✅ 位置建议：位置健康，可以考虑介入")

        return "\n".join(lines)

    except Exception as e:
        logger.error(f"{symbol} 位置分析失败: {e}")
        return "📊 位置分析：数据获取失败"

# ===================== 因子解读 =====================
def interpret_signal(sig):
    lines = []
    direction = sig['direction']
    is_long = (direction == 'LONG')

    oi = sig['oi_change'] * 100
    if oi > 5:
        lines.append(f"✅ 持仓量 1h 涨 {oi:+.2f}% — 大量新仓位开立，有真金白银进场")
    elif oi > 1:
        lines.append(f"🟡 持仓量 1h 涨 {oi:+.2f}% — 有新仓位但规模不大")
    elif oi > -1:
        lines.append(f"⚠️ 持仓量 1h 变化 {oi:+.2f}% — 几乎没有新仓位，价格靠存量资金推动")
    else:
        lines.append(f"❌ 持仓量 1h 跌 {oi:+.2f}% — 持仓在减少，有人在平仓离场")

    vr = sig['volume_ratio']
    if vr >= 2:
        lines.append(f"✅ 成交量放大 {vr:.2f} 倍 — 交易极度活跃，大资金在行动")
    elif vr >= 1.5:
        lines.append(f"🟢 成交量放大 {vr:.2f} 倍 — 交易明显活跃")
    elif vr >= 1:
        lines.append(f"🟡 成交量 {vr:.2f} 倍 — 交易量正常")
    else:
        lines.append(f"⚠️ 成交量 {vr:.2f} 倍 — 缩量，上涨不可靠")

    fr = sig['funding'] * 100
    if is_long:
        if 0 < fr < 0.05:
            lines.append(f"✅ 资金费率 {fr:.4f}% — 多头温和占优，不拥挤")
        elif 0.05 <= fr < 0.1:
            lines.append(f"🟡 资金费率 {fr:.4f}% — 多头偏热，留意回调")
        elif fr >= 0.1:
            lines.append(f"⚠️ 资金费率 {fr:.4f}% — 多头过度拥挤，容易多杀多")
        elif fr < 0:
            lines.append(f"❌ 资金费率 {fr:.4f}% — 空头占优，逆势做多风险大")
        else:
            lines.append(f"🟢 资金费率 {fr:.4f}% — 多空平衡")
    else:
        if fr < -0.05:
            lines.append(f"✅ 资金费率 {fr:.4f}% — 空头温和占优")
        elif 0 < fr < 0.05:
            lines.append(f"⚠️ 资金费率 {fr:.4f}% — 多头仍在，做空警惕反弹")
        else:
            lines.append(f"🟡 资金费率 {fr:.4f}%")

    pc = sig['price_change'] * 100
    if abs(pc) > 8:
        lines.append(f"🚨 15M 趋势：1h 涨跌 {pc:+.2f}% — 涨幅过大，极高追高风险")
    elif abs(pc) > 5:
        lines.append(f"⚠️ 15M 趋势：1h 涨跌 {pc:+.2f}% — 涨幅偏大，介入偏晚")
    elif abs(pc) > 2:
        lines.append(f"🟢 15M 趋势：1h 涨跌 {pc:+.2f}% — 启动明确")
    else:
        lines.append(f"🟡 15M 趋势：1h 涨跌 {pc:+.2f}% — 启动温和")

    score = sig['score']
    oi_strong = abs(oi) > 1
    vol_strong = vr > 1.5

    lines.append("")
    if score >= 4 and oi_strong and vol_strong:
        verdict = "💎 综合：OI 有新钱 + 量能放大 + 多因子共振，信号强"
    elif score >= 4 and (oi_strong or vol_strong):
        verdict = "🟢 综合：分数高且有硬因子支撑，可重点关注"
    elif score >= 4:
        verdict = "🟡 综合：分数高但缺硬因子，观察为主"
    elif score == 3 and oi_strong and vol_strong:
        verdict = "🟢 综合：分数刚够但双硬因子撑着，值得关注"
    elif score == 3 and (oi_strong or vol_strong):
        verdict = "🟡 综合：分数刚够，有一个硬因子，可小仓位观察"
    else:
        verdict = "⚠️ 综合：信号偏弱，谨慎"
    lines.append(verdict)
    return "\n".join(lines)

# ===================== 信号通知（带位置分析）=====================
def notify_signal(sig):
    emoji = "🚀" if sig['direction'] == 'LONG' else "📉"
    position_text = analyze_position(sig['symbol'])

    text = (
        f"{emoji} {sig['symbol']} — {sig['direction']} 已确认\n"
        f"━━━━━━━━━━━━━━━\n"
        f"💰 价格: ${sig['price']:.6f}\n"
        f"📊 OI 1h: {sig['oi_change']*100:+.2f}%\n"
        f"📈 价格 1h: {sig['price_change']*100:+.2f}%\n"
        f"🔊 量能比: {sig['volume_ratio']:.2f}x\n"
        f"💵 资金费率: {sig['funding']*100:.4f}%\n"
        f"⭐ 信号强度: {sig['score']}/5\n"
        f"━━━━━━━━━━━━━━━\n"
        f"📖 因子解读:\n{interpret_signal(sig)}\n"
        f"━━━━━━━━━━━━━━━\n"
        f"📊 位置分析:\n{position_text}\n"
        f"━━━━━━━━━━━━━━━\n"
        f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    )
    notify_all(text)

# ===================== 状态缓存 =====================
state_b = defaultdict(lambda: {
    "active": False,
    "last_high": None,
    "base_low": None,
    "push_count": 0,
    "day": None,
    "daily_round": 0,
    "start_pct": 0.0,
    "first_price": None,
    "daily_high": 0.0,
    "daily_low": 1e10,
    "push_times": 0,
    "trigger_time": None,
})

pending_confirm = set()
active_tracking = {}
lock = Lock()

# ===================== 触发层 =====================
def scan_trigger(sym):
    try:
        sb = state_b[sym]
        today = date.today()

        if sb["day"] != today:
            sb["day"] = today
            sb["daily_round"] = 0
            if sym not in active_tracking:
                sb["active"] = False
                sb["push_count"] = 0

        if sb["daily_round"] >= MAX_DAILY_ROUND:
            return

        k3 = get_klines(sym, "3m", 6)
        highs = [float(x[2]) for x in k3]
        lows = [float(x[3]) for x in k3]
        price_now = float(k3[-1][4])

        if not sb["active"]:
            hh = highs[-3] < highs[-2] < highs[-1]
            start_pct = (highs[-1] - lows[-3]) / lows[-3] * 100
            if hh and start_pct >= HH_MIN_TOTAL_PCT:
                sb["active"] = True
                sb["last_high"] = highs[-1]
                sb["base_low"] = lows[-3]
                sb["push_count"] = 1
                sb["daily_round"] += 1
                sb["start_pct"] = start_pct
                sb["first_price"] = price_now
                sb["daily_high"] = price_now
                sb["daily_low"] = price_now
                sb["trigger_time"] = datetime.now()

                # 【Bug1修复】只有高成交额的币才进入确认池
                if sym in high_volume_symbols:
                    with lock:
                        pending_confirm.add(sym)
                    logger.info(f"🟢 触发: {sym} (start_pct={start_pct:.2f}%)")
                else:
                    logger.info(f"🔇 触发但成交额不足: {sym}")

        sb["daily_high"] = max(sb["daily_high"], price_now)
        sb["daily_low"] = min(sb["daily_low"], price_now)
    except Exception:
        pass

# ===================== 确认层（含成交额门槛 + 硬因子 + 追高否决）=====================
def analyze_symbol(symbol):
    try:
        # 成交额门槛：不在高成交额集合里的直接否决
        if symbol not in high_volume_symbols:
            return None

        oi_hist = get_oi_history(symbol, "1h", 2)
        if len(oi_hist) < 2:
            return None
        oi_prev = float(oi_hist[0]['sumOpenInterest'])
        oi_now = float(oi_hist[-1]['sumOpenInterest'])
        oi_change = (oi_now - oi_prev) / oi_prev if oi_prev != 0 else 0

        klines = get_klines(symbol, "15m", 20)
        closes = [float(k[4]) for k in klines]
        volumes = [float(k[5]) for k in klines]
        if len(volumes) < 4:
            return None
        price_change_1h = (closes[-1] - closes[-5]) / closes[-5] if closes[-5] != 0 else 0

        avg_vol = sum(volumes[:-3]) / len(volumes[:-3]) if len(volumes[:-3]) > 0 else 1
        recent_vol = sum(volumes[-3:]) / 3
        volume_ratio = recent_vol / avg_vol if avg_vol > 0 else 0

        bullish = sum(1 for k in klines[-3:] if float(k[4]) > float(k[1]))
        funding = get_funding_rate(symbol)

        bull, bear = 0, 0

        if oi_change > OI_STRONG:
            bull += 1 if price_change_1h > 0 else 0
            bear += 1 if price_change_1h < 0 else 0
        if volume_ratio > VOLUME_MULTIPLIER:
            if price_change_1h > PRICE_CHANGE_MIN:
                bull += 1
            elif price_change_1h < -PRICE_CHANGE_MIN:
                bear += 1
        if bullish >= 2:
            bull += 1
        elif bullish == 0:
            bear += 1
        if 0 < funding < FUNDING_RATE_MAX:
            bull += 1
        elif funding < 0:
            bear += 1
        elif funding > 0.001:
            bear += 1
        recent_high = max(closes[-20:-1]) if len(closes[-20:-1]) > 0 else closes[-1]
        recent_low = min(closes[-20:-1]) if len(closes[-20:-1]) > 0 else closes[-1]
        if closes[-1] > recent_high:
            bull += 1
        elif closes[-1] < recent_low:
            bear += 1

        # 硬因子门槛（OI变化>1% 或 量能>1.5倍）
        oi_contributed = abs(oi_change) > 0.01
        vol_contributed = volume_ratio > 1.5
        if not (oi_contributed or vol_contributed):
            return None

        if bull >= MIN_FACTORS_TO_ALERT:
            if price_change_1h > MAX_1H_CHANGE:
                logger.info(f"⛔ {symbol} 否决: 1h涨幅 {price_change_1h*100:.2f}% 超 8%")
                return None
            return {
                "symbol": symbol, "direction": "LONG", "score": bull,
                "oi_change": oi_change, "price_change": price_change_1h,
                "volume_ratio": volume_ratio, "funding": funding,
                "price": closes[-1]
            }
        if bear >= MIN_FACTORS_TO_ALERT:
            if price_change_1h < -MAX_1H_CHANGE:
                logger.info(f"⛔ {symbol} 否决: 1h跌幅 {price_change_1h*100:.2f}% 超 8%")
                return None
            return {
                "symbol": symbol, "direction": "SHORT", "score": bear,
                "oi_change": oi_change, "price_change": price_change_1h,
                "volume_ratio": volume_ratio, "funding": funding,
                "price": closes[-1]
            }
        return None
    except Exception as e:
        logger.error(f"{symbol} 确认分析失败: {e}")
        return None

# ===================== 追踪层 =====================
def track_active():
    for sym in list(active_tracking.keys()):
        sb = active_tracking[sym]
        try:
            if sb["last_high"] is None or sb["base_low"] is None:
                del active_tracking[sym]
                continue

            k3 = get_klines(sym, "3m", 6)
            highs = [float(x[2]) for x in k3]
            lows = [float(x[3]) for x in k3]
            price_now = float(k3[-1][4])

            drawdown = (sb["last_high"] - lows[-1]) / sb["last_high"]
            if drawdown >= DRAWDOWN_FAIL:
                notify_all(
                    f"❌ {sym} 信号失效\n"
                    f"━━━━━━━━━━━━━━━\n"
                    f"回撤: {drawdown*100:.2f}%（阈值 {DRAWDOWN_FAIL*100:.0f}%）\n"
                    f"当前价: {price_now:.6f}\n"
                    f"建议: 减仓 / 离场观望"
                )
                sb["active"] = False
                del active_tracking[sym]
                continue

            if highs[-1] > sb["last_high"] * (1 + MIN_PUSH_PCT) and sb["push_count"] < MAX_PUSH:
                push_pct = (highs[-1] - sb["last_high"]) / sb["last_high"] * 100
                sb["last_high"] = highs[-1]
                sb["push_count"] += 1
                cur_pct = (sb["last_high"] - sb["base_low"]) / sb["base_low"] * 100
                sb["push_times"] += 1
                sb["daily_high"] = max(sb["daily_high"], price_now)
                notify_all(
                    f"🚀 {sym} 推进（第{sb['push_count']}次）\n"
                    f"━━━━━━━━━━━━━━━\n"
                    f"当前价: {price_now:.6f}\n"
                    f"本次新涨: +{push_pct:.2f}%\n"
                    f"结构总涨幅: +{cur_pct:.2f}%\n"
                    f"3M HH 持续突破"
                )

            if sb["push_count"] >= MAX_PUSH:
                del active_tracking[sym]
        except Exception as e:
            logger.error(f"{sym} 追踪失败: {e}")

# ===================== 噪音过滤（并发版）=====================
def market_too_noisy(symbols):
    sample = symbols[:50]
    if not sample:
        return False

    def _check(sym):
        try:
            k = get_klines(sym, "1m", 2)
            o = float(k[-1][1])
            c = float(k[-1][4])
            return (c - o) / o * 100 >= MARKET_NOISE_PCT
        except:
            return False

    with ThreadPoolExecutor(max_workers=15) as ex:
        results = list(ex.map(_check, sample))

    up_count = sum(results)
    ratio = up_count / len(sample)
    if ratio >= MARKET_NOISE_RATIO:
        logger.info(f"⚠️ 全市场普涨 ({ratio*100:.0f}%)，跳过本轮")
        return True
    return False

# ===================== 日报（修复版）=====================
def generate_daily_report(report_date=None):
    if report_date is None:
        report_date = date.today()
    rows = []
    for sym, sb in state_b.items():
        if sb.get("first_price") is None:
            continue
        first = sb["first_price"]
        high = sb.get("daily_high") or first
        low = sb.get("daily_low") or first
        rows.append({
            "币种": sym,
            "首次触发价": first,
            "当日最高": high,
            "当日最低": low,
            "当日涨幅(%)": round((high - first) / first * 100, 2),
            "推进次数": sb.get("push_times", 0),
        })

    if not rows:
        logger.info(f"📊 {report_date} 日报：无触发记录，跳过")
        notify_all(f"📊 {report_date} 日报：今日无触发记录")
        return

    df = pd.DataFrame(rows).sort_values("当日涨幅(%)", ascending=False)
    fname = f"日报_{report_date.strftime('%Y%m%d')}.xlsx"
    df.to_excel(fname, index=False)
    send_email_attach(f"盘面日报 {report_date}", "附件为今日全部触发记录", fname)
    logger.info(f"📊 已生成日报: {fname}，共 {len(rows)} 条记录")
    notify_all(f"📊 {report_date} 日报已发送，共 {len(rows)} 条记录")

# ===================== 主循环 =====================
def main_loop():
    logger.info("🚀 融合监控系统启动（全市场扫描 + 5000万确认版）")
    notify_all(
        "✅ 融合监控系统启动\n"
        "扫描：全市场山寨合约\n"
        "确认：24h成交额>5000万 + 多因子 + 硬因子门槛\n"
        "追踪：推进(+4%) / 失效(5%回撤)\n"
        "位置：3M/15M/1H/日线 四层分析"
    )

    symbols = get_symbols()
    # 【Bug2修复】启动时检查高成交额集合是否为空
    if not symbols or not high_volume_symbols:
        logger.error("无可用币种或无法获取成交额数据，退出")
        notify_all("❌ 系统启动失败：无法获取币种列表或成交额数据")
        return

    last_refresh = time.time()
    last_report_day = None

    while True:
        try:
            now_cn = datetime.now(timezone(timedelta(hours=8)))
            today = now_cn.date()

            # 0点日报（最前面，不被跳过）
            if now_cn.hour == 0 and last_report_day != today:
                yesterday = today - timedelta(days=1)
                generate_daily_report(yesterday)
                last_report_day = today
                for sym, sb in state_b.items():
                    if sym in active_tracking:
                        continue
                    sb["daily_high"] = 0.0
                    sb["daily_low"] = 1e10
                    sb["push_times"] = 0
                    sb["first_price"] = None

            # 刷新币种列表
            if time.time() - last_refresh > SYMBOL_REFRESH_INTERVAL:
                new_syms = get_symbols()
                if new_syms:
                    symbols = new_syms
                    last_refresh = time.time()
                    # 刷新后如果高成交额集合为空，记录警告但继续运行（可能网络波动）
                    if not high_volume_symbols:
                        logger.warning("刷新后成交额数据为空，后续可能无法确认信号")

            # 噪音过滤
            if market_too_noisy(symbols):
                time.sleep(SCAN_INTERVAL)
                continue

            # 预创建 state_b 条目（线程安全）
            for sym in symbols:
                _ = state_b[sym]

            # ===== 触发层：全市场扫 3M HH =====
            t0 = time.time()
            with ThreadPoolExecutor(max_workers=MAX_WORKERS_TRIGGER) as ex:
                list(ex.map(scan_trigger, symbols))
            logger.info(f"全市场扫描完成 ({time.time() - t0:.1f}s)，{len(symbols)}币，候选 {len(pending_confirm)} 个")

            # ===== 确认层（自动过滤成交额不足的）=====
            with lock:
                to_confirm = list(pending_confirm)[:MAX_CANDIDATES_PER_CYCLE]
                pending_confirm.clear()

            if to_confirm:
                with ThreadPoolExecutor(max_workers=MAX_WORKERS_CONFIRM) as ex:
                    futures = {ex.submit(analyze_symbol, s): s for s in to_confirm}
                    for fu in as_completed(futures):
                        sig = fu.result()
                        sym = futures[fu]
                        if sig and should_alert(sig['symbol'], sig['direction']):
                            logger.info(f"✅ 确认信号: {sig['symbol']} {sig['direction']} 强度{sig['score']}")
                            save_signal(sig)
                            notify_signal(sig)
                            mark_alerted(sig['symbol'], sig['direction'])
                            active_tracking[sig['symbol']] = state_b[sig['symbol']]
                        else:
                            state_b[sym]["active"] = False

            # ===== 追踪层 =====
            if active_tracking:
                track_active()

            time.sleep(SCAN_INTERVAL)

        except KeyboardInterrupt:
            logger.info("🛑 手动退出")
            break
        except Exception as e:
            logger.error(f"主循环异常: {e}")
            time.sleep(10)

if __name__ == "__main__":
    main_loop()