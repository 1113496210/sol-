# -*- coding: utf-8 -*-
"""
Binance USDT 永续合约 · 融合监控系统（最终稳定版）
=====================================
触发层：3M HH 结构（系统B，灵敏）
确认层：多因子打分（OI/量能/资金费率/K线/突破，≥3因子）
追踪层：HH 推进（≥4% 才推进）+ 回撤失效（5%）
通知：Telegram + QQ 邮箱（即时 + 每日Excel日报）
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
SCAN_INTERVAL = 60                 # 全市场扫描间隔（秒）
SYMBOL_REFRESH_INTERVAL = 3600     # 币种列表刷新间隔
REQUEST_TIMEOUT = 10

# 流动性过滤
MIN_24H_VOLUME_USDT = 10_000_000   # 24h 成交额 < 1000万 USDT 的币不监控
ALT_BLACKLIST = ["BTCUSDT", "ETHUSDT"]   # 主流币不进观察池

# 系统B（触发层）参数
HH_MIN_TOTAL_PCT = 1.2            # 启动结构最小涨幅（%）
DRAWDOWN_FAIL = 0.05              # 回撤 5% 视为失效
MAX_PUSH = 3                      # 单轮最多推进次数
MAX_DAILY_ROUND = 2               # 单币每日最多触发轮数

# 多因子确认层参数
MIN_FACTORS_TO_ALERT = 3          # ≥3 因子同向才推送
OI_STRONG = 0.05                  # OI 1h 变化阈值（5%）
VOLUME_MULTIPLIER = 1.5           # 量能放大倍数
PRICE_CHANGE_MIN = 0.01           # 1h 价格变化 1%
FUNDING_RATE_MAX = 0.001          # 资金费率上限

# 全市场噪音过滤（普涨时不推）
MARKET_NOISE_PCT = 1.2
MARKET_NOISE_RATIO = 0.6

# 去重
DEDUP_HOURS = 2

# 并发
MAX_WORKERS_TRIGGER = 20          # 触发层并发
MAX_WORKERS_CONFIRM = 10          # 确认层并发
MAX_CANDIDATES_PER_CYCLE = 30     # 每轮最多确认的候选币数量

# 推进过滤（必须涨幅 >= 4% 才推进）
MIN_PUSH_PCT = 0.04               # 推进至少涨 4% 才推送

LOG_FILE = "monitor.log"

# ===================== 日志 + 数据库（单连接+锁）======================
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
    """只查询是否重复，不写入"""
    cutoff = datetime.now() - timedelta(hours=DEDUP_HOURS)
    with db_lock:
        cur = db_conn.execute(
            "SELECT 1 FROM sent_log WHERE symbol=? AND direction=? AND sent_at > ?",
            (symbol, direction, cutoff)
        )
        return cur.fetchone() is None

def mark_alerted(symbol, direction):
    """推送成功后写入去重记录"""
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
def get_symbols():
    try:
        info = session.get(f"{BINANCE_API}/fapi/v1/exchangeInfo").json()
        all_syms = [
            s['symbol'] for s in info['symbols']
            if s['contractType'] == 'PERPETUAL'
            and s['quoteAsset'] == 'USDT'
            and s['status'] == 'TRADING'
        ]
        tickers = session.get(f"{BINANCE_API}/fapi/v1/ticker/24hr").json()
        active = set()
        for t in tickers:
            try:
                if float(t.get('quoteVolume', 0)) > MIN_24H_VOLUME_USDT:
                    active.add(t['symbol'])
            except:
                pass
        result = [s for s in all_syms if s in active and s not in ALT_BLACKLIST]
        logger.info(f"全部永续: {len(all_syms)} → 活跃币种: {len(result)}")
        return result
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

# ===================== 白话解读模块 =====================
def interpret_signal(sig):
    """根据因子数值生成白话解读"""
    lines = []
    
    # OI 解读
    oi = sig['oi_change'] * 100
    if oi > 5:
        lines.append(f"✅ 持仓量大涨 {oi:+.2f}% — 真有新资金进场，趋势可信")
    elif oi > 1:
        lines.append(f"🟡 持仓量小涨 {oi:+.2f}% — 有新仓但不算激进")
    elif oi > -1:
        lines.append(f"⚠️ 持仓量几乎不变 {oi:+.2f}% — 老仓换手，不是新钱进场，涨得快崩得也快")
    else:
        lines.append(f"❌ 持仓量在减少 {oi:+.2f}% — 资金在撤退，可能是逼空反弹，不是真趋势")
    
    # 量能解读
    vr = sig['volume_ratio']
    if vr >= 2:
        lines.append(f"✅ 成交量放大 {vr:.2f} 倍 — 资金涌入明显")
    elif vr >= 1.5:
        lines.append(f"🟢 成交量放大 {vr:.2f} 倍 — 量能健康")
    elif vr >= 1:
        lines.append(f"🟡 成交量 {vr:.2f} 倍 — 略有放量")
    else:
        lines.append(f"⚠️ 成交量 {vr:.2f} 倍 — 没放量，光涨价不放量是假涨")
    
    # 资金费率解读（百分比）
    fr = sig['funding'] * 100
    if 0 < fr < 0.05:
        lines.append(f"✅ 资金费率 {fr:.4f}% — 多头温和占优，不拥挤")
    elif 0.05 <= fr < 0.1:
        lines.append(f"🟡 资金费率 {fr:.4f}% — 多头偏热，留意回调")
    elif fr >= 0.1:
        lines.append(f"⚠️ 资金费率 {fr:.4f}% — 多头过度拥挤，小心多杀多")
    elif fr < 0:
        lines.append(f"❌ 资金费率 {fr:.4f}% — 空头主导，做多要谨慎")
    else:
        lines.append(f"🟢 资金费率 {fr:.4f}% — 正常水平")
    
    # 价格变化解读
    pc = sig['price_change'] * 100
    if pc > 5:
        lines.append(f"⚠️ 1h 已涨 {pc:+.2f}% — 涨幅偏大，介入晚了风险高")
    elif pc > 2:
        lines.append(f"🟢 1h 涨幅 {pc:+.2f}% — 启动明确")
    else:
        lines.append(f"🟡 1h 涨幅 {pc:+.2f}% — 启动温和")
    
    # 综合判断
    score = sig['score']
    oi_or_vol_strong = (oi > 1) or (vr > 1.5)
    if score >= 4 and oi_or_vol_strong:
        verdict = "💎 综合判断：信号质量高，OI 和量能至少一个有真金白银"
    elif score >= 4:
        verdict = "🟡 综合判断：分数高但 OI/量能偏弱，可能是软因子凑出来的"
    elif score == 3 and oi_or_vol_strong:
        verdict = "🟢 综合判断：分数刚够，但有硬因子支撑，可观察"
    else:
        verdict = "⚠️ 综合判断：分数刚够且硬因子(OI/量能)弱，可能是假信号，谨慎"
    
    lines.append("")
    lines.append(verdict)
    return "\n".join(lines)

def notify_signal(sig):
    emoji = "🚀" if sig['direction'] == 'LONG' else "📉"
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
        f"📖 解读:\n{interpret_signal(sig)}\n"
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

# ===================== 触发层（系统B扫描） =====================
def scan_trigger(sym):
    try:
        sb = state_b[sym]
        today = date.today()

        # 跨日重置（保护正在追踪的币）
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
                with lock:
                    pending_confirm.add(sym)
                logger.info(f"🟢 触发: {sym} (start_pct={start_pct:.2f}%)")

        # 更新每日极值
        sb["daily_high"] = max(sb["daily_high"], price_now)
        sb["daily_low"] = min(sb["daily_low"], price_now)

    except Exception:
        pass

# ===================== 确认层 =====================
def analyze_symbol(symbol):
    try:
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

        # OI
        if oi_change > OI_STRONG:
            bull += 1 if price_change_1h > 0 else 0
            bear += 1 if price_change_1h < 0 else 0

        # 量能+方向
        if volume_ratio > VOLUME_MULTIPLIER:
            if price_change_1h > PRICE_CHANGE_MIN:
                bull += 1
            elif price_change_1h < -PRICE_CHANGE_MIN:
                bear += 1

        # K线形态
        if bullish >= 2:
            bull += 1
        elif bullish == 0:
            bear += 1

        # 资金费率
        if 0 < funding < FUNDING_RATE_MAX:
            bull += 1
        elif funding < 0:
            bear += 1
        elif funding > 0.001:
            bear += 1

        # 突破
        recent_high = max(closes[-20:-1]) if len(closes[-20:-1]) > 0 else closes[-1]
        recent_low = min(closes[-20:-1]) if len(closes[-20:-1]) > 0 else closes[-1]
        if closes[-1] > recent_high:
            bull += 1
        elif closes[-1] < recent_low:
            bear += 1

        if bull >= MIN_FACTORS_TO_ALERT:
            return {"symbol": symbol, "direction": "LONG", "score": bull,
                    "oi_change": oi_change, "price_change": price_change_1h,
                    "volume_ratio": volume_ratio, "funding": funding,
                    "price": closes[-1]}
        if bear >= MIN_FACTORS_TO_ALERT:
            return {"symbol": symbol, "direction": "SHORT", "score": bear,
                    "oi_change": oi_change, "price_change": price_change_1h,
                    "volume_ratio": volume_ratio, "funding": funding,
                    "price": closes[-1]}
        return None
    except Exception as e:
        logger.error(f"{symbol} 确认分析失败: {e}")
        return None

# ===================== 追踪层（推进 ≥4% 才推） =====================
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

            # 失效判断
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

            # 推进判断：新高且涨幅 ≥ MIN_PUSH_PCT（≥4%）
            target = sb["last_high"] * (1 + MIN_PUSH_PCT)
            if highs[-1] >= target and sb["push_count"] < MAX_PUSH:
                sb["last_high"] = highs[-1]
                sb["push_count"] += 1
                cur_pct = (sb["last_high"] - sb["base_low"]) / sb["base_low"] * 100
                sb["push_times"] += 1
                sb["daily_high"] = max(sb["daily_high"], price_now)
                notify_all(
                    f"🚀 {sym} 推进（第{sb['push_count']}次）\n"
                    f"━━━━━━━━━━━━━━━\n"
                    f"当前价: {price_now:.6f}\n"
                    f"结构涨幅: +{cur_pct:.2f}%\n"
                    f"3M HH 持续突破（+{MIN_PUSH_PCT*100:.0f}%阈值）"
                )

            if sb["push_count"] >= MAX_PUSH:
                del active_tracking[sym]
        except Exception as e:
            logger.error(f"{sym} 追踪失败: {e}")

# ===================== 噪音过滤 =====================
def market_too_noisy(symbols):
    up_count = 0
    sample = symbols[:50]
    for sym in sample:
        try:
            k = get_klines(sym, "1m", 2)
            o = float(k[-1][1])
            c = float(k[-1][4])
            if (c - o) / o * 100 >= MARKET_NOISE_PCT:
                up_count += 1
        except:
            pass
    if not sample:
        return False
    ratio = up_count / len(sample)
    if ratio >= MARKET_NOISE_RATIO:
        logger.info(f"⚠️ 全市场普涨 ({ratio*100:.0f}%)，跳过本轮")
        return True
    return False

# ===================== 日报 =====================
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
        return
    df = pd.DataFrame(rows).sort_values("当日涨幅(%)", ascending=False)
    fname = f"日报_{report_date.strftime('%Y%m%d')}.xlsx"
    df.to_excel(fname, index=False)
    send_email_attach(f"盘面日报 {report_date}", "附件为今日全部触发记录", fname)
    logger.info(f"📊 已生成日报: {fname}")

# ===================== 主循环 =====================
def main_loop():
    logger.info("🚀 融合监控系统启动")
    notify_all("✅ 融合监控系统启动\n触发层(3M HH) + 确认层(多因子) + 追踪层(≥4%推进/5%失效)")

    symbols = get_symbols()
    if not symbols:
        logger.error("无可用币种，退出")
        return

    last_refresh = time.time()
    last_report_day = None

    while True:
        try:
            now_cn = datetime.now(timezone(timedelta(hours=8)))
            today = now_cn.date()

            # 0点生成昨日日报，重置状态（跳过正在追踪的币）
            if now_cn.hour == 0 and last_report_day != today:
                yesterday = today - timedelta(days=1)
                generate_daily_report(yesterday)
                last_report_day = today
                # 重置所有未在追踪中的币的每日统计
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

            # 噪音过滤
            if market_too_noisy(symbols):
                time.sleep(SCAN_INTERVAL)
                continue

            # 触发层
            t0 = time.time()
            with ThreadPoolExecutor(max_workers=MAX_WORKERS_TRIGGER) as ex:
                list(ex.map(scan_trigger, symbols))
            logger.info(f"触发扫描完成 ({time.time()-t0:.1f}s)，候选 {len(pending_confirm)} 个")

            # 确认层
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

            # 追踪层
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