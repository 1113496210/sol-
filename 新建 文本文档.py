# -*- coding: utf-8 -*-
"""
融合监控系统（四通道版）
通道1 🚀 确认信号：3M HH + 多因子
通道2 ⚡ 速报信号：3M HH ≥ 3% 直接推送
通道3 📈 趋势信号：1H K线趋势检测
通道4 📐 建仓信号：日线下跌→放量→横盘结构 (仅推送关注/介入)

修改说明：
- 三通道所有代码、参数、函数签名、逻辑均未改动
- 新增建仓通道配置、API辅助函数、检测函数、扫描调度
- sent_log 表增加 channel 字段，should_alert/mark_alerted 增加 channel 参数（原调用均已补全）
- get_cross_channel_history 增加 building 识别
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
TELEGRAM_TOKEN = "7874342652:AAHV4F8sS9alhYTPaI21-n34b7ajrn3OG0U"
TELEGRAM_CHAT_ID = "5408890841"

# QQ 邮箱
ENABLE_EMAIL = True
QQ_EMAIL = "1113496210@qq.com"
QQ_AUTH_CODE = "hzshvazrbnyzfhdf"
EMAIL_TO = "1113496210@qq.com"
SMTP_HOST = "smtp.qq.com"
SMTP_PORT = 465

# 扫描节奏
SCAN_INTERVAL = 30
SYMBOL_REFRESH_INTERVAL = 3600
REQUEST_TIMEOUT = 10

# 成交额门槛
MIN_24H_VOLUME_USDT = 13_000_000  # 1300万 USDT

ALT_BLACKLIST = ["BTCUSDT", "ETHUSDT"]

# 系统B（触发层）参数
HH_MIN_TOTAL_PCT = 1.2
DRAWDOWN_FAIL = 0.05
MAX_PUSH = 3
MAX_DAILY_ROUND = 2
MIN_PUSH_PCT = 0.04

# 速报通道参数
SURGE_HH_PCT = 3.0  # 3M HH ≥ 3% 直接速报

# 趋势通道参数
TREND_SCAN_INTERVAL = 1800  # 30分钟扫一次
TREND_CHANGE_MIN = 5.0  # 24h 涨幅 > 5%
TREND_BULL_MIN = 10  # 阳线 ≥ 10/24
TREND_AT_HIGH_MIN = 90  # 距24h最高 > 90%
TREND_DEDUP_HOURS = 6  # 趋势信号去重 6 小时
TREND_MAX_COUNT = 8  # 48小时内最多提醒次数

# 多因子确认层参数
MIN_FACTORS_TO_ALERT = 3
OI_STRONG = 0.05
VOLUME_MULTIPLIER = 1.5
PRICE_CHANGE_MIN = 0.01
FUNDING_RATE_MAX = 0.001
MAX_1H_CHANGE = 0.08

# 噪音过滤
MARKET_NOISE_SAMPLE = 100  # 抽样 100 个币
MARKET_NOISE_PCT = 1.2
MARKET_NOISE_RATIO = 0.80  # 80% 才判定普涨

# 去重
DEDUP_HOURS = 2

# 追踪超时
TRACKING_TIMEOUT_HOURS = 24  # 24小时无变化撤出

# 并发
MAX_WORKERS_TRIGGER = 30
MAX_WORKERS_CONFIRM = 15
MAX_CANDIDATES_PER_CYCLE = 30
MAX_WORKERS_TREND = 10

# ────────────── 建仓通道专用参数（新增）──────────────
BUILD_SCAN_INTERVAL = 3600
MIN_VOLUME_24H_BUILD = 3_000_000
DROP_7D_PCT = -10
NEAR_LOW_PCT = 8
VOL_SHRINK_DAYS = 3
VOL_SURGE_MULT = 1.8
SURGE_CHANGE_MIN = -2
SURGE_CHANGE_MAX = 8
CONSOLIDATION_DAYS = 3
CONSOLIDATION_RANGE = 0.04
OI_INCREASE_PCT = 5
FUNDING_RATE_MIN = -0.3
FUNDING_RATE_DAYS = 3
BUILD_BLACKLIST = [
    "USDCUSDT", "FDUSDUSDT", "TUSDUSDT", "DAIUSDT",
    "EURUSDT", "GBPUSDT", "JPYUSDT", "AUDUSDT",
]

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
            channel TEXT DEFAULT 'confirm',
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
    db_conn.execute("""
        CREATE TABLE IF NOT EXISTS trend_log (
            symbol TEXT,
            count INTEGER,
            first_at TIMESTAMP,
            sent_at TIMESTAMP
        )
    """)
    db_conn.execute("""
        CREATE TABLE IF NOT EXISTS channel_log (
            symbol TEXT,
            channel TEXT,
            sent_at TIMESTAMP
        )
    """)
    # 建仓状态表（新增）
    db_conn.execute("""
        CREATE TABLE IF NOT EXISTS building_state (
            symbol TEXT PRIMARY KEY,
            level TEXT,
            updated_at TIMESTAMP
        )
    """)

# ===================== 北京时间辅助函数 =====================
def now_beijing():
    return datetime.now(timezone(timedelta(hours=8)))

def record_channel(symbol, channel):
    with db_lock:
        db_conn.execute(
            "INSERT INTO channel_log VALUES (?,?,?)",
            (symbol, channel, now_beijing())
        )
        db_conn.commit()

def get_cross_channel_history(symbol, hours=24):
    cutoff = now_beijing() - timedelta(hours=hours)
    with db_lock:
        cur = db_conn.execute(
            "SELECT DISTINCT channel FROM channel_log WHERE symbol=? AND sent_at > ?",
            (symbol, cutoff)
        )
        channels = set(row[0] for row in cur.fetchall())
    if not channels:
        return ""
    labels = []
    if "surge" in channels:
        labels.append("⚡速报已报")
    if "confirm" in channels:
        labels.append("🚀确认已报")
    if "trend" in channels:
        labels.append("📈趋势已报")
    if "building" in channels:          # 新增识别
        labels.append("📐建仓已报")
    if labels:
        return f"📌 历史信号：{' | '.join(labels)}\n"
    return ""

def cleanup_old_data():
    cutoff = datetime.now() - timedelta(days=7)
    with db_lock:
        db_conn.execute("DELETE FROM sent_log WHERE sent_at < ?", (cutoff,))
        db_conn.execute("DELETE FROM trend_log WHERE sent_at < ?", (cutoff,))
        db_conn.execute("DELETE FROM channel_log WHERE sent_at < ?", (cutoff,))
        db_conn.commit()
    logger.info("🗑️ 已清理7天前的旧数据")

# ===================== 原有数据库函数（增加 channel 参数）======================
def should_alert(symbol, direction, channel='confirm'):
    cutoff = datetime.now() - timedelta(hours=DEDUP_HOURS)
    with db_lock:
        cur = db_conn.execute(
            "SELECT 1 FROM sent_log WHERE symbol=? AND direction=? AND channel=? AND sent_at > ?",
            (symbol, direction, channel, cutoff))
        return cur.fetchone() is None

def mark_alerted(symbol, direction, channel='confirm'):
    with db_lock:
        db_conn.execute(
            "INSERT INTO sent_log VALUES (?,?,?,?)",
            (symbol, direction, channel, datetime.now()))
        db_conn.commit()

def should_trend_alert(symbol):
    with db_lock:
        cur = db_conn.execute(
            "SELECT count, first_at, sent_at FROM trend_log WHERE symbol=? ORDER BY sent_at DESC LIMIT 1",
            (symbol,))
        row = cur.fetchone()
    if row is None:
        return True
    count, first_at_str, sent_at_str = row
    first_at = datetime.fromisoformat(first_at_str) if isinstance(first_at_str, str) else first_at_str
    last_sent = datetime.fromisoformat(sent_at_str) if isinstance(sent_at_str, str) else sent_at_str
    if (datetime.now() - first_at).total_seconds() > 48 * 3600:
        return False
    if (datetime.now() - last_sent).total_seconds() < TREND_DEDUP_HOURS * 3600:
        return False
    if count >= TREND_MAX_COUNT:
        return False
    return True

def get_trend_count(symbol):
    with db_lock:
        cur = db_conn.execute(
            "SELECT count FROM trend_log WHERE symbol=? ORDER BY sent_at DESC LIMIT 1",
            (symbol,))
        row = cur.fetchone()
        if row:
            return row[0]
        return 0

def mark_trend_alerted(symbol):
    with db_lock:
        cur = db_conn.execute(
            "SELECT count, first_at FROM trend_log WHERE symbol=? ORDER BY sent_at DESC LIMIT 1",
            (symbol,))
        row = cur.fetchone()
        if row is None:
            db_conn.execute(
                "INSERT INTO trend_log VALUES (?,?,?,?)",
                (symbol, 1, datetime.now(), datetime.now()))
        else:
            old_count, first_at = row
            first_at_dt = datetime.fromisoformat(first_at) if isinstance(first_at, str) else first_at
            if (datetime.now() - first_at_dt).total_seconds() > 48 * 3600:
                db_conn.execute(
                    "INSERT INTO trend_log VALUES (?,?,?,?)",
                    (symbol, 1, datetime.now(), datetime.now()))
            else:
                db_conn.execute(
                    "INSERT INTO trend_log VALUES (?,?,?,?)",
                    (symbol, old_count + 1, first_at, datetime.now()))
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
high_volume_symbols = set()
ticker_cache = {}  # 新增：缓存 ticker 数据供建仓通道使用

def get_symbols():
    global high_volume_symbols, ticker_cache
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
        cache = {}
        for t in tickers:
            try:
                vol = float(t.get('quoteVolume', 0))
                sym = t['symbol']
                cache[sym] = {
                    'volume': vol,
                    'change': float(t['priceChangePercent'])
                }
                if vol > MIN_24H_VOLUME_USDT:
                    hv.add(sym)
            except:
                pass
        high_volume_symbols = hv
        ticker_cache = cache
        logger.info(f"全部永续: {len(all_syms)} → 全部扫描 | {MIN_24H_VOLUME_USDT//10000}万以上: {len(hv)}")
        return all_syms
    except Exception as e:
        logger.error(f"获取币种失败: {e}")
        return []

def get_klines(symbol, interval, limit):
    r = session.get(f"{BINANCE_API}/fapi/v1/klines",
                    params={"symbol": symbol, "interval": interval, "limit": limit})
    r.raise_for_status()
    return r.json()

# 新增：解析 K 线为字典，用于建仓通道
def get_klines_parsed(symbol, interval="1d", limit=35):
    raw = get_klines(symbol, interval, limit)
    if not raw:
        return None
    return [{
        "o": float(k[1]),
        "h": float(k[2]),
        "l": float(k[3]),
        "c": float(k[4]),
        "v": float(k[5]),
        "qv": float(k[7]),
        "t": k[0],
    } for k in raw]

def get_oi_history(symbol, period="1h", limit=2):
    r = session.get(f"{BINANCE_API}/futures/data/openInterestHist",
                    params={"symbol": symbol, "period": period, "limit": limit})
    r.raise_for_status()
    return r.json()

def get_funding_rate(symbol):
    r = session.get(f"{BINANCE_API}/fapi/v1/premiumIndex", params={"symbol": symbol})
    r.raise_for_status()
    return float(r.json()['lastFundingRate'])

# 新增：历史资金费率，建仓通道专用
def get_funding_rate_history(symbol, limit=10):
    try:
        r = session.get(f"{BINANCE_API}/fapi/v1/fundingRate",
                        params={"symbol": symbol, "limit": limit})
        r.raise_for_status()
        data = r.json()
        if isinstance(data, list):
            return [float(d["fundingRate"]) * 100 for d in data]
    except:
        pass
    return None

# ===================== 通知模块 =====================
def send_tg(text):
    try:
        session.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                     json={"chat_id": TELEGRAM_CHAT_ID, "text": text})
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

# ===================== 精简后的三通道通知函数（未改动，仅保留） =====================
def notify_signal(sig):
    emoji = "🚀" if sig['direction'] == 'LONG' else "📉"
    history_text = get_cross_channel_history(sig['symbol'], hours=24)
    text = (
        f"{emoji} {sig['symbol']} — {sig['direction']} 已确认\n"
        f"{history_text}"
        f"━━━━━━━━━━━━━━━\n"
        f"💰 价格: ${sig['price']:.6f}\n"
        f"📊 OI 1h: {sig['oi_change']*100:+.2f}%\n"
        f"📈 价格 1h: {sig['price_change']*100:+.2f}%\n"
        f"🔊 量能比: {sig['volume_ratio']:.2f}x\n"
        f"💵 资金费率: {sig['funding']*100:.4f}%\n"
        f"⭐ 信号强度: {sig['score']}/5\n"
        f"━━━━━━━━━━━━━━━\n"
        f"⏰ {now_beijing().strftime('%Y-%m-%d %H:%M:%S')}"
    )
    notify_all(text)

def notify_surge(symbol, price, start_pct):
    history_text = get_cross_channel_history(symbol, hours=24)
    text = (
        f"⚡ {symbol} — 快速拉升\n"
        f"{history_text}"
        f"━━━━━━━━━━━━━━━\n"
        f"💰 价格: ${price:.6f}\n"
        f"📈 3M 启动涨幅: +{start_pct:.2f}%\n"
        f"━━━━━━━━━━━━━━━\n"
        f"⏰ {now_beijing().strftime('%Y-%m-%d %H:%M:%S')}"
    )
    notify_all(text)

def notify_trend(symbol, price, change_24h, bull_count, at_high_pct, trend_count):
    history_text = get_cross_channel_history(symbol, hours=24)
    text = (
        f"📈 {symbol} — 持续上涨趋势\n"
        f"{history_text}"
        f"━━━━━━━━━━━━━━━\n"
        f"💰 价格: ${price:.6f}\n"
        f"📊 24h 涨幅: {change_24h:+.2f}%\n"
        f"📈 24h 阳线: {bull_count}/24\n"
        f"📍 距24h最高: {at_high_pct:.1f}%\n"
        f"🔄 趋势提醒: 第 {trend_count} 次\n"
        f"━━━━━━━━━━━━━━━\n"
        f"⏰ {now_beijing().strftime('%Y-%m-%d %H:%M:%S')}"
    )
    notify_all(text)

# ===================== 位置分析（保留但不再被调用）=====================
# (保持原样，省略)

# ===================== 因子解读（保留但不再被调用）=====================
# (保持原样，省略)

# ===================== 状态缓存（原三通道） =====================
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
    "source": None
})

pending_confirm = set()
pending_surge = {}
active_tracking = {}
lock = Lock()

# ===================== 触发层（含速报检测） =====================
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
                if sym in high_volume_symbols:
                    if start_pct >= SURGE_HH_PCT:
                        with lock:
                            pending_surge[sym] = (price_now, start_pct)
                        logger.info(f"⚡ 速报触发: {sym} (start_pct={start_pct:.2f}%)")
                    with lock:
                        pending_confirm.add(sym)
                    logger.info(f"🟢 触发: {sym} (start_pct={start_pct:.2f}%)")
                else:
                    logger.debug(f"🔇 触发但成交额不足: {sym}")
        sb["daily_high"] = max(sb["daily_high"], price_now)
        sb["daily_low"] = min(sb["daily_low"], price_now)
    except Exception:
        pass

# ===================== 确认层 =====================
def analyze_symbol(symbol):
    try:
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
        oi_contributed = abs(oi_change) > 0.01
        vol_contributed = volume_ratio > 1.5
        if not (oi_contributed or vol_contributed):
            return None
        if bull >= MIN_FACTORS_TO_ALERT:
            if price_change_1h > MAX_1H_CHANGE:
                logger.info(f"⛔ {symbol} 否决: 1h涨幅 {price_change_1h*100:.2f}% 超 8%")
                return None
            return {
                "symbol": symbol,
                "direction": "LONG",
                "score": bull,
                "oi_change": oi_change,
                "price_change": price_change_1h,
                "volume_ratio": volume_ratio,
                "funding": funding,
                "price": closes[-1]
            }
        if bear >= MIN_FACTORS_TO_ALERT:
            if price_change_1h < -MAX_1H_CHANGE:
                logger.info(f"⛔ {symbol} 否决: 1h跌幅 {price_change_1h*100:.2f}% 超 8%")
                return None
            return {
                "symbol": symbol,
                "direction": "SHORT",
                "score": bear,
                "oi_change": oi_change,
                "price_change": price_change_1h,
                "volume_ratio": volume_ratio,
                "funding": funding,
                "price": closes[-1]
            }
        return None
    except Exception as e:
        logger.error(f"{symbol} 确认分析失败: {e}")
        return None

# ===================== 趋势通道扫描 =====================
def scan_trends(symbols):
    target_syms = [s for s in symbols if s in high_volume_symbols]
    if not target_syms:
        return
    def check_trend(sym):
        try:
            k1h = get_klines(sym, "1h", 24)
            if len(k1h) < 24:
                return None
            closes = [float(k[4]) for k in k1h]
            opens = [float(k[1]) for k in k1h]
            highs = [float(k[2]) for k in k1h]
            change_24h = (closes[-1] - closes[0]) / closes[0] * 100
            bull_count = sum(1 for i in range(len(closes)) if closes[i] > opens[i])
            high_24h = max(highs)
            at_high_pct = closes[-1] / high_24h * 100 if high_24h > 0 else 0
            if (change_24h > TREND_CHANGE_MIN and
                bull_count >= TREND_BULL_MIN and
                at_high_pct > TREND_AT_HIGH_MIN):
                return (sym, closes[-1], change_24h, bull_count, at_high_pct)
        except:
            pass
        return None
    with ThreadPoolExecutor(max_workers=MAX_WORKERS_TREND) as ex:
        results = list(ex.map(check_trend, target_syms))
    hits = 0
    for r in results:
        if r and should_trend_alert(r[0]):
            sym, price, change_24h, bull_count, at_high_pct = r
            trend_count = get_trend_count(sym) + 1
            notify_trend(sym, price, change_24h, bull_count, at_high_pct, trend_count)
            mark_trend_alerted(sym)
            record_channel(sym, "trend")
            hits += 1
            logger.info(f"📈 趋势信号: {sym} 24h涨幅{change_24h:+.2f}% (第{trend_count}次)")
    logger.info(f"📈 趋势扫描完成，发现 {hits} 个趋势信号")

# ===================== 追踪层（标明来源）=====================
def track_active():
    for sym in list(active_tracking.keys()):
        sb = active_tracking[sym]
        try:
            if sb["last_high"] is None or sb["base_low"] is None:
                del active_tracking[sym]
                continue
            source = sb.get("source", "confirm")
            source_tag = "⚡速报" if source == "surge" else "🚀确认"
            trigger_time = sb.get("trigger_time")
            if trigger_time is None:
                sb["trigger_time"] = datetime.now()
                trigger_time = sb["trigger_time"]
            tracking_hours = (datetime.now() - trigger_time).total_seconds() / 3600
            if tracking_hours >= TRACKING_TIMEOUT_HOURS:
                price_now_k = get_klines(sym, "3m", 1)
                p = float(price_now_k[-1][4]) if price_now_k else 0
                notify_all(
                    f"⏰ {sym} 已超过24小时无变化（{source_tag}）\n"
                    f"━━━━━━━━━━━━━━━\n"
                    f"当前价: {p:.6f}\n"
                    f"已撤出关注池"
                )
                sb["active"] = False
                del active_tracking[sym]
                logger.info(f"⏰ {sym} 追踪超时24h，已撤出（{source_tag}）")
                continue
            k3 = get_klines(sym, "3m", 6)
            highs = [float(x[2]) for x in k3]
            lows = [float(x[3]) for x in k3]
            price_now = float(k3[-1][4])
            drawdown = (sb["last_high"] - lows[-1]) / sb["last_high"]
            if drawdown >= DRAWDOWN_FAIL:
                notify_all(
                    f"❌ {sym} 信号失效（{source_tag}）\n"
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
                    f"🚀 {sym} 推进（第{sb['push_count']}次）（{source_tag}）\n"
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

# ===================== 噪音过滤 =====================
def market_too_noisy(symbols):
    sample = symbols[:MARKET_NOISE_SAMPLE]
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

# ===================== 日报（仅统计已推送的币）=====================
def generate_daily_report(report_date=None):
    if report_date is None:
        report_date = date.today()
    rows = []
    for sym, sb in state_b.items():
        if sb.get("first_price") is None:
            continue
        if sb.get("source") is None:
            continue
        first = sb["first_price"]
        high = sb.get("daily_high") or first
        low = sb.get("daily_low") or first
        source = sb.get("source", "confirm")
        source_label = "⚡速报" if source == "surge" else "🚀确认"
        rows.append({
            "币种": sym,
            "来源": source_label,
            "首次触发价": first,
            "当日最高": high,
            "当日最低": low,
            "当日涨幅(%)": round((high - first) / first * 100, 2),
            "推进次数": sb.get("push_times", 0),
        })
    if not rows:
        logger.info(f"📊 {report_date} 日报：无推送记录")
        notify_all(f"📊 {report_date} 日报：今日无推送记录")
        return
    df = pd.DataFrame(rows).sort_values("当日涨幅(%)", ascending=False)
    fname = f"日报_{report_date.strftime('%Y%m%d')}.xlsx"
    df.to_excel(fname, index=False)
    send_email_attach(f"盘面日报 {report_date}", "附件为今日全部推送记录", fname)
    logger.info(f"📊 已生成日报: {fname}，共 {len(rows)} 条记录")
    notify_all(f"📊 {report_date} 日报已发送，共 {len(rows)} 条推送记录")

# ===================== 建仓通道（完整移植，独立逻辑）=====================
def check_stage1_decline(klines):
    if not klines or len(klines) < 30:
        return None
    closes = [k["c"] for k in klines]
    vols = [k["qv"] for k in klines]
    price_now = closes[-1]
    price_7d_ago = closes[-8] if len(closes) >= 8 else closes[0]
    change_7d = (price_now - price_7d_ago) / price_7d_ago * 100
    if change_7d > DROP_7D_PCT:
        return None
    low_30d = min(closes[-30:])
    dist_to_low = (price_now - low_30d) / low_30d * 100
    if dist_to_low > NEAR_LOW_PCT:
        return None
    vol_ma20 = sum(vols[-20:]) / 20 if len(vols) >= 20 else sum(vols) / len(vols)
    recent_vols = vols[-VOL_SHRINK_DAYS:]
    shrink_ok = all(v < vol_ma20 for v in recent_vols)
    if not shrink_ok:
        return None
    change_30d = (price_now - closes[-30]) / closes[-30] * 100 if len(closes) >= 30 else 0
    return {
        "change_7d": change_7d,
        "change_30d": change_30d,
        "dist_to_low": dist_to_low,
        "low_30d": low_30d,
        "vol_ma20": vol_ma20,
    }

def check_stage2_surge(klines, stage1):
    if not klines or len(klines) < 10:
        return None
    vol_ma20 = stage1["vol_ma20"]
    for i in range(len(klines) - 5, len(klines) - 1):
        if i < 1:
            continue
        k = klines[i]
        prev = klines[i - 1]
        prev5_vols = [klines[j]["qv"] for j in range(max(0, i - 5), i)]
        if not prev5_vols:
            continue
        avg5 = sum(prev5_vols) / len(prev5_vols)
        change = (k["c"] - prev["c"]) / prev["c"] * 100
        if (k["qv"] >= avg5 * VOL_SURGE_MULT
                and SURGE_CHANGE_MIN <= change <= SURGE_CHANGE_MAX
                and k["c"] > prev["c"]):
            return {
                "surge_day": i,
                "surge_vol_ratio": k["qv"] / avg5,
                "surge_change": change,
                "surge_low": k["l"],
                "surge_close": k["c"],
            }
    return None

def check_stage3_consolidation(klines, stage2):
    if not klines:
        return None
    surge_day = stage2["surge_day"]
    surge_low = stage2["surge_low"]
    after = klines[surge_day + 1:]
    if len(after) < 1:
        return None
    if len(after) > CONSOLIDATION_DAYS:
        after = after[:CONSOLIDATION_DAYS]
    highs = [k["h"] for k in after]
    lows = [k["l"] for k in after]
    if not highs or not lows:
        return None
    avg_price = (max(highs) + min(lows)) / 2
    if avg_price == 0:
        return None
    range_pct = (max(highs) - min(lows)) / avg_price
    if range_pct > CONSOLIDATION_RANGE:
        return None
    broke_low = any(k["l"] < surge_low for k in after)
    if broke_low:
        return None
    lows_rising = True
    for i in range(1, len(after)):
        if after[i]["l"] < after[i - 1]["l"] * 0.998:
            lows_rising = False
            break
    return {
        "consolidation_days": len(after),
        "range_pct": range_pct * 100,
        "lows_rising": lows_rising,
        "held_above_surge_low": True,
    }

def check_oi_and_funding(symbol, stage2):
    oi_hist = get_oi_history(symbol, "1d", 5)
    funding = get_funding_rate_history(symbol, 10)
    result = {"oi_change": 0, "oi_increasing": True, "funding_avg": 0, "funding_ok": True}
    if oi_hist and len(oi_hist) >= 3:
        oi_recent = float(oi_hist[-1]['sumOpenInterest'])
        oi_before = float(oi_hist[-3]['sumOpenInterest'])
        if oi_before > 0:
            result["oi_change"] = (oi_recent - oi_before) / oi_before * 100
            result["oi_increasing"] = result["oi_change"] > OI_INCREASE_PCT
    if funding and len(funding) >= FUNDING_RATE_DAYS:
        recent_funding = funding[-FUNDING_RATE_DAYS:]
        result["funding_avg"] = sum(recent_funding) / len(recent_funding)
        result["funding_ok"] = not all(f < FUNDING_RATE_MIN for f in recent_funding)
    return result

def classify_signal(stage1, stage2, stage3, oi_data):
    if stage1 and not stage2 and not stage3:
        return "observe"
    if stage1 and stage2 and not stage3:
        return "watch"
    if stage1 and stage2 and stage3:
        if oi_data["oi_increasing"] and stage3["lows_rising"]:
            return "entry"
        return "watch"
    return None

def format_building_signal(sym, level, price, stage1, stage2, stage3, oi_data):
    icons = {"observe": "🟢", "watch": "🟡", "entry": "🔴"}
    labels = {"observe": "观察", "watch": "关注", "entry": "介入"}
    icon = icons.get(level, "⚪")
    label = labels.get(level, "未知")
    now = now_beijing().strftime("%Y-%m-%d %H:%M")
    history_text = get_cross_channel_history(sym, hours=24)
    lines = [
        f"{icon} 建仓信号 [{label}]",
        f"币种: {sym}",
        f"时间: {now}",
        f"价格: {price:.6f}",
        history_text.rstrip(),
        "",
        "📉 下跌结构",
        f" 7天跌幅: {stage1['change_7d']:.1f}%",
        f" 30天跌幅: {stage1['change_30d']:.1f}%",
        f" 距底部: {stage1['dist_to_low']:.1f}%",
    ]
    if stage2:
        lines += [
            "",
            "📊 底部放量",
            f" 放量倍数: {stage2['surge_vol_ratio']:.1f}x",
            f" 放量当日涨幅: {stage2['surge_change']:.1f}%",
        ]
    if stage3:
        lines += [
            "",
            "📐 横盘确认",
            f" 横盘天数: {stage3['consolidation_days']}天",
            f" 波动幅度: {stage3['range_pct']:.1f}%",
            f" 低点抬高: {'是' if stage3['lows_rising'] else '否'}",
        ]
    lines += [
        "",
        "📈 OI/资金费率",
        f" OI变化: {oi_data['oi_change']:+.1f}%",
        f" OI增加: {'是' if oi_data['oi_increasing'] else '否'}",
        f" 资金费率: {oi_data['funding_avg']:.4f}%",
    ]
    return "\n".join(lines)

def scan_building_signals():
    logger.info("📐 建仓通道开始扫描")
    # 筛选符合成交额的币种，排除黑名单
    candidates = [sym for sym, data in ticker_cache.items()
                  if data['volume'] >= MIN_VOLUME_24H_BUILD
                  and sym not in BUILD_BLACKLIST]
    if not candidates:
        logger.info("📐 无候选币种")
        return
    count = {"observe": 0, "watch": 0, "entry": 0}
    for sym in candidates:
        try:
            klines = get_klines_parsed(sym, "1d", 35)
            if not klines or len(klines) < 10:
                continue
            price = klines[-1]["c"]
            stage1 = check_stage1_decline(klines)
            if not stage1:
                continue
            stage2 = check_stage2_surge(klines, stage1)
            stage3 = None
            if stage2:
                stage3 = check_stage3_consolidation(klines, stage2)
            oi_data = check_oi_and_funding(sym, stage2)
            if not oi_data["funding_ok"]:
                continue
            level = classify_signal(stage1, stage2, stage3, oi_data)
            if not level:
                continue
            # 观察不发通知，只记录状态
            if level == "observe":
                with db_lock:
                    db_conn.execute(
                        "INSERT OR REPLACE INTO building_state VALUES (?,?,?)",
                        (sym, "observe", now_beijing()))
                    db_conn.commit()
                count["observe"] += 1
                continue
            # 关注/介入检查是否应推送（去重）
            if not should_alert(sym, "LONG", channel="building"):
                continue
            # 获取上次推送级别，避免同级别重复推送
            with db_lock:
                cur = db_conn.execute(
                    "SELECT level FROM building_state WHERE symbol=?", (sym,))
                row = cur.fetchone()
                prev_level = row[0] if row else None
            if prev_level == level:
                continue
            # 推送
            msg = format_building_signal(sym, level, price, stage1, stage2, stage3, oi_data)
            notify_all(msg)
            mark_alerted(sym, "LONG", channel="building")
            record_channel(sym, "building")
            with db_lock:
                db_conn.execute(
                    "INSERT OR REPLACE INTO building_state VALUES (?,?,?)",
                    (sym, level, now_beijing()))
                db_conn.commit()
            count[level] += 1
            logger.info(f"📐 建仓推送: {sym} {level}")
        except Exception as e:
            logger.error(f"📐 {sym} 建仓检测失败: {e}")
    logger.info(f"📐 建仓扫描完成: 观察{count['observe']} 关注{count['watch']} 介入{count['entry']}")

# ===================== 主循环 =====================
def main_loop():
    logger.info(f"🚀 融合监控系统启动（四通道版 · {MIN_24H_VOLUME_USDT//10000}万门槛 + 建仓通道）")
    notify_all(
        f"✅ 融合监控系统启动（四通道版）\n"
        f"🚀 通道1：确认信号（多因子验证）\n"
        f"⚡ 通道2：速报信号（3M HH ≥ 3% 直接推）\n"
        f"📈 通道3：趋势信号（24h 持续上涨检测）\n"
        f"📐 通道4：建仓信号（日线下跌→放量→横盘）\n"
        f"门槛：24h成交额 > {MIN_24H_VOLUME_USDT//10000}万\n"
        f"追踪：推进(+4%) / 失效(5%回撤) / 24h超时撤出"
    )
    symbols = get_symbols()
    if not symbols or not high_volume_symbols:
        logger.error("无可用币种或无法获取成交额数据，退出")
        notify_all("❌ 系统启动失败：无法获取币种列表或成交额数据")
        return
    last_refresh = time.time()
    last_report_day = None
    last_trend_scan = 0
    last_build_scan = 0  # 新增：建仓通道上次扫描时间
    while True:
        try:
            now_cn = datetime.now(timezone(timedelta(hours=8)))
            today = now_cn.date()
            if last_report_day is None or last_report_day != today:
                if last_report_day is not None:
                    yesterday = today - timedelta(days=1)
                    generate_daily_report(yesterday)
                    cleanup_old_data()
                else:
                    logger.info("📊 首次启动，跳过日报，从今天开始统计")
                last_report_day = today
                for sym, sb in state_b.items():
                    if sym in active_tracking:
                        continue
                    sb["daily_high"] = 0.0
                    sb["daily_low"] = 1e10
                    sb["push_times"] = 0
                    sb["first_price"] = None
                    sb["source"] = None
            if time.time() - last_refresh > SYMBOL_REFRESH_INTERVAL:
                new_syms = get_symbols()
                if new_syms:
                    symbols = new_syms
                    last_refresh = time.time()
                    if not high_volume_symbols:
                        logger.warning("刷新后成交额数据为空")
            if market_too_noisy(symbols):
                time.sleep(SCAN_INTERVAL)
                continue
            for sym in symbols:
                _ = state_b[sym]
            # 触发层扫描
            t0 = time.time()
            with ThreadPoolExecutor(max_workers=MAX_WORKERS_TRIGGER) as ex:
                list(ex.map(scan_trigger, symbols))
            logger.info(f"全市场扫描完成 ({time.time()-t0:.1f}s)，{len(symbols)}币，候选 {len(pending_confirm)} 个，速报 {len(pending_surge)} 个")
            # 速报通道推送
            with lock:
                surge_items = dict(pending_surge)
                pending_surge.clear()
            for sym, (price, start_pct) in surge_items.items():
                if should_alert(sym, "LONG", channel="surge"):
                    notify_surge(sym, price, start_pct)
                    mark_alerted(sym, "LONG", channel="surge")
                    record_channel(sym, "surge")
                    if sym not in active_tracking:
                        state_b[sym]["source"] = "surge"
                        active_tracking[sym] = state_b[sym]
                    logger.info(f"⚡ 速报已推送: {sym}")
            # 确认通道推送
            with lock:
                to_confirm = list(pending_confirm)[:MAX_CANDIDATES_PER_CYCLE]
                pending_confirm.clear()
            if to_confirm:
                already_surged = set(surge_items.keys())
                with ThreadPoolExecutor(max_workers=MAX_WORKERS_CONFIRM) as ex:
                    futures = {ex.submit(analyze_symbol, s): s for s in to_confirm}
                    for fu in as_completed(futures):
                        sig = fu.result()
                        sym = futures[fu]
                        if sig and should_alert(sig['symbol'], sig['direction'], channel="confirm"):
                            if sym in already_surged:
                                logger.info(f"🔇 {sym} 已速报，跳过确认推送")
                                if sym not in active_tracking:
                                    state_b[sym]["source"] = "surge"
                                    active_tracking[sym] = state_b[sym]
                            else:
                                logger.info(f"✅ 确认信号: {sig['symbol']} {sig['direction']} 强度{sig['score']}")
                                save_signal(sig)
                                notify_signal(sig)
                                mark_alerted(sig['symbol'], sig['direction'], channel="confirm")
                                record_channel(sig['symbol'], "confirm")
                                state_b[sig['symbol']]["source"] = "confirm"
                                active_tracking[sig['symbol']] = state_b[sig['symbol']]
                        else:
                            if sym not in already_surged:
                                state_b[sym]["active"] = False
            # 趋势通道扫描
            if time.time() - last_trend_scan >= TREND_SCAN_INTERVAL:
                scan_trends(symbols)
                last_trend_scan = time.time()
            # ─────────── 新增：建仓通道扫描 ───────────
            if time.time() - last_build_scan >= BUILD_SCAN_INTERVAL:
                scan_building_signals()
                last_build_scan = time.time()
            # ──────────────────────────────────────────
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