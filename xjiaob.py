# -*- coding: utf-8 -*-
"""
融合监控系统（四通道版 · 优化版）
通道0 🟡 启动预警：1M + 3M 前置雷达
通道1 🚀 启动确认：3~6分钟延续验证，慢启动延长至15分钟
通道2 ⚡ 速报信号：3M HH ≥ 2%
通道3 📈 趋势信号：1H K线趋势检测
通道4 📐 建仓信号：日线下跌→放量→横盘结构

修改说明：
- 成交额门槛 800 万
- 追高否决阈值 10%
- 3M结构使用已收盘K线；1M用于即时启动验证
- 15M/1H改为后级强化，不再挡住第一脚启动
- 动态推进：ATR + 涨幅 + 新高 + 回撤
- SQLite驱动每日00:05日报
- 启动静默15分钟
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

# Telegram / QQ 凭据（强烈建议改为环境变量，此处保留原方式仅用于演示）
TELEGRAM_TOKEN = "7874342652:AAFQKWIrSVszpi1z60ixnr-VYpXf26rG8UY"
TELEGRAM_CHAT_ID = "5408890841"

# QQ 邮箱
ENABLE_EMAIL = True
QQ_EMAIL = "1113496210@qq.com"
QQ_AUTH_CODE = "gnlxwgxduzexgeag"
EMAIL_TO = "1113496210@qq.com"
SMTP_HOST = "smtp.qq.com"
SMTP_PORT = 465

# 扫描节奏
SCAN_INTERVAL = 30
SYMBOL_REFRESH_INTERVAL = 3600
REQUEST_TIMEOUT = 10

# 成交额门槛（已改为800万）
MIN_24H_VOLUME_USDT = 8_000_000

ALT_BLACKLIST = ["BTCUSDT", "ETHUSDT"]

# ================= 新版启动雷达 =================
# 3M负责发现结构，1M负责确认即时攻击，不让慢周期指标卡住第一脚。
EARLY_HH_MIN = 0.8
EARLY_HH_MAX = 2.5
EARLY_1M_BODY_MIN = 0.25
EARLY_1M_VOL_MULT = 1.5
EARLY_3M_VOL_MULT = 1.3
EARLY_CLOSE_POSITION = 0.70
EARLY_BREAK_LOOKBACK = 5

FAST_CHECK_3M_SEC = 180
FAST_CHECK_6M_SEC = 360
SLOW_CHECK_15M_SEC = 900

FAST_MIN_PROGRESS = 0.008
SLOW_MIN_PROGRESS = 0.005

# 动态推进：固定百分比 + ATR
PUSH_ATR_PERIOD = 14
NORMAL_PUSH_MIN = 0.008
NORMAL_PUSH_ATR = 2.0
STRONG_PUSH_MIN = 0.015
STRONG_PUSH_ATR = 4.0
EXPLOSIVE_PUSH_MIN = 0.030
EXPLOSIVE_PUSH_ATR = 6.0

# 新版回撤
SOFT_DRAWDOWN = 0.008
EXTREME_DRAWDOWN = 0.05

# 启动失败冷却
EARLY_FAIL_COOLDOWN_SEC = 1800
LONG_FAIL_COOLDOWN_SEC = 3600

# 正式信号评分
FORMAL_SCORE_MIN = 4
STRONG_SCORE_MIN = 5

# 系统B兼容参数
DRAWDOWN_FAIL = EXTREME_DRAWDOWN
MAX_PUSH = 3
MAX_DAILY_ROUND = 2
MIN_PUSH_PCT = 0.01

# 速报通道
SURGE_HH_PCT = 2.0

# 趋势通道参数
TREND_SCAN_INTERVAL = 1800
TREND_CHANGE_MIN = 5.0
TREND_BULL_MIN = 10
TREND_AT_HIGH_MIN = 90
TREND_DEDUP_HOURS = 6
TREND_MAX_COUNT = 8

# 多因子强化确认层
OI_STRONG = 0.05
VOLUME_MULTIPLIER = 1.8
PRICE_CHANGE_MIN = 0.01
FUNDING_RATE_MAX = 0.001
MAX_1H_CHANGE = 0.08

# 追高风险
CHASE_4H_HIGH_DISTANCE = 0.01
CHASE_DAY_RANGE_TOP = 0.85
CHASE_MAX_RISK = 80

# BTC / 市场状态
BTC_PAUSE_15M = 0.01
MARKET_DOWN_RATIO = 0.60

# 噪音过滤
MARKET_NOISE_SAMPLE = 100
MARKET_NOISE_PCT = 1.2
MARKET_NOISE_RATIO = 0.80

# 去重
DEDUP_HOURS = 2
EARLY_DEDUP_MINUTES = 30

# 追踪
TRACKING_TIMEOUT_HOURS = 24
MOMENTUM_WEAK_MINUTES = 15
MOMENTUM_FAIL_MINUTES = 30

# 每日报告
DAILY_REPORT_HOUR = 0
DAILY_REPORT_MINUTE = 5
REPORT_KEEP_DAYS = 30

# 并发
MAX_WORKERS_TRIGGER = 30
MAX_WORKERS_CONFIRM = 15
MAX_WORKERS_EARLY = 20
MAX_CANDIDATES_PER_CYCLE = 30
MAX_WORKERS_TREND = 10

# 启动静默期（秒）
SILENT_PERIOD_SEC = 900
startup_time = 0   # 全局变量，在main_loop中赋值

# ────────────── 建仓通道专用参数 ──────────────
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
    db_conn.execute("""
        CREATE TABLE IF NOT EXISTS building_state (
            symbol TEXT PRIMARY KEY,
            level TEXT,
            updated_at TIMESTAMP
        )
    """)
    db_conn.execute("""
        CREATE TABLE IF NOT EXISTS early_signal_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            trigger_time TIMESTAMP NOT NULL,
            trigger_price REAL NOT NULL,
            trigger_high REAL,
            trigger_low REAL,
            start_pct REAL,
            early_score INTEGER DEFAULT 0,
            volume_ratio REAL DEFAULT 0,
            stage TEXT DEFAULT 'EARLY',
            result TEXT DEFAULT 'PENDING',
            confirm_time TIMESTAMP,
            confirm_price REAL,
            price_3m REAL,
            price_6m REAL,
            price_15m REAL,
            max_price REAL,
            min_price REAL,
            max_gain REAL,
            max_drawdown REAL,
            progress_class TEXT,
            chase_risk INTEGER DEFAULT 0,
            formal_score INTEGER DEFAULT 0,
            fail_reason TEXT,
            cooldown_until TIMESTAMP
        )
    """)
    db_conn.execute("""
        CREATE TABLE IF NOT EXISTS tracking_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT,
            early_id INTEGER,
            event_time TIMESTAMP,
            event_type TEXT,
            price REAL,
            change_pct REAL,
            drawdown_pct REAL,
            progress_class TEXT,
            note TEXT
        )
    """)
    db_conn.execute("""
        CREATE TABLE IF NOT EXISTS daily_report_status (
            report_date TEXT PRIMARY KEY,
            generated_at TIMESTAMP,
            filepath TEXT,
            email_ok INTEGER DEFAULT 0,
            attempts INTEGER DEFAULT 0,
            last_error TEXT
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
    if "building" in channels:
        labels.append("📐建仓已报")
    if "early" in channels:
        labels.append("🟡启动预警已报")
    if "momentum_weak" in channels:
        labels.append("🟡动能减弱已报")
    if "failure" in channels:
        labels.append("❌失效已报")
    if "push" in channels:
        labels.append("🚀推进已报")
    if "risk_reject" in channels:
        labels.append("⚠️风险过滤已报")
    if "expired" in channels:
        labels.append("⏰超时已报")
    if labels:
        return f"📌 历史信号：{' | '.join(labels)}\n"
    return ""


# ===================== 数据库函数（含 channel 参数）======================
def should_alert(symbol, direction, channel='confirm'):
    cutoff = now_beijing() - timedelta(hours=DEDUP_HOURS)
    with db_lock:
        cur = db_conn.execute(
            "SELECT 1 FROM sent_log WHERE symbol=? AND direction=? AND channel=? AND sent_at > ?",
            (symbol, direction, channel, cutoff))
        return cur.fetchone() is None

def mark_alerted(symbol, direction, channel='confirm'):
    with db_lock:
        db_conn.execute(
            "INSERT INTO sent_log VALUES (?,?,?,?)",
            (symbol, direction, channel, now_beijing()))
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
    if (now_beijing() - parse_ts(first_at)).total_seconds() > 48 * 3600:
        return False
    if (now_beijing() - parse_ts(last_sent)).total_seconds() < TREND_DEDUP_HOURS * 3600:
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
                (symbol, 1, now_beijing().isoformat(), now_beijing().isoformat()))
        else:
            old_count, first_at = row
            first_at_dt = datetime.fromisoformat(first_at) if isinstance(first_at, str) else first_at
            if (now_beijing() - parse_ts(first_at_dt)).total_seconds() > 48 * 3600:
                db_conn.execute(
                    "INSERT INTO trend_log VALUES (?,?,?,?)",
                    (symbol, 1, now_beijing().isoformat(), now_beijing().isoformat()))
            else:
                db_conn.execute(
                    "INSERT INTO trend_log VALUES (?,?,?,?)",
                    (symbol, old_count + 1, first_at, now_beijing().isoformat()))
        db_conn.commit()

def save_signal(sig):
    with db_lock:
        db_conn.execute("""
            INSERT INTO signals_history VALUES (?,?,?,?,?,?,?,?,?)
        """, (
            sig['symbol'], sig['direction'], sig['score'],
            sig['price'], sig['oi_change'], sig['price_change'],
            sig['volume_ratio'], sig['funding'], now_beijing().isoformat()
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
ticker_cache = {}

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
        r = session.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": text}
        )
        r.raise_for_status()
        return True
    except Exception as e:
        logger.error(f"TG发送失败: {e}")
        return False


def send_email_text(subject, content):
    if not ENABLE_EMAIL:
        return False
    server = None
    try:
        msg = MIMEText(content, "plain", "utf-8")
        msg["From"] = formataddr((str(Header("盘面监控", "utf-8")), QQ_EMAIL))
        msg["To"] = EMAIL_TO
        msg["Subject"] = Header(subject, "utf-8").encode()

        server = smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, timeout=REQUEST_TIMEOUT)
        server.login(QQ_EMAIL, QQ_AUTH_CODE)
        server.sendmail(
            QQ_EMAIL,
            [e.strip() for e in EMAIL_TO.split(",") if e.strip()],
            msg.as_string()
        )
        return True
    except Exception as e:
        logger.error(f"邮件发送失败: {e}")
        return False
    finally:
        if server:
            try:
                server.quit()
            except Exception:
                pass


def send_email_attach(subject, body, filepath):
    if not ENABLE_EMAIL:
        return False

    server = None
    try:
        msg = MIMEMultipart()
        msg["From"] = formataddr((str(Header("盘面监控", "utf-8")), QQ_EMAIL))
        msg["To"] = EMAIL_TO
        msg["Subject"] = Header(subject, "utf-8").encode()
        msg.attach(MIMEText(body, "plain", "utf-8"))

        with open(filepath, "rb") as f:
            part = MIMEApplication(f.read(), Name=os.path.basename(filepath))
            part["Content-Disposition"] = (
                f'attachment; filename="{os.path.basename(filepath)}"'
            )
            msg.attach(part)

        server = smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, timeout=REQUEST_TIMEOUT)
        server.login(QQ_EMAIL, QQ_AUTH_CODE)
        server.sendmail(
            QQ_EMAIL,
            [e.strip() for e in EMAIL_TO.split(",") if e.strip()],
            msg.as_string()
        )
        return True
    except Exception as e:
        logger.error(f"邮件附件发送失败: {e}")
        return False
    finally:
        if server:
            try:
                server.quit()
            except Exception:
                pass


def notify_tg_only(text, channel=None, symbol=None):
    ok = send_tg(text)
    if channel and symbol:
        record_channel(symbol, channel)
    return ok


def notify_tg_email(text, subject, channel=None, symbol=None):
    tg_ok = send_tg(text)
    email_ok = send_email_text(subject, text)
    if channel and symbol:
        record_channel(symbol, channel)
    return tg_ok, email_ok


def notify_all(text, subject="实时监控提示", channel=None, symbol=None):
    return notify_tg_email(text, subject, channel=channel, symbol=symbol)


# ===================== 实时信号文案 =====================
def notify_signal(sig):
    history_text = get_cross_channel_history(sig["symbol"], hours=24)
    score = sig.get("score", 0)
    max_score = sig.get("max_score", 6)
    quality = sig.get("quality_label", "正式启动")

    text = (
        f"🚀 {sig['symbol']} — {quality}\n"
        f"{history_text}"
        f"━━━━━━━━━━━━━━━\n"
        f"💰 价格: ${sig['price']:.8f}\n"
        f"📈 启动后涨幅: {sig.get('post_start_pct', 0):+.2f}%\n"
        f"🔊 量能比: {sig.get('volume_ratio', 0):.2f}x\n"
        f"📊 OI 1h: {sig.get('oi_change', 0)*100:+.2f}%\n"
        f"💵 资金费率: {sig.get('funding', 0)*100:.4f}%\n"
        f"📐 推进: {sig.get('progress_class', '正常') }\n"
        f"🛡 追高风险: {sig.get('chase_risk', 0)}/100\n"
        f"⭐ 综合评分: {score}/{max_score}\n"
        f"━━━━━━━━━━━━━━━\n"
        f"⏰ {now_beijing().strftime('%Y-%m-%d %H:%M:%S')}"
    )
    notify_tg_email(
        text,
        f"🚀 [正式启动] {sig['symbol']} 评分{score}/{max_score}",
        channel="confirm",
        symbol=sig["symbol"]
    )


def notify_surge(symbol, price, start_pct):
    history_text = get_cross_channel_history(symbol, hours=24)
    text = (
        f"⚡ {symbol} — 快速拉升\n"
        f"{history_text}"
        f"━━━━━━━━━━━━━━━\n"
        f"💰 价格: ${price:.8f}\n"
        f"📈 3M 启动涨幅: +{start_pct:.2f}%\n"
        f"━━━━━━━━━━━━━━━\n"
        f"⏰ {now_beijing().strftime('%Y-%m-%d %H:%M:%S')}"
    )
    notify_tg_email(
        text,
        f"⚡ [快速拉升] {symbol} +{start_pct:.2f}%",
        channel="surge",
        symbol=symbol
    )


def notify_trend(symbol, price, change_24h, bull_count, at_high_pct, trend_count):
    history_text = get_cross_channel_history(symbol, hours=24)
    text = (
        f"📈 {symbol} — 持续上涨趋势\n"
        f"{history_text}"
        f"━━━━━━━━━━━━━━━\n"
        f"💰 价格: ${price:.8f}\n"
        f"📊 24h 涨幅: {change_24h:+.2f}%\n"
        f"📈 24h 阳线: {bull_count}/24\n"
        f"📍 距24h最高: {at_high_pct:.1f}%\n"
        f"🔄 趋势提醒: 第 {trend_count} 次\n"
        f"━━━━━━━━━━━━━━━\n"
        f"⏰ {now_beijing().strftime('%Y-%m-%d %H:%M:%S')}"
    )
    notify_tg_email(
        text,
        f"📈 [趋势信号] {symbol}",
        channel="trend",
        symbol=symbol
    )


def notify_early(symbol, price, start_pct, early_score, volume_ratio, break_ok):
    history_text = get_cross_channel_history(symbol, hours=24)
    text = (
        f"🟡 {symbol} — 启动预警\n"
        f"{history_text}"
        f"━━━━━━━━━━━━━━━\n"
        f"💰 价格: ${price:.8f}\n"
        f"📈 3M启动幅度: +{start_pct:.2f}%\n"
        f"🔊 1M/3M量能: {volume_ratio:.2f}x\n"
        f"📐 结构突破: {'是' if break_ok else '否'}\n"
        f"⭐ 启动质量: {early_score}/4\n"
        f"⏱ 进入3~6分钟延续验证\n"
        f"━━━━━━━━━━━━━━━\n"
        f"⏰ {now_beijing().strftime('%Y-%m-%d %H:%M:%S')}"
    )
    notify_tg_only(text, channel="early", symbol=symbol)


def notify_momentum_weak(symbol, price, start_pct, minutes_elapsed, progress_class):
    text = (
        f"🟡 {symbol} — 动能减弱\n"
        f"━━━━━━━━━━━━━━━\n"
        f"当前价: {price:.8f}\n"
        f"启动后: {start_pct:+.2f}%\n"
        f"经过: {minutes_elapsed}分钟\n"
        f"推进状态: {progress_class}\n"
        f"说明: 暂未失效，但暂时没有有效新高\n"
        f"━━━━━━━━━━━━━━━\n"
        f"⏰ {now_beijing().strftime('%Y-%m-%d %H:%M:%S')}"
    )
    notify_tg_only(text, channel="momentum_weak", symbol=symbol)


def notify_failure(symbol, price, reason, drawdown_pct):
    text = (
        f"❌ {symbol} — 启动失效\n"
        f"━━━━━━━━━━━━━━━\n"
        f"当前价: {price:.8f}\n"
        f"回撤: {drawdown_pct:+.2f}%\n"
        f"原因: {reason}\n"
        f"已退出观察池并进入冷却。\n"
        f"━━━━━━━━━━━━━━━\n"
        f"⏰ {now_beijing().strftime('%Y-%m-%d %H:%M:%S')}"
    )
    notify_tg_only(text, channel="failure", symbol=symbol)


# ===================== 建仓/通道兼容辅助 =====================

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
    "source": None
})

# 新版状态机
pending_early = {}
active_tracking = {}
early_lock = Lock()

# 兼容旧集合，避免原逻辑引用报错
pending_confirm = set()
pending_surge = {}
lock = Lock()

# 每2分钟由主循环刷新，正式确认层直接读取，避免重复扫描100个币。
market_state_cache = {"btc_change_15m": 0.0, "market_down_ratio": 0.0}


def parse_ts(value):
    if isinstance(value, datetime):
        dt = value
    elif not value:
        return now_beijing()
    else:
        try:
            dt = datetime.fromisoformat(str(value))
        except Exception:
            return now_beijing()
    # 兼容历史数据库中可能存在的无时区时间，统一解释为北京时间。
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone(timedelta(hours=8)))
    return dt


def iso_now():
    return now_beijing().isoformat()


def add_tracking_event(symbol, early_id, event_type, price=None,
                       change_pct=None, drawdown_pct=None,
                       progress_class=None, note=None):
    with db_lock:
        db_conn.execute("""
            INSERT INTO tracking_events
            (symbol, early_id, event_time, event_type, price,
             change_pct, drawdown_pct, progress_class, note)
            VALUES (?,?,?,?,?,?,?,?,?)
        """, (
            symbol, early_id, now_beijing().isoformat(), event_type,
            price, change_pct, drawdown_pct, progress_class, note
        ))
        db_conn.commit()


def update_early_row(early_id, **kwargs):
    if not kwargs:
        return
    allowed = {
        "stage", "result", "confirm_time", "confirm_price",
        "price_3m", "price_6m", "price_15m", "max_price",
        "min_price", "max_gain", "max_drawdown",
        "progress_class", "chase_risk", "formal_score",
        "fail_reason", "cooldown_until"
    }
    kwargs = {k: v for k, v in kwargs.items() if k in allowed}
    if not kwargs:
        return
    assignments = ", ".join(f"{k}=?" for k in kwargs)
    values = [v.isoformat() if isinstance(v, datetime) else v for v in kwargs.values()]
    values.append(early_id)
    with db_lock:
        db_conn.execute(
            f"UPDATE early_signal_log SET {assignments} WHERE id=?",
            values
        )
        db_conn.commit()


def get_existing_early(symbol, within_minutes=30):
    cutoff = now_beijing() - timedelta(minutes=within_minutes)
    with db_lock:
        cur = db_conn.execute("""
            SELECT id, trigger_time, trigger_price, trigger_low, stage, result
            FROM early_signal_log
            WHERE symbol=? AND trigger_time>? AND result IN ('PENDING','CONFIRMED')
            ORDER BY id DESC LIMIT 1
        """, (symbol, cutoff.isoformat()))
        return cur.fetchone()


def is_symbol_in_cooldown(symbol):
    now = now_beijing()
    with db_lock:
        cur = db_conn.execute("""
            SELECT cooldown_until
            FROM early_signal_log
            WHERE symbol=? AND cooldown_until IS NOT NULL
            ORDER BY id DESC LIMIT 1
        """, (symbol,))
        row = cur.fetchone()
    if not row or not row[0]:
        return False
    try:
        return parse_ts(row[0]) > now
    except Exception:
        return False


def atr_percent_from_1m(klines, period=PUSH_ATR_PERIOD):
    if not klines or len(klines) < period + 1:
        return 0.0
    trs = []
    prev_close = None
    for k in klines[-(period+1):]:
        high = float(k[2])
        low = float(k[3])
        close = float(k[4])
        if prev_close is None:
            tr = high - low
        else:
            tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        if close > 0:
            trs.append(tr / close)
        prev_close = close
    return (sum(trs) / len(trs)) if trs else 0.0


def classify_progress(change_pct_decimal, atr_pct_decimal):
    if atr_pct_decimal <= 0:
        normal_threshold = NORMAL_PUSH_MIN
        strong_threshold = STRONG_PUSH_MIN
        explosive_threshold = EXPLOSIVE_PUSH_MIN
    else:
        normal_threshold = max(NORMAL_PUSH_MIN, atr_pct_decimal * NORMAL_PUSH_ATR)
        strong_threshold = max(STRONG_PUSH_MIN, atr_pct_decimal * STRONG_PUSH_ATR)
        explosive_threshold = max(EXPLOSIVE_PUSH_MIN, atr_pct_decimal * EXPLOSIVE_PUSH_ATR)

    if change_pct_decimal >= explosive_threshold:
        return "爆发", explosive_threshold
    if change_pct_decimal >= strong_threshold:
        return "强推进", strong_threshold
    if change_pct_decimal >= normal_threshold:
        return "正常推进", normal_threshold
    if change_pct_decimal > 0:
        return "弱推进", normal_threshold
    return "无推进", normal_threshold


def get_short_term_vwap(symbol, limit=60):
    try:
        k = get_klines(symbol, "1m", limit)
        if not k:
            return None
        pv = 0.0
        vv = 0.0
        for x in k:
            typical = (float(x[2]) + float(x[3]) + float(x[4])) / 3.0
            vol = float(x[5])
            pv += typical * vol
            vv += vol
        return pv / vv if vv > 0 else None
    except Exception:
        return None


def get_4h_and_day_position(symbol):
    try:
        k4 = get_klines(symbol, "4h", 30)
        kd = get_klines(symbol, "1h", 24)
        if not k4 or not kd:
            return 0, 0, 0
        price = float(kd[-1][4])
        high4 = max(float(x[2]) for x in k4)
        high24 = max(float(x[2]) for x in kd)
        low24 = min(float(x[3]) for x in kd)
        day_range_pos = (
            (price - low24) / (high24 - low24)
            if high24 > low24 else 0.5
        )
        dist4 = (high4 - price) / price if price > 0 else 1.0
        change1h = (
            (float(kd[-1][4]) - float(kd[-2][4])) / float(kd[-2][4])
            if len(kd) >= 2 and float(kd[-2][4]) > 0 else 0
        )
        return dist4, day_range_pos, change1h
    except Exception:
        return 0, 0, 0


def compute_chase_risk(symbol, price):
    risk = 0
    try:
        dist4, day_pos, change1h = get_4h_and_day_position(symbol)
        if change1h > MAX_1H_CHANGE:
            risk += 35
        elif change1h > MAX_1H_CHANGE * 0.75:
            risk += 20

        if 0 < dist4 < CHASE_4H_HIGH_DISTANCE:
            risk += 25

        if day_pos >= CHASE_DAY_RANGE_TOP:
            risk += 20

        vwap = get_short_term_vwap(symbol, 60)
        if vwap and price > vwap:
            premium = (price - vwap) / vwap
            if premium > 0.02:
                risk += 20
            elif premium > 0.01:
                risk += 10

        return min(100, risk)
    except Exception:
        return risk


def get_market_state(symbols=None):
    try:
        btc = get_klines("BTCUSDT", "1m", 20)
        if len(btc) >= 16:
            btc_change_15m = (
                (float(btc[-1][4]) - float(btc[-16][4])) /
                float(btc[-16][4])
            )
        else:
            btc_change_15m = 0.0
    except Exception:
        btc_change_15m = 0.0

    down_ratio = 0.0
    if symbols:
        sample = [s for s in symbols if s in high_volume_symbols][:100]
        if sample:
            def _one(s):
                try:
                    k = get_klines(s, "1m", 16)
                    if len(k) < 16:
                        return 0
                    return 1 if float(k[-1][4]) < float(k[-16][4]) else 0
                except Exception:
                    return 0
            with ThreadPoolExecutor(max_workers=15) as ex:
                vals = list(ex.map(_one, sample))
            down_ratio = sum(vals) / len(vals)

    return {
        "btc_change_15m": btc_change_15m,
        "market_down_ratio": down_ratio
    }


def insert_early_signal(symbol, trigger_price, trigger_high, trigger_low,
                        start_pct, early_score, volume_ratio):
    with db_lock:
        cur = db_conn.execute("""
            INSERT INTO early_signal_log
            (symbol, trigger_time, trigger_price, trigger_high, trigger_low,
             start_pct, early_score, volume_ratio, stage, result,
             max_price, min_price)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            symbol, now_beijing().isoformat(), trigger_price, trigger_high,
            trigger_low, start_pct, early_score, volume_ratio,
            "EARLY", "PENDING", trigger_price, trigger_price
        ))
        early_id = cur.lastrowid
        db_conn.commit()
    return early_id

# ===================== 启动雷达 =====================
def scan_trigger(sym):
    """早期雷达：3M已收盘结构 + 当前1M攻击，不再等待15M全阳。"""
    try:
        if sym not in high_volume_symbols:
            return

        # 已在观察 / 正式跟踪，避免重复创建
        if sym in pending_early or sym in active_tracking:
            return

        existing = get_existing_early(sym, EARLY_DEDUP_MINUTES)
        if existing or is_symbol_in_cooldown(sym):
            return

        sb = state_b[sym]
        today = now_beijing().date()
        if sb["day"] != today:
            sb["day"] = today
            sb["daily_round"] = 0
            if sym not in active_tracking:
                sb["active"] = False
                sb["push_count"] = 0

        if sb["daily_round"] >= MAX_DAILY_ROUND:
            return

        k3 = get_klines(sym, "3m", 8)
        if len(k3) < 6:
            return

        # 使用已经收盘的3M K线：最后一根未必收盘，因此排除最后一根。
        closed3 = k3[:-1]
        highs = [float(x[2]) for x in closed3]
        lows = [float(x[3]) for x in closed3]
        closes = [float(x[4]) for x in closed3]
        opens = [float(x[1]) for x in closed3]
        vols = [float(x[5]) for x in closed3]

        hh = highs[-3] < highs[-2] < highs[-1]
        start_pct = (
            (highs[-1] - lows[-3]) / lows[-3] * 100
            if lows[-3] > 0 else 0
        )
        if not hh or not (EARLY_HH_MIN <= start_pct <= EARLY_HH_MAX):
            return

        current1 = get_klines(sym, "1m", 12)
        if len(current1) < 6:
            return

        last = current1[-1]
        prev = current1[-2]
        last_o, last_h, last_l, last_c, last_v = map(float, [
            last[1], last[2], last[3], last[4], last[5]
        ])
        prev_h = float(prev[2])

        body_pct = (last_c - last_o) / last_o * 100 if last_o > 0 else 0
        avg_v = sum(float(x[5]) for x in current1[:-1]) / max(1, len(current1)-1)
        vol_ratio_1m = last_v / avg_v if avg_v > 0 else 0

        high_low_range = last_h - last_l
        close_pos = (
            (last_c - last_l) / high_low_range
            if high_low_range > 0 else 0.5
        )

        prior3 = max(highs[-EARLY_BREAK_LOOKBACK-1:-1]) if len(highs) >= EARLY_BREAK_LOOKBACK+1 else highs[-2]
        break_ok = last_c > max(prev_h, prior3)

        score = 0
        score += 1  # 3M HH
        if body_pct >= EARLY_1M_BODY_MIN:
            score += 1
        if vol_ratio_1m >= EARLY_1M_VOL_MULT:
            score += 1
        if close_pos >= EARLY_CLOSE_POSITION or break_ok:
            score += 1

        # 至少达到3/4才发“启动预警”
        if score < 3:
            return

        price_now = last_c
        early_id = insert_early_signal(
            sym, price_now, highs[-1], lows[-3],
            start_pct, score, vol_ratio_1m
        )

        entry = {
            "id": early_id,
            "symbol": sym,
            "trigger_time": time.time(),
            "trigger_dt": now_beijing(),
            "trigger_price": price_now,
            "trigger_high": highs[-1],
            "trigger_low": lows[-3],
            "start_pct": start_pct,
            "early_score": score,
            "volume_ratio": vol_ratio_1m,
            "max_price": price_now,
            "min_price": price_now,
            "last_high": highs[-1],
            "highest_price": price_now,
            "last_check_price": price_now,
            "check_3m_done": False,
            "check_6m_done": False,
            "check_15m_done": False,
            "weak_notified": False,
            "source": "early",
            "stage": "EARLY",
            "active": False,
            # 新增三个键
            "new_high_observed": False,
            "max_gain_seen": -1.0,
            "max_drawdown_seen": 0.0,
        }

        with early_lock:
            pending_early[sym] = entry

        sb["daily_round"] += 1
        sb["first_price"] = price_now
        sb["daily_high"] = price_now
        sb["daily_low"] = price_now
        sb["trigger_time"] = now_beijing()
        sb["source"] = "early"

        if time.time() - startup_time >= SILENT_PERIOD_SEC:
            notify_early(
                sym, price_now, start_pct, score,
                vol_ratio_1m, break_ok
            )
        else:
            logger.info(f"⏳ 静默期，启动预警不推送: {sym}")

        logger.info(
            f"🟡 启动预警: {sym} "
            f"HH={start_pct:.2f}% score={score}/4 vol={vol_ratio_1m:.2f}x"
        )

        # >=2%同时保留快速拉升通道，但不因此重复进入early
        if start_pct >= SURGE_HH_PCT and time.time() - startup_time >= SILENT_PERIOD_SEC:
            if should_alert(sym, "LONG", channel="surge"):
                notify_surge(sym, price_now, start_pct)
                mark_alerted(sym, "LONG", channel="surge")

    except Exception as e:
        logger.error(f"{sym} 启动雷达失败: {e}")


# ===================== 正式确认/强化评分 =====================
def analyze_symbol(symbol, early=None, market_state=None):
    """正式确认层：启动通过后才运行，6分制，不再依赖3根15M全阳硬门槛。"""
    try:
        if symbol not in high_volume_symbols:
            return None

        k5 = get_klines(symbol, "5m", 24)
        k15 = get_klines(symbol, "15m", 12)
        if len(k5) < 12 or len(k15) < 6:
            return None

        # 1H OI
        oi_hist = get_oi_history(symbol, "1h", 2)
        if len(oi_hist) >= 2:
            oi_prev = float(oi_hist[0]["sumOpenInterest"])
            oi_now = float(oi_hist[-1]["sumOpenInterest"])
            oi_change = (oi_now - oi_prev) / oi_prev if oi_prev > 0 else 0
        else:
            oi_change = 0

        funding = get_funding_rate(symbol)

        # 1) 5M结构
        closes5 = [float(k[4]) for k in k5]
        highs5 = [float(k[2]) for k in k5]
        volumes5 = [float(k[5]) for k in k5]
        structure_ok = closes5[-1] > max(highs5[-11:-1])

        # 2) 启动后的继续创新高
        continued = False
        post_start_pct = 0.0
        if early:
            current = closes5[-1]
            post_start_pct = (
                (current - early["trigger_price"]) / early["trigger_price"]
                if early["trigger_price"] else 0
            )
            continued = current > early.get("trigger_high", early["trigger_price"])

        # 3) 最近3根5M量能
        base_vol = sum(volumes5[:-3]) / max(1, len(volumes5)-3)
        recent_vol = sum(volumes5[-3:]) / 3
        volume_ratio = recent_vol / base_vol if base_vol > 0 else 0
        volume_ok = volume_ratio >= VOLUME_MULTIPLIER

        # 4) 15M：至少2/3阳，并且最近一根实体结构强
        last3 = k15[-3:]
        bullish = sum(1 for k in last3 if float(k[4]) > float(k[1]))
        recent15_ok = (
            bullish >= 2 and
            float(last3[-1][4]) > float(last3[-2][2])
        )

        # 5) OI
        oi_ok = oi_change > 0

        # 6) Funding：不明显过热
        funding_ok = not (funding > FUNDING_RATE_MAX)

        # 7) 市场环境
        if market_state is None:
            market_state = market_state_cache.copy()
        btc_bad = abs(market_state["btc_change_15m"]) >= BTC_PAUSE_15M
        market_bad = market_state["market_down_ratio"] >= MARKET_DOWN_RATIO

        # 追高风险
        price = closes5[-1]
        chase_risk = compute_chase_risk(symbol, price)

        score = 0
        score += 1 if structure_ok else 0
        score += 1 if continued else 0
        score += 1 if volume_ok else 0
        score += 1 if oi_ok else 0
        score += 1 if (recent15_ok and funding_ok) else 0
        score += 1 if not btc_bad and not market_bad else 0

        # 风险否决
        one_hour_change = 0
        if len(closes5) >= 13:
            one_hour_change = (closes5[-1] - closes5[-13]) / closes5[-13]

        if one_hour_change > MAX_1H_CHANGE:
            return {
                "symbol": symbol, "direction": "LONG",
                "rejected": True, "reject_reason": "1h涨幅超过追高阈值",
                "price": price, "score": score, "max_score": 6, "chase_risk": chase_risk
            }
        if chase_risk >= CHASE_MAX_RISK:
            return {
                "symbol": symbol, "direction": "LONG",
                "rejected": True, "reject_reason": f"追高风险{chase_risk}/100",
                "price": price, "score": score, "max_score": 6, "chase_risk": chase_risk
            }

        if market_bad:
            return {
                "symbol": symbol, "direction": "LONG",
                "rejected": True, "reject_reason": "全市场下跌占比超过60%",
                "price": price, "score": score, "max_score": 6, "chase_risk": chase_risk
            }

        quality = (
            "强启动" if score >= STRONG_SCORE_MIN
            else "正式启动" if score >= FORMAL_SCORE_MIN
            else "弱启动"
        )

        k1_for_progress = get_klines(symbol, "1m", PUSH_ATR_PERIOD + 2)
        atr_for_progress = atr_percent_from_1m(k1_for_progress) if k1_for_progress else 0.0

        return {
            "symbol": symbol,
            "direction": "LONG",
            "score": score,
            "max_score": 6,
            "quality_label": quality,
            "price": price,
            "oi_change": oi_change,
            "volume_ratio": volume_ratio,
            "funding": funding,
            "post_start_pct": post_start_pct * 100,
            "progress_class": classify_progress(post_start_pct, atr_for_progress)[0],
            "chase_risk": chase_risk,
            "btc_change_15m": market_state["btc_change_15m"],
            "market_down_ratio": market_state["market_down_ratio"],
            "rejected": False
        }

    except Exception as e:
        logger.error(f"{symbol} 正式确认分析失败: {e}")
        return None


def process_early_validation():
    """每30秒运行：3m承接、6m推进、15m慢启动。"""
    now_ts = time.time()

    for sym in list(pending_early.keys()):
        with early_lock:
            entry = pending_early.get(sym)
        if not entry:
            continue

        try:
            elapsed = now_ts - entry["trigger_time"]
            k1 = get_klines(sym, "1m", 20)
            if not k1:
                continue

            price = float(k1[-1][4])
            lows = [float(x[3]) for x in k1]
            highs = [float(x[2]) for x in k1]

            # 先保存“本次检查前”的最高收盘/观察价，再判断当前是否形成新高。
            old_highest = entry.get("highest_price", entry["trigger_price"])
            entry["highest_price_before_check"] = old_highest
            current_window_high = max(highs[-3:])
            entry["max_price"] = max(entry["max_price"], price, current_window_high)
            entry["min_price"] = min(entry["min_price"], price, min(lows[-3:]))
            new_high = price > old_highest
            entry["highest_price"] = max(old_highest, entry["max_price"])

            gain = (
                (price - entry["trigger_price"]) / entry["trigger_price"]
                if entry["trigger_price"] > 0 else 0
            )
            drawdown = (
                (entry["max_price"] - price) / entry["max_price"]
                if entry["max_price"] > 0 else 0
            )

            # 更新历史极值
            entry["max_gain_seen"] = max(entry.get("max_gain_seen", -1), gain)
            entry["max_drawdown_seen"] = max(entry.get("max_drawdown_seen", 0), drawdown)

            # 如果出现新高，设置标志
            if new_high:
                entry["new_high_observed"] = True

            atr_pct = atr_percent_from_1m(k1)
            progress_class, normal_threshold = classify_progress(gain, atr_pct)

            update_early_row(
                entry["id"],
                max_price=entry["max_price"],
                min_price=entry["min_price"],
                max_gain=entry["max_gain_seen"],
                max_drawdown=entry["max_drawdown_seen"],
                progress_class=progress_class
            )

            # 硬失败：跌破启动基准低点
            if price < entry["trigger_low"]:
                reason = "跌破启动基准低点"
                cooldown = now_beijing() + timedelta(
                    seconds=EARLY_FAIL_COOLDOWN_SEC
                )
                update_early_row(
                    entry["id"],
                    result="FAILED",
                    stage="FAILED",
                    fail_reason=reason,
                    cooldown_until=cooldown
                )
                add_tracking_event(
                    sym, entry["id"], "FAILED", price=price,
                    change_pct=gain * 100,
                    drawdown_pct=drawdown * 100,
                    progress_class=progress_class,
                    note=reason
                )
                if elapsed >= FAST_CHECK_3M_SEC:
                    notify_failure(sym, price, reason, -drawdown * 100)
                with early_lock:
                    pending_early.pop(sym, None)
                continue

            # 极端回撤
            if drawdown >= EXTREME_DRAWDOWN:
                reason = "启动后最大回撤达到5%"
                cooldown = now_beijing() + timedelta(
                    seconds=LONG_FAIL_COOLDOWN_SEC
                )
                update_early_row(
                    entry["id"],
                    result="FAILED",
                    stage="FAILED",
                    fail_reason=reason,
                    cooldown_until=cooldown
                )
                add_tracking_event(
                    sym, entry["id"], "EXTREME_FAIL", price=price,
                    change_pct=gain * 100,
                    drawdown_pct=drawdown * 100,
                    progress_class=progress_class,
                    note=reason
                )
                notify_failure(sym, price, reason, -drawdown * 100)
                with early_lock:
                    pending_early.pop(sym, None)
                continue

            # 3分钟：看承接，不要求必须暴涨
            if elapsed >= FAST_CHECK_3M_SEC and not entry["check_3m_done"]:
                entry["check_3m_done"] = True
                entry["stage"] = "VALIDATING_6M"
                update_early_row(
                    entry["id"],
                    stage="VALIDATING_6M",
                    price_3m=price
                )
                add_tracking_event(
                    sym, entry["id"], "3M_CHECK", price=price,
                    change_pct=gain * 100,
                    drawdown_pct=drawdown * 100,
                    progress_class=progress_class,
                    note="3分钟承接检查"
                )

            # 6分钟：核心确认
            if elapsed >= FAST_CHECK_6M_SEC and not entry["check_6m_done"]:
                entry["check_6m_done"] = True

                # 当前价格、创新高、动态推进三者至少满足较强的组合。
                fast_ok = (
                    gain >= FAST_MIN_PROGRESS and
                    new_high and
                    price >= entry["trigger_price"]
                )

                # 慢启动：不给立即失败，进入15分钟观察
                slow_ok = (
                    gain >= SLOW_MIN_PROGRESS and
                    price >= entry["trigger_price"] and
                    drawdown < SOFT_DRAWDOWN * 1.5
                )

                if fast_ok or (slow_ok and progress_class in ("正常推进", "强推进", "爆发")):
                    entry["stage"] = "CONFIRMED"
                    entry["active"] = True
                    entry["confirm_price"] = price

                    update_early_row(
                        entry["id"],
                        result="CONFIRMED",
                        stage="CONFIRMED",
                        confirm_time=now_beijing(),
                        confirm_price=price,
                        price_6m=price,
                        progress_class=progress_class
                    )

                    add_tracking_event(
                        sym, entry["id"], "CONFIRMED", price=price,
                        change_pct=gain * 100,
                        drawdown_pct=drawdown * 100,
                        progress_class=progress_class,
                        note="6分钟启动确认"
                    )

                    # 正式确认：6M启动成立后，风险否决不得绕过邮件过滤。
                    sig = analyze_symbol(sym, entry, market_state_cache)
                    if sig and sig.get("rejected"):
                        notify_tg_only(
                            f"⚠️ {sym} — 启动成立，但被风险过滤\n"
                            f"当前价: {price:.8f}\n"
                            f"启动后涨幅: {gain*100:+.2f}%\n"
                            f"追高风险: {sig.get('chase_risk', 0)}/100\n"
                            f"原因: {sig.get('reject_reason', '未知风险')}\n"
                            f"建议：暂不追高，继续观察。",
                            channel="risk_reject", symbol=sym
                        )
                        update_early_row(
                            entry["id"],
                            formal_score=sig.get("score", 0),
                            chase_risk=sig.get("chase_risk", 0),
                            fail_reason=sig.get("reject_reason", "风险过滤")
                        )
                    elif sig and sig.get("score", 0) >= FORMAL_SCORE_MIN:
                        notify_signal(sig)
                        update_early_row(
                            entry["id"],
                            formal_score=sig["score"],
                            chase_risk=sig.get("chase_risk", 0)
                        )
                    else:
                        # 6M已确认，但慢指标未齐：仍发送一次正式启动邮件，明确后级未强化。
                        basic_sig = {
                            "symbol": sym,
                            "direction": "LONG",
                            "price": price,
                            "score": sig.get("score", 0) if sig else 0,
                            "max_score": 6,
                            "quality_label": "启动确认（后级强化不足）",
                            "oi_change": sig.get("oi_change", 0) if sig else 0,
                            "volume_ratio": sig.get("volume_ratio", 0) if sig else 0,
                            "funding": sig.get("funding", 0) if sig else 0,
                            "post_start_pct": gain * 100,
                            "progress_class": progress_class,
                            "chase_risk": sig.get("chase_risk", 0) if sig else 0,
                        }
                        notify_signal(basic_sig)

                    # 进入正式跟踪
                    active_tracking[sym] = entry
                    with early_lock:
                        pending_early.pop(sym, None)
                    continue

                # 6分钟没有形成明显推进：暂不立刻杀，延长到15分钟
                entry["stage"] = "WATCH_15M"
                update_early_row(
                    entry["id"],
                    stage="WATCH_15M",
                    price_6m=price,
                    progress_class=progress_class
                )
                add_tracking_event(
                    sym, entry["id"], "6M_WEAK", price=price,
                    change_pct=gain * 100,
                    drawdown_pct=drawdown * 100,
                    progress_class=progress_class,
                    note="6分钟未达到快速确认，延长到15分钟"
                )

            # 15分钟慢启动
            if elapsed >= SLOW_CHECK_15M_SEC and not entry["check_15m_done"]:
                entry["check_15m_done"] = True
                update_early_row(entry["id"], price_15m=price)

                slow_new_high = entry.get("new_high_observed", False)
                slow_confirm = (
                    gain >= SLOW_MIN_PROGRESS and
                    slow_new_high and
                    drawdown < SOFT_DRAWDOWN * 1.5
                )

                if slow_confirm:
                    entry["stage"] = "CONFIRMED"
                    entry["active"] = True
                    entry["confirm_price"] = price
                    update_early_row(
                        entry["id"],
                        result="CONFIRMED",
                        stage="CONFIRMED",
                        confirm_time=now_beijing(),
                        confirm_price=price,
                        progress_class=progress_class
                    )
                    sig = analyze_symbol(sym, entry, market_state_cache)
                    if sig and sig.get("rejected"):
                        notify_tg_only(
                            f"⚠️ {sym} — 慢启动成立，但被风险过滤\n"
                            f"当前价: {price:.8f}\n"
                            f"启动后涨幅: {gain*100:+.2f}%\n"
                            f"追高风险: {sig.get('chase_risk', 0)}/100\n"
                            f"原因: {sig.get('reject_reason', '未知风险')}\n"
                            f"建议：暂不追高，继续观察。",
                            channel="risk_reject", symbol=sym
                        )
                        update_early_row(
                            entry["id"],
                            formal_score=sig.get("score", 0),
                            chase_risk=sig.get("chase_risk", 0),
                            fail_reason=sig.get("reject_reason", "风险过滤")
                        )
                    else:
                        notify_signal({
                            "symbol": sym, "direction": "LONG", "price": price,
                            "score": sig.get("score", 0) if sig else 0,
                            "max_score": 6,
                            "quality_label": "慢启动确认",
                            "oi_change": sig.get("oi_change", 0) if sig else 0,
                            "volume_ratio": sig.get("volume_ratio", 0) if sig else 0,
                            "funding": sig.get("funding", 0) if sig else 0,
                            "post_start_pct": gain * 100,
                            "progress_class": progress_class,
                            "chase_risk": sig.get("chase_risk", 0) if sig else 0,
                        })
                    active_tracking[sym] = entry
                    with early_lock:
                        pending_early.pop(sym, None)
                    continue
                else:
                    reason = "15分钟内未形成有效延续"
                    cooldown = now_beijing() + timedelta(
                        seconds=EARLY_FAIL_COOLDOWN_SEC
                    )
                    update_early_row(
                        entry["id"],
                        result="FAILED",
                        stage="FAILED",
                        fail_reason=reason,
                        cooldown_until=cooldown
                    )
                    add_tracking_event(
                        sym, entry["id"], "TIMEOUT_FAIL", price=price,
                        change_pct=gain * 100,
                        drawdown_pct=drawdown * 100,
                        progress_class=progress_class,
                        note=reason
                    )
                    with early_lock:
                        pending_early.pop(sym, None)

        except Exception as e:
            logger.error(f"{sym} 启动验证失败: {e}")


def track_active():
    """正式信号后：1M/3M动态推进 + 15/30分钟动能管理。"""
    for sym in list(active_tracking.keys()):
        sb = active_tracking[sym]
        try:
            if sb.get("last_high") is None or sb.get("trigger_price") is None:
                # 兼容极少数旧状态
                if sb.get("base_low") is None:
                    del active_tracking[sym]
                    continue

            trigger_dt = sb.get("trigger_dt") or sb.get("trigger_time")
            if isinstance(trigger_dt, str):
                trigger_dt = parse_ts(trigger_dt)
            elapsed_min = (
                now_beijing() - trigger_dt
            ).total_seconds() / 60

            k1 = get_klines(sym, "1m", PUSH_ATR_PERIOD + 5)
            if not k1:
                continue

            price = float(k1[-1][4])
            high_now = max(float(x[2]) for x in k1[-3:])
            low_now = min(float(x[3]) for x in k1[-3:])

            previous_high_for_time = sb.get(
                "last_recorded_high", sb.get("highest_price", sb.get("first_price") or price)
            )
            if high_now > previous_high_for_time:
                sb["last_recorded_high"] = high_now
                sb["last_new_high_time"] = time.time()

            sb["highest_price"] = max(
                sb.get("highest_price", sb.get("first_price") or price),
                price, high_now
            )
            sb["daily_high"] = max(sb.get("daily_high", price), price)
            sb["daily_low"] = min(sb.get("daily_low", price), price)

            trigger_price = sb.get("trigger_price", sb.get("first_price", price))
            trigger_low = sb.get("trigger_low", sb.get("base_low", price))
            gain = (price - trigger_price) / trigger_price if trigger_price > 0 else 0
            drawdown = (
                (sb["highest_price"] - price) / sb["highest_price"]
                if sb["highest_price"] > 0 else 0
            )

            # 维护历史极值
            sb["max_gain_seen"] = max(sb.get("max_gain_seen", gain), gain)
            sb["max_drawdown_seen"] = max(sb.get("max_drawdown_seen", drawdown), drawdown)

            progress_class, _ = classify_progress(
                gain, atr_percent_from_1m(k1)
            )

            if elapsed_min >= TRACKING_TIMEOUT_HOURS * 60:
                update_early_row(
                    sb["id"], result="EXPIRED", stage="EXPIRED",
                    max_price=sb["highest_price"],
                    min_price=sb["daily_low"],
                    max_gain=sb["max_gain_seen"],
                    max_drawdown=sb["max_drawdown_seen"]
                )
                add_tracking_event(
                    sym, sb["id"], "EXPIRED", price=price,
                    change_pct=gain*100, drawdown_pct=drawdown*100,
                    progress_class=progress_class,
                    note="24小时超时"
                )
                notify_tg_only(
                    f"⏰ {sym} — 24小时跟踪结束\n"
                    f"当前价: {price:.8f}\n"
                    f"启动后: {gain*100:+.2f}%\n"
                    f"已撤出关注池。",
                    channel="expired", symbol=sym
                )
                del active_tracking[sym]
                continue

            # 结构失效：启动低点被破
            if price < trigger_low:
                update_early_row(
                    sb["id"], result="FAILED", stage="FAILED",
                    max_price=sb["highest_price"],
                    min_price=price,
                    max_gain=sb["max_gain_seen"],
                    max_drawdown=sb["max_drawdown_seen"],
                    fail_reason="正式确认后跌破启动基准低点",
                    cooldown_until=now_beijing() + timedelta(
                        seconds=LONG_FAIL_COOLDOWN_SEC
                    )
                )
                add_tracking_event(
                    sym, sb["id"], "STRUCTURE_FAIL", price=price,
                    change_pct=gain*100, drawdown_pct=drawdown*100,
                    progress_class=progress_class,
                    note="跌破启动基准低点"
                )
                notify_failure(sym, price, "正式确认后跌破启动基准低点", -drawdown*100)
                del active_tracking[sym]
                continue

            # 极端回撤
            if drawdown >= EXTREME_DRAWDOWN:
                update_early_row(
                    sb["id"], result="FAILED", stage="FAILED",
                    max_price=sb["highest_price"],
                    min_price=price,
                    max_gain=sb["max_gain_seen"],
                    max_drawdown=sb["max_drawdown_seen"],
                    fail_reason="正式确认后最大回撤达到5%",
                    cooldown_until=now_beijing() + timedelta(
                        seconds=LONG_FAIL_COOLDOWN_SEC
                    )
                )
                add_tracking_event(
                    sym, sb["id"], "EXTREME_FAIL", price=price,
                    change_pct=gain*100, drawdown_pct=drawdown*100,
                    progress_class=progress_class,
                    note="最大回撤达到5%"
                )
                notify_failure(sym, price, "正式确认后最大回撤达到5%", -drawdown*100)
                del active_tracking[sym]
                continue

            # 软回撤：只记录，不刷屏
            if drawdown >= SOFT_DRAWDOWN:
                add_tracking_event(
                    sym, sb["id"], "SOFT_DRAWDOWN", price=price,
                    change_pct=gain*100, drawdown_pct=drawdown*100,
                    progress_class=progress_class,
                    note="软回撤0.8%"
                )

            # 新高推进
            previous_high = sb.get("last_recorded_high", trigger_price)
            if high_now > previous_high:
                sb["last_new_high_time"] = time.time()
                push_pct = (
                    (high_now - previous_high) / previous_high
                    if previous_high > 0 else 0
                )
                if push_pct >= max(
                    NORMAL_PUSH_MIN,
                    atr_percent_from_1m(k1) * NORMAL_PUSH_ATR
                ):
                    sb["push_times"] = sb.get("push_times", 0) + 1
                    sb["push_count"] = min(
                        MAX_PUSH, sb.get("push_count", 0) + 1
                    )
                    sb["last_recorded_high"] = high_now
                    update_early_row(
                        sb["id"],
                        max_price=sb["highest_price"],
                        max_gain=sb["max_gain_seen"],
                        max_drawdown=sb["max_drawdown_seen"],
                        progress_class=progress_class
                    )
                    add_tracking_event(
                        sym, sb["id"], "NEW_HIGH", price=price,
                        change_pct=push_pct*100,
                        drawdown_pct=drawdown*100,
                        progress_class=progress_class,
                        note="有效新高推进"
                    )

                    if time.time() - startup_time >= SILENT_PERIOD_SEC:
                        notify_tg_only(
                            f"🚀 {sym} — {progress_class}\n"
                            f"当前价: {price:.8f}\n"
                            f"启动后: {gain*100:+.2f}%\n"
                            f"本次新高推进: {push_pct*100:+.2f}%\n"
                            f"累计推进次数: {sb.get('push_times',0)}",
                            channel="push", symbol=sym
                        )

            # 15分钟无新高：仅第一次提醒
            if "last_new_high_time" not in sb:
                sb["last_new_high_time"] = time.time() - (elapsed_min * 60)
            minutes_since_high = elapsed_min
            if sb.get("last_new_high_time"):
                minutes_since_high = (
                    time.time() - sb["last_new_high_time"]
                ) / 60

            if (
                elapsed_min >= MOMENTUM_WEAK_MINUTES and
                not sb.get("weak_notified") and
                (time.time() - sb.get("last_new_high_time", time.time())) / 60
                >= MOMENTUM_WEAK_MINUTES
            ):
                notify_momentum_weak(
                    sym, price, gain*100,
                    MOMENTUM_WEAK_MINUTES, progress_class
                )
                sb["weak_notified"] = True

            # 30分钟没有新高且没有形成新的有效推进：退出观察
            if (
                elapsed_min >= MOMENTUM_FAIL_MINUTES and
                (time.time() - sb.get("last_new_high_time", time.time())) / 60
                >= MOMENTUM_FAIL_MINUTES
                and gain < FAST_MIN_PROGRESS
            ):
                reason = "30分钟无有效延续"
                update_early_row(
                    sb["id"], result="FLAT",
                    stage="FLAT",
                    max_price=sb["highest_price"],
                    min_price=sb["daily_low"],
                    max_gain=sb["max_gain_seen"],
                    max_drawdown=sb["max_drawdown_seen"],
                    fail_reason=reason
                )
                add_tracking_event(
                    sym, sb["id"], "FLAT_EXIT", price=price,
                    change_pct=gain*100,
                    drawdown_pct=drawdown*100,
                    progress_class=progress_class,
                    note=reason
                )
                del active_tracking[sym]

            sb["daily_high"] = max(sb.get("daily_high", price), price)
            sb["daily_low"] = min(sb.get("daily_low", price), price)

        except Exception as e:
            logger.error(f"{sym} 动态追踪失败: {e}")

# ===================== 趋势通道 =====================
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
            if time.time() - startup_time < SILENT_PERIOD_SEC:
                logger.info(f"⏳ 静默期，跳过趋势推送: {sym}")
                continue
            trend_count = get_trend_count(sym) + 1
            notify_trend(sym, price, change_24h, bull_count, at_high_pct, trend_count)
            mark_trend_alerted(sym)
            record_channel(sym, "trend")
            hits += 1
            logger.info(f"📈 趋势信号: {sym} 24h涨幅{change_24h:+.2f}% (第{trend_count}次)")
    logger.info(f"📈 趋势扫描完成，发现 {hits} 个趋势信号")

# ===================== 追踪层 =====================
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


# ===================== 日报 =====================
def generate_daily_report(report_date=None):
    if report_date is None:
        report_date = now_beijing().date()

    date_start = datetime(
        report_date.year, report_date.month, report_date.day,
        tzinfo=timezone(timedelta(hours=8))
    )
    date_end = date_start + timedelta(days=1)

    with db_lock:
        early_rows = db_conn.execute("""
            SELECT
                id, symbol, trigger_time, trigger_price, start_pct,
                early_score, volume_ratio, stage, result,
                confirm_time, confirm_price, price_3m, price_6m, price_15m,
                max_price, min_price, max_gain, max_drawdown,
                progress_class, chase_risk, formal_score, fail_reason
            FROM early_signal_log
            WHERE trigger_time>=? AND trigger_time<?
            ORDER BY trigger_time ASC
        """, (date_start.isoformat(), date_end.isoformat())).fetchall()

        event_rows = db_conn.execute("""
            SELECT symbol, early_id, event_time, event_type, price,
                   change_pct, drawdown_pct, progress_class, note
            FROM tracking_events
            WHERE event_time>=? AND event_time<?
            ORDER BY event_time ASC
        """, (date_start.isoformat(), date_end.isoformat())).fetchall()

    cols = [
        "ID","币种","启动时间","启动价","3M启动幅度%","预警评分",
        "初始量能比","最终阶段","最终结果","确认时间","确认价",
        "3M价格","6M价格","15M价格","最高价","最低价",
        "最大涨幅%","最大回撤%","推进等级","追高风险","正式评分","失败原因"
    ]
    df = pd.DataFrame(early_rows, columns=cols)

    if not df.empty:
        for c in ["最大涨幅%","最大回撤%","3M启动幅度%"]:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).round(2)
        df["启动时间"] = pd.to_datetime(df["启动时间"], errors="coerce")
        df["确认时间"] = pd.to_datetime(df["确认时间"], errors="coerce")

    total = len(df)
    pre_alert = total
    confirmed = int((df["最终结果"] == "CONFIRMED").sum()) if total else 0
    failed = int((df["最终结果"] == "FAILED").sum()) if total else 0
    flat = int((df["最终结果"] == "FLAT").sum()) if total else 0
    pending = int((df["最终结果"] == "PENDING").sum()) if total else 0
    expired = int((df["最终结果"] == "EXPIRED").sum()) if total else 0

    # 3/6/15分钟漏斗
    if total:
        p3 = int(df["3M价格"].notna().sum())
        p6 = int(df["6M价格"].notna().sum())
        p15 = int(df["15M价格"].notna().sum())
        confirm_rate = confirmed / total * 100
    else:
        p3 = p6 = p15 = 0
        confirm_rate = 0

    stats_rows = [
        ["启动预警", pre_alert, 100.0],
        ["记录到3分钟", p3, p3/pre_alert*100 if pre_alert else 0],
        ["进入6分钟节点", p6, p6/pre_alert*100 if pre_alert else 0],
        ["进入15分钟节点", p15, p15/pre_alert*100 if pre_alert else 0],
        ["正式确认", confirmed, confirm_rate],
        ["失败", failed, failed/pre_alert*100 if pre_alert else 0],
        ["横盘退出", flat, flat/pre_alert*100 if pre_alert else 0],
        ["超时", expired, expired/pre_alert*100 if pre_alert else 0],
        ["仍待处理", pending, pending/pre_alert*100 if pre_alert else 0],
    ]
    funnel = pd.DataFrame(stats_rows, columns=["阶段","数量","占启动预警比例%"])
    funnel["占启动预警比例%"] = funnel["占启动预警比例%"].round(2)

    # 失败原因
    failure_df = pd.DataFrame(columns=["失败原因","数量","占失败比例%"])
    if total:
        fails = df.loc[df["最终结果"].isin(["FAILED","FLAT","EXPIRED"])]
        if not fails.empty:
            vc = fails["失败原因"].fillna("未分类").value_counts()
            failure_df = pd.DataFrame({
                "失败原因": vc.index,
                "数量": vc.values
            })
            failure_df["占失败比例%"] = (
                failure_df["数量"] / max(1, len(fails)) * 100
            ).round(2)

    # 最佳信号
    best_df = pd.DataFrame()
    if total:
        best_df = df.sort_values(
            ["最大涨幅%", "正式评分"],
            ascending=[False, False]
        ).head(10).copy()
        best_df = best_df[
            ["币种","启动时间","启动价","最高价","最大涨幅%",
             "最大回撤%","最终结果","正式评分","推进等级"]
        ]

    # 事件明细
    event_cols = [
        "币种","启动ID","事件时间","事件类型","价格","变化%",
        "回撤%","推进等级","说明"
    ]
    events_df = pd.DataFrame(event_rows, columns=event_cols)
    if not events_df.empty:
        events_df["事件时间"] = pd.to_datetime(
            events_df["事件时间"], errors="coerce"
        )

    fname = f"日报_{report_date.strftime('%Y%m%d')}.xlsx"

    with pd.ExcelWriter(fname, engine="openpyxl") as writer:
        if df.empty:
            pd.DataFrame([["当天没有启动预警记录"]], columns=["说明"]).to_excel(
                writer, sheet_name="信号明细", index=False
            )
        else:
            df.to_excel(writer, sheet_name="信号明细", index=False)

        funnel.to_excel(writer, sheet_name="启动漏斗", index=False)

        if failure_df.empty:
            pd.DataFrame([["当天没有失败记录"]], columns=["说明"]).to_excel(
                writer, sheet_name="失败分析", index=False
            )
        else:
            failure_df.to_excel(writer, sheet_name="失败分析", index=False)

        if best_df.empty:
            pd.DataFrame([["当天没有最佳信号"]], columns=["说明"]).to_excel(
                writer, sheet_name="最佳信号", index=False
            )
        else:
            best_df.to_excel(writer, sheet_name="最佳信号", index=False)

        if events_df.empty:
            pd.DataFrame([["当天没有跟踪事件"]], columns=["说明"]).to_excel(
                writer, sheet_name="跟踪事件", index=False
            )
        else:
            events_df.to_excel(writer, sheet_name="跟踪事件", index=False)

    # 日报邮件正文
    top_text = "无"
    if not best_df.empty:
        top = best_df.iloc[0]
        top_text = (
            f"{top['币种']}，启动后最大涨幅 "
            f"{float(top['最大涨幅%']):+.2f}%"
        )

    body = (
        f"📊 盘面日报 {report_date}\n\n"
        f"启动预警：{pre_alert}\n"
        f"正式确认：{confirmed}\n"
        f"失败：{failed}\n"
        f"横盘退出：{flat}\n"
        f"超时：{expired}\n"
        f"仍待处理：{pending}\n\n"
        f"预警→确认：{confirm_rate:.2f}%\n"
        f"今日最佳：{top_text}\n\n"
        f"附件包含：信号明细、启动漏斗、失败分析、最佳信号、跟踪事件。"
    )

    email_ok = send_email_attach(
        f"📊 [盘面日报] {report_date}",
        body,
        fname
    )

    # 状态表记录：每天只允许成功完成一次
    with db_lock:
        db_conn.execute("""
            INSERT INTO daily_report_status
            (report_date, generated_at, filepath, email_ok, attempts, last_error)
            VALUES (?,?,?,?,?,?)
            ON CONFLICT(report_date) DO UPDATE SET
              generated_at=excluded.generated_at,
              filepath=excluded.filepath,
              email_ok=excluded.email_ok,
              attempts=daily_report_status.attempts+1,
              last_error=excluded.last_error
        """, (
            str(report_date),
            now_beijing().isoformat(),
            fname,
            1 if email_ok else 0,
            1,
            "" if email_ok else "邮件发送失败"
        ))
        db_conn.commit()

    logger.info(
        f"📊 {report_date} 日报生成完成：{fname}，"
        f"预警{pre_alert}，确认{confirmed}，邮件{'成功' if email_ok else '失败'}"
    )

    # TG只发摘要，不发送Excel内容
    tg_text = (
        f"📊 {report_date} 盘面日报\n"
        f"启动预警: {pre_alert}\n"
        f"正式确认: {confirmed}\n"
        f"失败: {failed}\n"
        f"横盘: {flat}\n"
        f"超时: {expired}\n"
        f"确认率: {confirm_rate:.2f}%\n"
        f"最佳: {top_text}\n"
        f"邮箱日报: {'✅' if email_ok else '❌'}"
    )
    send_tg(tg_text)

    return email_ok


def report_already_sent(report_date):
    with db_lock:
        cur = db_conn.execute(
            "SELECT email_ok FROM daily_report_status WHERE report_date=?",
            (str(report_date),)
        )
        row = cur.fetchone()
        return bool(row and row[0] == 1)


def maybe_send_daily_report():
    """北京时间00:05后发送前一天日报；如果邮件失败，下次循环重试。"""
    now = now_beijing()
    if now.hour < DAILY_REPORT_HOUR:
        return
    if now.hour == DAILY_REPORT_HOUR and now.minute < DAILY_REPORT_MINUTE:
        return

    report_date = now.date() - timedelta(days=1)
    if report_already_sent(report_date):
        return

    try:
        generate_daily_report(report_date)
    except Exception as e:
        logger.error(f"📊 日报任务异常: {e}")
        with db_lock:
            db_conn.execute("""
                INSERT INTO daily_report_status
                (report_date, generated_at, filepath, email_ok, attempts, last_error)
                VALUES (?,?,?,?,?,?)
                ON CONFLICT(report_date) DO UPDATE SET
                  attempts=daily_report_status.attempts+1,
                  last_error=excluded.last_error
            """, (
                str(report_date), now_beijing().isoformat(),
                "", 0, 1, str(e)
            ))
            db_conn.commit()


def cleanup_old_data():
    cutoff = now_beijing() - timedelta(days=REPORT_KEEP_DAYS)
    cutoff_iso = cutoff.isoformat()
    with db_lock:
        db_conn.execute("DELETE FROM sent_log WHERE sent_at < ?", (cutoff_iso,))
        db_conn.execute("DELETE FROM trend_log WHERE sent_at < ?", (cutoff_iso,))
        db_conn.execute("DELETE FROM channel_log WHERE sent_at < ?", (cutoff_iso,))
        db_conn.execute("DELETE FROM tracking_events WHERE event_time < ?", (cutoff_iso,))
        # early_signal_log按时间保留更久，便于复盘；这里保留30天
        db_conn.execute("DELETE FROM early_signal_log WHERE trigger_time < ?", (cutoff_iso,))
        db_conn.execute(
            "DELETE FROM daily_report_status WHERE report_date < ?",
            (str(now_beijing().date() - timedelta(days=REPORT_KEEP_DAYS)),)
        )
        db_conn.commit()
    logger.info(f"🗑️ 已清理{REPORT_KEEP_DAYS}天前的旧历史数据")
# ===================== 建仓通道 =====================
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
            if level == "observe":
                with db_lock:
                    db_conn.execute(
                        "INSERT OR REPLACE INTO building_state VALUES (?,?,?)",
                        (sym, "observe", now_beijing()))
                    db_conn.commit()
                count["observe"] += 1
                continue
            if time.time() - startup_time < SILENT_PERIOD_SEC:
                logger.info(f"⏳ 静默期，跳过建仓推送: {sym}")
                continue
            if not should_alert(sym, "LONG", channel="building"):
                continue
            with db_lock:
                cur = db_conn.execute(
                    "SELECT level FROM building_state WHERE symbol=?", (sym,))
                row = cur.fetchone()
                prev_level = row[0] if row else None
            if prev_level == level:
                continue
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
    global startup_time
    startup_time = time.time()

    logger.info("🚀 融合监控系统启动（启动雷达 + 四通道 + 动态日报版）")

    startup_notice = (
        "✅ 融合监控系统启动\n"
        "🟡 通道0：启动雷达（1M + 3M）\n"
        "🚀 通道1：启动确认（3~6分钟，慢启动延长至15分钟）\n"
        "⚡ 通道2：快速拉升（3M HH ≥ 2%）\n"
        "📈 通道3：趋势信号（1H）\n"
        "📐 通道4：建仓信号（日线结构）\n"
        "📊 日报：北京时间00:05，SQLite重算后发送Excel\n"
        "📮 实时邮件：正式启动/速报/趋势/建仓关注与介入\n"
        "📱 TG：预警、正式信号、失效、跟踪、日报摘要"
    )
    notify_all(startup_notice, "监控系统启动")

    symbols = get_symbols()
    if not symbols or not high_volume_symbols:
        logger.error("无可用币种或无法获取成交额数据，退出")
        notify_all(
            "❌ 系统启动失败：无法获取币种列表或成交额数据",
            "系统启动失败"
        )
        return

    last_refresh = time.time()
    last_trend_scan = 0
    last_build_scan = 0
    last_market_state_scan = 0
    global market_state_cache
    market_state_cache = {"btc_change_15m": 0.0, "market_down_ratio": 0.0}

    while True:
        try:
            # 每日任务：00:05之后发送前一天日报
            maybe_send_daily_report()

            # 到新自然日时只重置内存统计，不动历史数据库
            today = now_beijing().date()
            for sym, sb in state_b.items():
                if sb.get("day") != today:
                    sb["day"] = today
                    sb["daily_round"] = 0
                    sb["push_times"] = 0
                    sb["first_price"] = None
                    sb["daily_high"] = 0.0
                    sb["daily_low"] = 1e10
                    sb["source"] = None
                    if sym not in active_tracking:
                        sb["active"] = False
                        sb["push_count"] = 0

            if time.time() - last_refresh > SYMBOL_REFRESH_INTERVAL:
                new_syms = get_symbols()
                if new_syms:
                    symbols = new_syms
                    last_refresh = time.time()

            # 每2分钟更新一次BTC/市场状态缓存
            if time.time() - last_market_state_scan >= 120:
                try:
                    market_state_cache = get_market_state(symbols)
                except Exception as e:
                    logger.warning(f"市场状态更新失败: {e}")
                last_market_state_scan = time.time()

            # 全市场普涨噪音不再直接挡住全部系统；只减少启动雷达扫描
            if market_state_cache["market_down_ratio"] >= 0.85:
                logger.info(
                    f"⚠️ 市场极端弱势，下跌占比"
                    f"{market_state_cache['market_down_ratio']*100:.0f}%"
                )

            # ① 启动雷达
            t0 = time.time()
            with ThreadPoolExecutor(max_workers=MAX_WORKERS_TRIGGER) as ex:
                list(ex.map(scan_trigger, symbols))
            logger.info(
                f"🟡 启动雷达完成 ({time.time()-t0:.1f}s)，"
                f"观察池 {len(pending_early)}"
            )

            # ② 3/6/15分钟验证
            process_early_validation()

            # ③ 趋势通道
            if time.time() - last_trend_scan >= TREND_SCAN_INTERVAL:
                scan_trends(symbols)
                last_trend_scan = time.time()

            # ④ 建仓通道
            if time.time() - last_build_scan >= BUILD_SCAN_INTERVAL:
                scan_building_signals()
                last_build_scan = time.time()

            # ⑤ 正式信号追踪
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