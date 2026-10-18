# -*- coding: utf-8 -*-
"""
融合监控系统（四通道版 · 修复版 · 通知层未改动）
通道0 🟡 启动预警：1M + 3M 前置雷达
通道1 🚀 启动确认：3~6分钟延续验证，慢启动延长至15分钟
通道2 ⚡ 速报信号：3M HH ≥ 2%
通道3 📈 趋势信号：1H K线趋势检测
通道4 📐 建仓信号：日线下跌→放量→横盘结构

本版修复（不改策略阈值，只修执行与统计）：
[FIX-1]  推进通知死代码：last_recorded_high 被提前覆盖 → 改为有效推进后才前移基准
[FIX-2]  趋势通道 48h 自锁 → 窗口过期视为新窗口
[FIX-3]  速报被 EARLY_HH_MAX 掐死 → 速报判断提到启动窗口门之前
[FIX-4]  "下跌>60%" 不可达 + 边界不一致 → 0.60 扣分 / 0.70 硬拒，统一 >=
[FIX-5]  btc_bad 用 abs() → 只在 BTC 下跌时扣分
[FIX-6]  日报 max_gain/max_drawdown 单位差 100 倍 → 报告层统一换算百分数
[FIX-7]  确认率口径错 → 用 confirm_time 判定，新增"确认后失效"
[FIX-8]  日报失败后每 30 秒重试 → 最多 3 次 + 10 分钟间隔
[FIX-9]  cleanup_old_data 从未调用 → 每日一次，并清理旧 xlsx
[FIX-10] 重启丢状态 → 启动时按 24h 窗口重建 pending/active
[FIX-11] 建仓通道用未收盘日线 → 只用已收盘日线
[FIX-12] tracking_events 软回撤每 30s 一条 → 10 分钟节流
[FIX-13] 无限频/无退避 → 全局限速 + 429/418 退避 + GET 重试
[FIX-14] 每轮 1000 次 SQLite 查 → 每轮一次预加载
[FIX-15] 建仓/趋势扫描阻塞主循环 → 后台线程执行
[FIX-16] 日志无轮转 → RotatingFileHandler
[FIX-17] sleep(30) 节拍漂移 → monotonic 定拍
[FIX-18] OI_STRONG 死参数 → 参与评分标记
[FIX-19] 删除死代码/死参数，补索引与 WAL
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
from logging.handlers import RotatingFileHandler
from pathlib import Path
from requests.adapters import HTTPAdapter
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.header import Header
from email.utils import formataddr

# ========================= 配置区 =========================
BINANCE_API = "https://fapi.binance.com"

# Telegram / QQ 凭据（通知层未改动，仍为原值；建议尽快改为环境变量并 revoke 旧 token）
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

# [FIX-13] 全局限速与重试
RATE_LIMIT_PER_SEC = 18.0      # 全局请求令牌桶（Binance fapi 权重上限约 2400/min）
HTTP_RETRY_ATTEMPTS = 3        # 仅 GET 重试
HTTP_BACKOFF_SEC = 1.0
HTTP_MAX_RETRY_AFTER = 30.0

# 成交额门槛
MIN_24H_VOLUME_USDT = 8_000_000

ALT_BLACKLIST = ["BTCUSDT", "ETHUSDT"]

# ================= 新版启动雷达 =================
EARLY_HH_MIN = 1.2
EARLY_HH_MAX = 2.5
EARLY_1M_BODY_MIN = 0.5
EARLY_1M_VOL_MULT = 2.0
EARLY_3M_VOL_MULT = 1.3
EARLY_CLOSE_POSITION = 0.80
EARLY_BREAK_LOOKBACK = 5

FAST_CHECK_3M_SEC = 180
FAST_CHECK_6M_SEC = 360
SLOW_CHECK_15M_SEC = 900

FAST_MIN_PROGRESS = 0.012
SLOW_MIN_PROGRESS = 0.008

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
SOFT_DRAWDOWN_EVENT_GAP_SEC = 600   # [FIX-12] 软回撤事件节流

# 启动失败冷却
EARLY_FAIL_COOLDOWN_SEC = 1800
LONG_FAIL_COOLDOWN_SEC = 3600

# 正式信号评分
FORMAL_SCORE_MIN = 4
STRONG_SCORE_MIN = 5

# 系统B兼容参数
MAX_PUSH = 3
MAX_DAILY_ROUND = 1

# 速报通道
SURGE_HH_PCT = 2.0

# 趋势通道参数
TREND_SCAN_INTERVAL = 1800
TREND_CHANGE_MIN = 5.0
TREND_BULL_MIN = 10
TREND_AT_HIGH_MIN = 90
TREND_DEDUP_HOURS = 6
TREND_MAX_COUNT = 8
TREND_WINDOW_HOURS = 48            # [FIX-2] 计数窗口

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
MARKET_HARD_DOWN_RATIO = 0.70      # [FIX-4] 原 0.50 导致 0.60 分支不可达
BTC_HARD_NEGATIVE = -0.005

# 去重
DEDUP_HOURS = 2
EARLY_DEDUP_MINUTES = 60

# 追踪
TRACKING_TIMEOUT_HOURS = 24
MOMENTUM_WEAK_MINUTES = 15
MOMENTUM_FAIL_MINUTES = 30

# 每日报告
DAILY_REPORT_HOUR = 0
DAILY_REPORT_MINUTE = 5
REPORT_KEEP_DAYS = 30
DAILY_REPORT_MAX_ATTEMPTS = 3      # [FIX-8]
DAILY_REPORT_RETRY_SEC = 600       # [FIX-8]
EVENT_SHEET_MAX_ROWS = 5000        # 明细表上限，避免 Excel 打不开

# 并发
MAX_WORKERS_TRIGGER = 30
MAX_WORKERS_TREND = 10

# 启动静默期（秒）
SILENT_PERIOD_SEC = 900
startup_time = 0

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

CHANNEL_CN = {
    "confirm": "🚀启动确认", "surge": "⚡速报", "trend": "📈趋势",
    "building": "📐建仓", "early": "🟡启动预警", "push": "🚀推进",
    "momentum_weak": "🟡动能减弱", "failure": "❌失效",
    "risk_reject": "⚠️风险过滤", "expired": "⏰超时",
}

LOG_FILE = "monitor.log"

# ===================== 日志 + 数据库 ======================
# [FIX-16] 日志轮转 + 控制台编码容错
try:
    import sys
    sys.stderr.reconfigure(errors="replace")
except Exception:
    pass

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        RotatingFileHandler(LOG_FILE, maxBytes=20 * 1024 * 1024,
                            backupCount=5, encoding="utf-8"),
        logging.StreamHandler(),
    ]
)
logger = logging.getLogger(__name__)

db_lock = Lock()
db_conn = sqlite3.connect("signals.db", check_same_thread=False)
sqlite3.register_adapter(datetime, lambda dt: dt.isoformat())

with db_conn:
    # [FIX-19] WAL + 索引
    db_conn.execute("PRAGMA journal_mode=WAL")
    db_conn.execute("PRAGMA synchronous=NORMAL")
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
    db_conn.execute("CREATE INDEX IF NOT EXISTS idx_early_symbol_time ON early_signal_log(symbol, trigger_time)")
    db_conn.execute("CREATE INDEX IF NOT EXISTS idx_early_cooldown ON early_signal_log(cooldown_until)")
    db_conn.execute("CREATE INDEX IF NOT EXISTS idx_early_result ON early_signal_log(result)")
    db_conn.execute("CREATE INDEX IF NOT EXISTS idx_channel_symbol_time ON channel_log(symbol, sent_at)")
    db_conn.execute("CREATE INDEX IF NOT EXISTS idx_sent_symbol ON sent_log(symbol, channel, sent_at)")
    db_conn.execute("CREATE INDEX IF NOT EXISTS idx_events_time ON tracking_events(event_time)")
    db_conn.execute("CREATE INDEX IF NOT EXISTS idx_events_symbol ON tracking_events(symbol, event_time)")

# ===================== 北京时间辅助函数 =====================
def now_beijing():
    return datetime.now(timezone(timedelta(hours=8)))

def parse_ts(value):
    if isinstance(value, datetime):
        dt = value
    elif not value:
        return now_beijing()
    elif isinstance(value, (int, float)):
        return datetime.fromtimestamp(float(value), tz=timezone(timedelta(hours=8)))
    else:
        try:
            dt = datetime.fromisoformat(str(value))
        except Exception:
            return now_beijing()
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone(timedelta(hours=8)))
    return dt

def entry_trigger_dt(sb):
    """[FIX-10] 统一取启动时间，兼容 datetime / iso 字符串 / epoch 秒。"""
    dt = sb.get("trigger_dt")
    if isinstance(dt, datetime):
        return dt
    tt = sb.get("trigger_time")
    if isinstance(tt, (int, float)):
        return datetime.fromtimestamp(float(tt), tz=timezone(timedelta(hours=8)))
    return parse_ts(tt)

def iso_now():
    return now_beijing().isoformat()

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

# ===================== 数据库函数 =====================
def should_alert(symbol, direction, channel='confirm', max_count=3):
    cutoff_2h = now_beijing() - timedelta(hours=DEDUP_HOURS)
    cutoff_24h = now_beijing() - timedelta(hours=24)
    with db_lock:
        cur = db_conn.execute(
            "SELECT 1 FROM sent_log WHERE symbol=? AND direction=? AND channel=? AND sent_at > ?",
            (symbol, direction, channel, cutoff_2h))
        if cur.fetchone() is not None:
            return False
        cur = db_conn.execute(
            "SELECT COUNT(*) FROM sent_log WHERE symbol=? AND direction=? AND channel=? AND sent_at > ?",
            (symbol, direction, channel, cutoff_24h))
        count_24h = cur.fetchone()[0]
        return count_24h < max_count

def mark_alerted(symbol, direction, channel='confirm'):
    with db_lock:
        db_conn.execute(
            "INSERT INTO sent_log VALUES (?,?,?,?)",
            (symbol, direction, channel, now_beijing()))
        db_conn.commit()

def should_trend_alert(symbol):
    """[FIX-2] 窗口过期返回 True，让 mark_trend_alerted 走重置分支（原来返回 False 会永久静音）。"""
    with db_lock:
        cur = db_conn.execute(
            "SELECT count, first_at, sent_at FROM trend_log WHERE symbol=? ORDER BY sent_at DESC LIMIT 1",
            (symbol,))
        row = cur.fetchone()
    if row is None:
        return True
    count, first_at, last_sent = row
    now = now_beijing()
    try:
        if (now - parse_ts(first_at)).total_seconds() > TREND_WINDOW_HOURS * 3600:
            return True
        if (now - parse_ts(last_sent)).total_seconds() < TREND_DEDUP_HOURS * 3600:
            return False
    except Exception:
        return True
    if (count or 0) >= TREND_MAX_COUNT:
        return False
    return True

def get_trend_count(symbol):
    with db_lock:
        cur = db_conn.execute(
            "SELECT count FROM trend_log WHERE symbol=? ORDER BY sent_at DESC LIMIT 1",
            (symbol,))
        row = cur.fetchone()
        if row:
            return row[0] or 0
        return 0

def mark_trend_alerted(symbol):
    with db_lock:
        cur = db_conn.execute(
            "SELECT count, first_at FROM trend_log WHERE symbol=? ORDER BY sent_at DESC LIMIT 1",
            (symbol,))
        row = cur.fetchone()
        now = now_beijing()
        if row is None:
            db_conn.execute(
                "INSERT INTO trend_log VALUES (?,?,?,?)",
                (symbol, 1, now.isoformat(), now.isoformat()))
        else:
            old_count, first_at = row
            if (now - parse_ts(first_at)).total_seconds() > TREND_WINDOW_HOURS * 3600:
                db_conn.execute(
                    "INSERT INTO trend_log VALUES (?,?,?,?)",
                    (symbol, 1, now.isoformat(), now.isoformat()))
            else:
                db_conn.execute(
                    "INSERT INTO trend_log VALUES (?,?,?,?)",
                    (symbol, (old_count or 0) + 1, first_at, now.isoformat()))
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
# [FIX-13] 全局限速 + 429/418 退避 + GET 重试
_rate_lock = Lock()
_rate_next = [0.0]

def _throttle():
    with _rate_lock:
        gap = 1.0 / RATE_LIMIT_PER_SEC
        now = time.monotonic()
        wait = _rate_next[0] - now
        if wait > 0:
            time.sleep(wait)
            now = time.monotonic()
        _rate_next[0] = now + gap

session = requests.Session()
adapter = HTTPAdapter(pool_connections=30, pool_maxsize=30)
session.mount("https://", adapter)
session.mount("http://", adapter)
_orig = session.request

def _patched(method, url, **kw):
    kw.setdefault("timeout", REQUEST_TIMEOUT)
    is_get = method.upper() == "GET"
    attempts = HTTP_RETRY_ATTEMPTS if is_get else 1
    last_exc = None
    r = None
    for i in range(attempts):
        _throttle()
        try:
            r = _orig(method, url, **kw)
        except Exception as e:
            last_exc = e
            if i + 1 < attempts:
                time.sleep(HTTP_BACKOFF_SEC * (i + 1))
                continue
            raise
        if r.status_code in (429, 418):
            try:
                wait = float(r.headers.get("Retry-After", 5) or 5)
            except Exception:
                wait = 5.0
            wait = min(HTTP_MAX_RETRY_AFTER, max(1.0, wait))
            logger.warning(f"HTTP {r.status_code} 限频，等待 {wait:.0f}s")
            time.sleep(wait)
            if i + 1 < attempts:
                continue
        return r
    if r is not None:
        return r
    if last_exc:
        raise last_exc
    raise RuntimeError("request failed")

session.request = _patched

# ===================== Binance API =====================
high_volume_symbols = set()
ticker_cache = {}

def get_symbols():
    global high_volume_symbols, ticker_cache
    try:
        r = session.get(f"{BINANCE_API}/fapi/v1/exchangeInfo")
        r.raise_for_status()
        info = r.json()
        all_syms = [
            s['symbol'] for s in info['symbols']
            if s['contractType'] == 'PERPETUAL'
            and s['quoteAsset'] == 'USDT'
            and s['status'] == 'TRADING'
            and s['symbol'] not in ALT_BLACKLIST
        ]
        r = session.get(f"{BINANCE_API}/fapi/v1/ticker/24hr")
        r.raise_for_status()
        tickers = r.json()
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
            except Exception:
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
    except Exception as e:
        logger.debug(f"{symbol} 资金费率历史失败: {e}")
    return None

# ===================== 通知模块（原样保留） =====================
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

pending_early = {}
active_tracking = {}
early_lock = Lock()

market_state_cache = {"btc_change_15m": 0.0, "market_down_ratio": 0.0}

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

def get_existing_early(symbol, within_minutes=60):
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

def build_scan_blocklist():
    """[FIX-14] 每轮一次性预加载，替代每个币 2 次 SQLite 查询。"""
    blocked = set()
    cutoff = (now_beijing() - timedelta(minutes=EARLY_DEDUP_MINUTES)).isoformat()
    now_iso = now_beijing().isoformat()
    try:
        with db_lock:
            rows = db_conn.execute("""
                SELECT symbol FROM early_signal_log
                WHERE trigger_time>? AND result IN ('PENDING','CONFIRMED')
            """, (cutoff,)).fetchall()
            blocked.update(r[0] for r in rows)
            rows = db_conn.execute("""
                SELECT symbol FROM early_signal_log
                WHERE cooldown_until IS NOT NULL AND cooldown_until > ?
            """, (now_iso,)).fetchall()
            blocked.update(r[0] for r in rows)
    except Exception as e:
        logger.warning(f"预加载状态失败: {e}")
    return blocked

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
def scan_trigger(sym, blocked=None):
    try:
        if sym not in high_volume_symbols:
            return
        if sym in pending_early or sym in active_tracking:
            return
        if blocked is not None:
            if sym in blocked:
                return
        else:
            if get_existing_early(sym, EARLY_DEDUP_MINUTES) or is_symbol_in_cooldown(sym):
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
        closed3 = k3[:-1]
        highs = [float(x[2]) for x in closed3]
        lows = [float(x[3]) for x in closed3]
        vols = [float(x[5]) for x in closed3]
        opens = [float(x[1]) for x in closed3]

        hh = highs[-3] < highs[-2] < highs[-1]
        start_pct = (
            (highs[-1] - lows[-3]) / lows[-3] * 100
            if lows[-3] > 0 else 0
        )
        if not hh:
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

        price_now = last_c

        # [FIX-3] 速报先判断：不再被 EARLY_HH_MAX 上限掐死（原来 >2.5% 直接 return）
        if start_pct >= SURGE_HH_PCT:
            if time.time() - startup_time >= SILENT_PERIOD_SEC:
                if should_alert(sym, "LONG", channel="surge", max_count=3):
                    notify_surge(sym, price_now, start_pct)
                    mark_alerted(sym, "LONG", channel="surge")
                    logger.info(f"⚡ 速报: {sym} 3M HH={start_pct:.2f}%")
            else:
                logger.info(f"⏳ 静默期，跳过快速拉升通知: {sym}")

        # 启动雷达窗口（只约束通道0，不拦通道2）
        if not (EARLY_HH_MIN <= start_pct <= EARLY_HH_MAX):
            return

        score = 1  # 3M HH 基础分
        if body_pct >= EARLY_1M_BODY_MIN:
            score += 1
        if vol_ratio_1m >= EARLY_1M_VOL_MULT:
            score += 1
        if close_pos >= EARLY_CLOSE_POSITION or break_ok:
            score += 1

        if score < 4:
            return

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
            "highest_price": price_now,
            "last_high": highs[-1],
            "last_check_price": price_now,
            "check_3m_done": False,
            "check_6m_done": False,
            "check_15m_done": False,
            "weak_notified": False,
            "source": "early",
            "stage": "EARLY",
            "active": False,
            "new_high_observed": False,
            "max_gain_seen": -1.0,
            "max_drawdown_seen": 0.0,
            "last_recorded_high": price_now,
            "last_new_high_time": time.time(),
        }

        with early_lock:
            pending_early[sym] = entry

        sb["daily_round"] += 1
        sb["first_price"] = price_now
        sb["daily_high"] = price_now
        sb["daily_low"] = price_now
        sb["trigger_time"] = now_beijing()
        sb["source"] = "early"

        logger.info(
            f"🟡 启动预警(内部): {sym} "
            f"HH={start_pct:.2f}% score={score}/4 vol={vol_ratio_1m:.2f}x"
        )

    except Exception as e:
        logger.error(f"{sym} 启动雷达失败: {e}")

# ===================== 正式确认/强化评分 =====================
def analyze_symbol(symbol, early=None, market_state=None):
    try:
        if symbol not in high_volume_symbols:
            return None

        k5 = get_klines(symbol, "5m", 24)
        k15 = get_klines(symbol, "15m", 12)
        if len(k5) < 12 or len(k15) < 6:
            return None

        oi_hist = get_oi_history(symbol, "1h", 2)
        if len(oi_hist) >= 2:
            oi_prev = float(oi_hist[0]["sumOpenInterest"])
            oi_now = float(oi_hist[-1]["sumOpenInterest"])
            oi_change = (oi_now - oi_prev) / oi_prev if oi_prev > 0 else 0
        else:
            oi_change = 0

        funding = get_funding_rate(symbol)

        closes5 = [float(k[4]) for k in k5]
        highs5 = [float(k[2]) for k in k5]
        volumes5 = [float(k[5]) for k in k5]
        structure_ok = closes5[-1] > max(highs5[-11:-1])

        continued = False
        post_start_pct = 0.0
        if early:
            current = closes5[-1]
            post_start_pct = (
                (current - early["trigger_price"]) / early["trigger_price"]
                if early["trigger_price"] else 0
            )
            continued = current > early.get("trigger_high", early["trigger_price"])

        base_vol = sum(volumes5[:-3]) / max(1, len(volumes5)-3)
        recent_vol = sum(volumes5[-3:]) / 3
        volume_ratio = recent_vol / base_vol if base_vol > 0 else 0
        volume_ok = volume_ratio >= VOLUME_MULTIPLIER

        last3 = k15[-3:]
        bullish = sum(1 for k in last3 if float(k[4]) > float(k[1]))
        recent15_ok = (
            bullish >= 2 and
            float(last3[-1][4]) > float(last3[-2][2])
        )

        oi_ok = oi_change > 0
        oi_strong = oi_change >= OI_STRONG          # [FIX-18] 启用原本闲置的 OI_STRONG
        funding_ok = not (funding > FUNDING_RATE_MAX)

        if market_state is None:
            market_state = market_state_cache.copy()

        # [FIX-5] BTC 只在下砸时算“环境差”，上涨不再扣分
        btc_dump = market_state["btc_change_15m"] <= -BTC_PAUSE_15M
        market_bad = market_state["market_down_ratio"] >= MARKET_DOWN_RATIO

        # [FIX-4] 软阈值 0.60 扣分、硬阈值 0.70 拒绝，两条都能走到
        hard_market_reject = (
            market_state["market_down_ratio"] >= MARKET_HARD_DOWN_RATIO or
            market_state["btc_change_15m"] <= BTC_HARD_NEGATIVE
        )

        price = closes5[-1]
        chase_risk = compute_chase_risk(symbol, price)

        score = 0
        score += 1 if structure_ok else 0
        score += 1 if continued else 0
        score += 1 if volume_ok else 0
        score += 1 if oi_ok else 0
        score += 1 if (recent15_ok and funding_ok) else 0
        score += 1 if (not btc_dump and not market_bad) else 0

        one_hour_change = 0
        if len(closes5) >= 13:
            one_hour_change = (closes5[-1] - closes5[-13]) / closes5[-13]

        if hard_market_reject:
            return {
                "symbol": symbol, "direction": "LONG",
                "rejected": True, "reject_reason": "市场状态不佳（下跌占比过高或BTC下跌）",
                "price": price, "score": score, "max_score": 6, "chase_risk": chase_risk
            }
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
            "oi_strong": oi_strong,
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

# ===================== 3/6/15分钟验证 =====================
def process_early_validation():
    now_ts = time.time()
    with early_lock:
        symbols_snapshot = list(pending_early.keys())

    for sym in symbols_snapshot:
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

            old_highest = entry.get("highest_price", entry["trigger_price"])
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

            entry["max_gain_seen"] = max(entry.get("max_gain_seen", -1), gain)
            entry["max_drawdown_seen"] = max(entry.get("max_drawdown_seen", 0), drawdown)

            if new_high:
                entry["new_high_observed"] = True

            atr_pct = atr_percent_from_1m(k1)
            progress_class, _ = classify_progress(gain, atr_pct)

            update_early_row(
                entry["id"],
                max_price=entry["max_price"],
                min_price=entry["min_price"],
                max_gain=entry["max_gain_seen"],
                max_drawdown=entry["max_drawdown_seen"],
                progress_class=progress_class
            )

            # 硬失败
            if price < entry["trigger_low"]:
                reason = "跌破启动基准低点"
                cooldown = now_beijing() + timedelta(seconds=EARLY_FAIL_COOLDOWN_SEC)
                update_early_row(
                    entry["id"], result="FAILED", stage="FAILED",
                    fail_reason=reason, cooldown_until=cooldown
                )
                add_tracking_event(sym, entry["id"], "FAILED", price=price,
                                   change_pct=gain*100, drawdown_pct=drawdown*100,
                                   progress_class=progress_class, note=reason)
                logger.info(f"❌ {sym} 启动失效(内部): {reason}")
                with early_lock:
                    pending_early.pop(sym, None)
                continue

            # 极端回撤
            if drawdown >= EXTREME_DRAWDOWN:
                reason = "启动后最大回撤达到5%"
                cooldown = now_beijing() + timedelta(seconds=LONG_FAIL_COOLDOWN_SEC)
                update_early_row(
                    entry["id"], result="FAILED", stage="FAILED",
                    fail_reason=reason, cooldown_until=cooldown
                )
                add_tracking_event(sym, entry["id"], "EXTREME_FAIL", price=price,
                                   change_pct=gain*100, drawdown_pct=drawdown*100,
                                   progress_class=progress_class, note=reason)
                logger.info(f"❌ {sym} 启动失效(内部): {reason}")
                with early_lock:
                    pending_early.pop(sym, None)
                continue

            # 3分钟
            if elapsed >= FAST_CHECK_3M_SEC and not entry["check_3m_done"]:
                entry["check_3m_done"] = True
                entry["stage"] = "VALIDATING_6M"
                update_early_row(entry["id"], stage="VALIDATING_6M", price_3m=price)
                add_tracking_event(sym, entry["id"], "3M_CHECK", price=price,
                                   change_pct=gain*100, drawdown_pct=drawdown*100,
                                   progress_class=progress_class, note="3分钟承接检查")

            # 6分钟
            if elapsed >= FAST_CHECK_6M_SEC and not entry["check_6m_done"]:
                entry["check_6m_done"] = True

                if len(k1) >= 6:
                    recent_vol = sum(float(x[5]) for x in k1[-3:]) / 3
                    base_vol = sum(float(x[5]) for x in k1[:-3]) / max(1, len(k1)-3)
                    vol_sustain = recent_vol > base_vol * 1.5
                else:
                    vol_sustain = False

                fast_ok = (
                    gain >= FAST_MIN_PROGRESS and
                    new_high and
                    price >= entry["trigger_price"] and
                    vol_sustain
                )
                slow_ok = (
                    gain >= SLOW_MIN_PROGRESS and
                    price >= entry["trigger_price"] and
                    drawdown < SOFT_DRAWDOWN * 1.5
                )

                if fast_ok or (slow_ok and progress_class in ("正常推进", "强推进", "爆发")):
                    entry["stage"] = "CONFIRMED"
                    entry["active"] = True
                    entry["confirm_price"] = price
                    update_early_row(entry["id"], result="CONFIRMED", stage="CONFIRMED",
                                    confirm_time=now_beijing(), confirm_price=price,
                                    price_6m=price, progress_class=progress_class)
                    add_tracking_event(sym, entry["id"], "CONFIRMED", price=price,
                                       change_pct=gain*100, drawdown_pct=drawdown*100,
                                       progress_class=progress_class, note="6分钟启动确认")

                    sig = analyze_symbol(sym, entry, market_state_cache)
                    if sig and sig.get("rejected"):
                        update_early_row(entry["id"], formal_score=sig.get("score",0),
                                        chase_risk=sig.get("chase_risk",0),
                                        fail_reason=sig.get("reject_reason","风险过滤"))
                        logger.info(f"⚠️ {sym} 启动成立但风险过滤(内部): {sig.get('reject_reason')}")
                    elif sig and sig.get("score",0) >= FORMAL_SCORE_MIN:
                        if time.time() - startup_time >= SILENT_PERIOD_SEC:
                            if should_alert(sym, "LONG", channel="confirm", max_count=3):
                                notify_signal(sig)
                                mark_alerted(sym, "LONG", channel="confirm")
                        else:
                            logger.info(f"⏳ 静默期，跳过正式启动通知: {sym}")
                        update_early_row(entry["id"], formal_score=sig["score"],
                                        chase_risk=sig.get("chase_risk",0))
                    else:
                        basic_sig = {
                            "symbol": sym, "direction": "LONG", "price": price,
                            "score": sig.get("score",0) if sig else 0, "max_score": 6,
                            "quality_label": "启动确认（后级强化不足）",
                            "oi_change": sig.get("oi_change",0) if sig else 0,
                            "volume_ratio": sig.get("volume_ratio",0) if sig else 0,
                            "funding": sig.get("funding",0) if sig else 0,
                            "post_start_pct": gain*100, "progress_class": progress_class,
                            "chase_risk": sig.get("chase_risk",0) if sig else 0,
                        }
                        if time.time() - startup_time >= SILENT_PERIOD_SEC:
                            if should_alert(sym, "LONG", channel="confirm", max_count=3):
                                notify_signal(basic_sig)
                                mark_alerted(sym, "LONG", channel="confirm")
                        else:
                            logger.info(f"⏳ 静默期，跳过正式启动通知: {sym}")

                    active_tracking[sym] = entry
                    with early_lock:
                        pending_early.pop(sym, None)
                    continue
                else:
                    entry["stage"] = "WATCH_15M"
                    update_early_row(entry["id"], stage="WATCH_15M", price_6m=price,
                                    progress_class=progress_class)
                    add_tracking_event(sym, entry["id"], "6M_WEAK", price=price,
                                       change_pct=gain*100, drawdown_pct=drawdown*100,
                                       progress_class=progress_class, note="6分钟未达标，延长到15分钟")

            # 15分钟
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
                    update_early_row(entry["id"], result="CONFIRMED", stage="CONFIRMED",
                                    confirm_time=now_beijing(), confirm_price=price,
                                    progress_class=progress_class)
                    add_tracking_event(sym, entry["id"], "CONFIRMED", price=price,
                                       change_pct=gain*100, drawdown_pct=drawdown*100,
                                       progress_class=progress_class, note="15分钟慢启动确认")
                    sig = analyze_symbol(sym, entry, market_state_cache)
                    if sig and sig.get("rejected"):
                        update_early_row(entry["id"], formal_score=sig.get("score",0),
                                        chase_risk=sig.get("chase_risk",0),
                                        fail_reason=sig.get("reject_reason","风险过滤"))
                        logger.info(f"⚠️ {sym} 慢启动成立但风险过滤(内部): {sig.get('reject_reason')}")
                    else:
                        basic_sig = {
                            "symbol": sym, "direction": "LONG", "price": price,
                            "score": sig.get("score",0) if sig else 0, "max_score": 6,
                            "quality_label": "慢启动确认",
                            "oi_change": sig.get("oi_change",0) if sig else 0,
                            "volume_ratio": sig.get("volume_ratio",0) if sig else 0,
                            "funding": sig.get("funding",0) if sig else 0,
                            "post_start_pct": gain*100, "progress_class": progress_class,
                            "chase_risk": sig.get("chase_risk",0) if sig else 0,
                        }
                        if time.time() - startup_time >= SILENT_PERIOD_SEC:
                            if should_alert(sym, "LONG", channel="confirm", max_count=3):
                                notify_signal(basic_sig)
                                mark_alerted(sym, "LONG", channel="confirm")
                        else:
                            logger.info(f"⏳ 静默期，跳过慢启动通知: {sym}")
                    active_tracking[sym] = entry
                    with early_lock:
                        pending_early.pop(sym, None)
                    continue
                else:
                    reason = "15分钟内未形成有效延续"
                    cooldown = now_beijing() + timedelta(seconds=EARLY_FAIL_COOLDOWN_SEC)
                    update_early_row(entry["id"], result="FAILED", stage="FAILED",
                                    fail_reason=reason, cooldown_until=cooldown)
                    add_tracking_event(sym, entry["id"], "TIMEOUT_FAIL", price=price,
                                       change_pct=gain*100, drawdown_pct=drawdown*100,
                                       progress_class=progress_class, note=reason)
                    logger.info(f"❌ {sym} 启动失效(内部): {reason}")
                    with early_lock:
                        pending_early.pop(sym, None)

        except Exception as e:
            logger.error(f"{sym} 启动验证失败: {e}")

# ===================== 动态跟踪 =====================
def track_active():
    for sym in list(active_tracking.keys()):
        sb = active_tracking[sym]
        try:
            trigger_dt = entry_trigger_dt(sb)
            elapsed_min = (now_beijing() - trigger_dt).total_seconds() / 60.0

            k1 = get_klines(sym, "1m", PUSH_ATR_PERIOD + 5)
            if not k1:
                continue

            price = float(k1[-1][4])
            high_now = max(float(x[2]) for x in k1[-3:])
            atr_pct = atr_percent_from_1m(k1)

            trigger_price = sb.get("trigger_price") or sb.get("first_price") or price
            trigger_low = sb.get("trigger_low") or sb.get("base_low") or price

            prev_highest = sb.get("highest_price") or trigger_price
            sb["highest_price"] = max(prev_highest, price, high_now)
            sb["daily_high"] = max(sb.get("daily_high") or price, price)
            sb["daily_low"] = min(sb.get("daily_low") or price, price)

            gain = (price - trigger_price) / trigger_price if trigger_price > 0 else 0.0
            highest = sb["highest_price"]
            drawdown = (highest - price) / highest if highest > 0 else 0.0

            sb["max_gain_seen"] = max(sb.get("max_gain_seen") or gain, gain)
            sb["max_drawdown_seen"] = max(sb.get("max_drawdown_seen") or drawdown, drawdown)

            progress_class, _ = classify_progress(gain, atr_pct)

            if elapsed_min >= TRACKING_TIMEOUT_HOURS * 60:
                update_early_row(sb["id"], result="EXPIRED", stage="EXPIRED",
                                max_price=sb["highest_price"],
                                min_price=min(sb.get("min_price") or price, price),
                                max_gain=sb["max_gain_seen"], max_drawdown=sb["max_drawdown_seen"])
                add_tracking_event(sym, sb["id"], "EXPIRED", price=price,
                                   change_pct=gain*100, drawdown_pct=drawdown*100,
                                   progress_class=progress_class, note="24小时超时")
                logger.info(f"⏰ {sym} 24小时跟踪结束(内部)")
                del active_tracking[sym]
                continue

            if price < trigger_low:
                update_early_row(sb["id"], result="FAILED", stage="FAILED",
                                max_price=sb["highest_price"], min_price=price,
                                max_gain=sb["max_gain_seen"], max_drawdown=sb["max_drawdown_seen"],
                                fail_reason="正式确认后跌破启动基准低点",
                                cooldown_until=now_beijing() + timedelta(seconds=LONG_FAIL_COOLDOWN_SEC))
                add_tracking_event(sym, sb["id"], "STRUCTURE_FAIL", price=price,
                                   change_pct=gain*100, drawdown_pct=drawdown*100,
                                   progress_class=progress_class, note="跌破启动基准低点")
                logger.info(f"❌ {sym} 启动失效(内部): 跌破启动基准低点")
                del active_tracking[sym]
                continue

            if drawdown >= EXTREME_DRAWDOWN:
                update_early_row(sb["id"], result="FAILED", stage="FAILED",
                                max_price=sb["highest_price"], min_price=price,
                                max_gain=sb["max_gain_seen"], max_drawdown=sb["max_drawdown_seen"],
                                fail_reason="正式确认后最大回撤达到5%",
                                cooldown_until=now_beijing() + timedelta(seconds=LONG_FAIL_COOLDOWN_SEC))
                add_tracking_event(sym, sb["id"], "EXTREME_FAIL", price=price,
                                   change_pct=gain*100, drawdown_pct=drawdown*100,
                                   progress_class=progress_class, note="最大回撤达到5%")
                logger.info(f"❌ {sym} 启动失效(内部): 最大回撤达到5%")
                del active_tracking[sym]
                continue

            # [FIX-12] 软回撤事件节流：10 分钟一条，避免 event 表爆炸
            if drawdown >= SOFT_DRAWDOWN:
                last_ts = sb.get("last_soft_dd_ts") or 0
                if time.time() - last_ts >= SOFT_DRAWDOWN_EVENT_GAP_SEC:
                    sb["last_soft_dd_ts"] = time.time()
                    add_tracking_event(sym, sb["id"], "SOFT_DRAWDOWN", price=price,
                                       change_pct=gain*100, drawdown_pct=drawdown*100,
                                       progress_class=progress_class, note="软回撤0.8%")

            # [FIX-1] 推进判定：微新高不前移基准，只有“有效推进”才前移
            if not sb.get("last_recorded_high"):
                sb["last_recorded_high"] = prev_highest
                sb["last_new_high_time"] = time.time()

            ref = sb["last_recorded_high"]
            if high_now > ref:
                sb["last_new_high_time"] = time.time()
                push_pct = (high_now - ref) / ref if ref > 0 else 0.0
                need = max(NORMAL_PUSH_MIN, atr_pct * NORMAL_PUSH_ATR)
                if push_pct >= need and sb.get("push_times", 0) < MAX_PUSH:
                    sb["last_recorded_high"] = high_now
                    sb["push_times"] = sb.get("push_times", 0) + 1
                    sb["push_count"] = min(MAX_PUSH, sb.get("push_count", 0) + 1)
                    update_early_row(sb["id"], max_price=sb["highest_price"],
                                    max_gain=sb["max_gain_seen"],
                                    max_drawdown=sb["max_drawdown_seen"],
                                    progress_class=progress_class)
                    add_tracking_event(sym, sb["id"], "NEW_HIGH", price=price,
                                       change_pct=push_pct*100, drawdown_pct=drawdown*100,
                                       progress_class=progress_class, note="有效新高推进")

                    if time.time() - startup_time >= SILENT_PERIOD_SEC:
                        if should_alert(sym, "LONG", channel="push", max_count=3):
                            notify_tg_only(
                                f"🚀 {sym} — {progress_class}\n"
                                f"当前价: {price:.8f}\n"
                                f"启动后: {gain*100:+.2f}%\n"
                                f"本次新高推进: {push_pct*100:+.2f}%\n"
                                f"累计推进次数: {sb.get('push_times',0)}",
                                channel="push", symbol=sym
                            )
                            mark_alerted(sym, "LONG", channel="push")
                    else:
                        logger.info(f"⏳ 静默期，跳过推进通知: {sym}")

            minutes_since_high = (time.time() - sb.get("last_new_high_time", time.time())) / 60.0

            if (elapsed_min >= MOMENTUM_WEAK_MINUTES and
                not sb.get("weak_notified") and
                minutes_since_high >= MOMENTUM_WEAK_MINUTES):
                add_tracking_event(sym, sb["id"], "MOMENTUM_WEAK", price=price,
                                   change_pct=gain*100, drawdown_pct=drawdown*100,
                                   progress_class=progress_class, note="动能减弱")
                sb["weak_notified"] = True
                logger.info(f"🟡 {sym} 动能减弱(内部)")

            if (elapsed_min >= MOMENTUM_FAIL_MINUTES and
                minutes_since_high >= MOMENTUM_FAIL_MINUTES and
                gain < FAST_MIN_PROGRESS):
                reason = "30分钟无有效延续"
                update_early_row(sb["id"], result="FLAT", stage="FLAT",
                                max_price=sb["highest_price"],
                                min_price=min(sb.get("min_price") or price, price),
                                max_gain=sb["max_gain_seen"], max_drawdown=sb["max_drawdown_seen"],
                                fail_reason=reason)
                add_tracking_event(sym, sb["id"], "FLAT_EXIT", price=price,
                                   change_pct=gain*100, drawdown_pct=drawdown*100,
                                   progress_class=progress_class, note=reason)
                logger.info(f"⏸ {sym} 30分钟无有效延续，退出跟踪(内部)")
                del active_tracking[sym]
                continue

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
        except Exception as e:
            logger.debug(f"{sym} 趋势检测失败: {e}")
        return None

    with ThreadPoolExecutor(max_workers=MAX_WORKERS_TREND) as ex:
        results = list(ex.map(check_trend, target_syms))

    hits = 0
    for r in results:
        if not r:
            continue
        sym, price, change_24h, bull_count, at_high_pct = r
        if not should_trend_alert(sym):
            continue
        if time.time() - startup_time < SILENT_PERIOD_SEC:
            logger.info(f"⏳ 静默期，跳过趋势通知: {sym}")
            continue
        trend_count = get_trend_count(sym) + 1
        notify_trend(sym, price, change_24h, bull_count, at_high_pct, trend_count)
        mark_trend_alerted(sym)
        # notify_trend -> notify_tg_email 里已 record_channel，这里不再重复写
        hits += 1
        logger.info(f"📈 趋势信号: {sym} 24h涨幅{change_24h:+.2f}% (第{trend_count}次)，距24h高{at_high_pct:.1f}%")
    logger.info(f"📈 趋势扫描完成，发现 {hits} 个趋势信号")

# ===================== 状态恢复 =====================
def recover_state():
    """[FIX-10] 进程重启后，按 24h 窗口重建 pending / active，避免 PENDING 永久挂起。"""
    cutoff = (now_beijing() - timedelta(hours=TRACKING_TIMEOUT_HOURS)).isoformat()
    try:
        with db_lock:
            rows = db_conn.execute("""
                SELECT id, symbol, trigger_time, trigger_price, trigger_high, trigger_low,
                       start_pct, early_score, volume_ratio, stage, result,
                       max_price, min_price, max_gain, max_drawdown
                FROM early_signal_log
                WHERE trigger_time > ? AND result IN ('PENDING','CONFIRMED')
                ORDER BY id ASC
            """, (cutoff,)).fetchall()
    except Exception as e:
        logger.error(f"状态恢复失败: {e}")
        return

    for row in rows:
        try:
            (eid, sym, trig, tprice, thigh, tlow, start_pct, escore, vratio,
             stage, result, max_price, min_price, max_gain, max_drawdown) = row
            if sym in pending_early or sym in active_tracking:
                continue
            tdt = parse_ts(trig)
            tprice = float(tprice)
            entry = {
                "id": eid,
                "symbol": sym,
                "trigger_time": tdt.timestamp(),
                "trigger_dt": tdt,
                "trigger_price": tprice,
                "trigger_high": float(thigh or tprice),
                "trigger_low": float(tlow or tprice),
                "start_pct": float(start_pct or 0),
                "early_score": int(escore or 0),
                "volume_ratio": float(vratio or 0),
                "max_price": float(max_price or tprice),
                "min_price": float(min_price or tprice),
                "highest_price": float(max_price or tprice),
                "last_high": float(thigh or tprice),
                "last_check_price": tprice,
                "check_3m_done": stage in ("VALIDATING_6M", "WATCH_15M"),
                "check_6m_done": stage == "WATCH_15M",
                "check_15m_done": False,
                "weak_notified": False,
                "source": "recover",
                "stage": stage or "EARLY",
                "active": result == "CONFIRMED",
                "new_high_observed": False,
                "max_gain_seen": float(max_gain) if max_gain is not None else -1.0,
                "max_drawdown_seen": float(max_drawdown or 0),
                "last_recorded_high": float(max_price or tprice),
                "last_new_high_time": time.time(),
            }
            if result == "CONFIRMED":
                active_tracking[sym] = entry
            else:
                pending_early[sym] = entry
        except Exception as e:
            logger.warning(f"恢复 {row[1]} 失败: {e}")

    logger.info(f"♻️ 状态恢复：待验证 {len(pending_early)}，跟踪中 {len(active_tracking)}（来源{len(rows)}条历史）")

# ===================== 日报 =====================
def normalize_fail_reason(reason):
    """[FIX-7] 失败原因归一化，避免自由文本无法聚合。"""
    r = (reason or "").strip()
    if not r:
        return "未分类"
    rules = [
        ("跌破", "跌破启动低点"),
        ("回撤", "回撤超限"),
        ("15分钟", "15分钟未延续"),
        ("30分钟", "30分钟无延续"),
        ("24小时", "跟踪超时"),
        ("超时", "跟踪超时"),
        ("追高", "风险过滤"),
        ("市场状态", "风险过滤"),
        ("1h涨幅", "风险过滤"),
        ("全市场", "风险过滤"),
    ]
    for key, label in rules:
        if key in r:
            return label
    return "其他"

def generate_daily_report(report_date=None):
    if report_date is None:
        report_date = now_beijing().date()

    date_start = datetime(report_date.year, report_date.month, report_date.day,
                         tzinfo=timezone(timedelta(hours=8)))
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
            LIMIT ?
        """, (date_start.isoformat(), date_end.isoformat(), EVENT_SHEET_MAX_ROWS)).fetchall()

        event_stats = db_conn.execute("""
            SELECT event_type, COUNT(*), AVG(change_pct), AVG(drawdown_pct)
            FROM tracking_events
            WHERE event_time>=? AND event_time<?
            GROUP BY event_type
            ORDER BY COUNT(*) DESC
        """, (date_start.isoformat(), date_end.isoformat())).fetchall()

        ch_rows = db_conn.execute("""
            SELECT channel, COUNT(*), COUNT(DISTINCT symbol)
            FROM channel_log
            WHERE sent_at>=? AND sent_at<?
            GROUP BY channel
            ORDER BY COUNT(*) DESC
        """, (date_start.isoformat(), date_end.isoformat())).fetchall()

    cols = [
        "ID","币种","启动时间","启动价","3M启动幅度%","预警评分",
        "初始量能比","最终阶段","最终结果","确认时间","确认价",
        "3M价格","6M价格","15M价格","最高价","最低价",
        "最大涨幅%","最大回撤%","推进等级","追高风险","正式评分","失败原因"
    ]
    df = pd.DataFrame(early_rows, columns=cols)

    if not df.empty:
        df["3M启动幅度%"] = pd.to_numeric(df["3M启动幅度%"], errors="coerce").fillna(0).round(2)
        # [FIX-6] DB 里 max_gain / max_drawdown 是小数（0.02），报表统一 ×100
        for c in ("最大涨幅%", "最大回撤%"):
            df[c] = (pd.to_numeric(df[c], errors="coerce").fillna(0) * 100).round(2)
        # [FIX-6] 时区：先按 UTC 解析再转上海，兼容历史混合偏移的数据
        for c in ("启动时间", "确认时间"):
            df[c] = (pd.to_datetime(df[c], errors="coerce", utc=True)
                     .dt.tz_convert("Asia/Shanghai").dt.tz_localize(None))
        df["失败原因分类"] = df["失败原因"].apply(normalize_fail_reason)

        now_naive = now_beijing().replace(tzinfo=None)
        df["成熟度"] = [
            "未知" if pd.isna(t) else
            ("已成熟" if (now_naive - t) >= timedelta(hours=TRACKING_TIMEOUT_HOURS) else "跟踪中")
            for t in df["启动时间"]
        ]

    total = len(df)

    if total:
        has_confirm = df["确认时间"].notna()
        confirmed = int(has_confirm.sum())                      # [FIX-7] 用确认时间判定
        confirm_then_fail = int((has_confirm & df["最终结果"].isin(
            ["FAILED", "FLAT", "EXPIRED"])).sum())
        failed = int((df["最终结果"] == "FAILED").sum())
        flat = int((df["最终结果"] == "FLAT").sum())
        pending = int((df["最终结果"] == "PENDING").sum())
        expired = int((df["最终结果"] == "EXPIRED").sum())
        p3 = int(df["3M价格"].notna().sum())
        p6 = int(df["6M价格"].notna().sum())
        p15 = int(df["15M价格"].notna().sum())
        confirm_rate = confirmed / total * 100
        mature_rows = df[df["成熟度"] == "已成熟"]
        if len(mature_rows):
            mature_rate = mature_rows["确认时间"].notna().sum() / len(mature_rows) * 100
        else:
            mature_rate = 0.0
        mfe = float(df["最大涨幅%"].max()) if total else 0.0
        mfe_median = float(df["最大涨幅%"].median()) if total else 0.0
        mae_median = float(df["最大回撤%"].median()) if total else 0.0
    else:
        confirmed = confirm_then_fail = failed = flat = pending = expired = 0
        p3 = p6 = p15 = 0
        confirm_rate = mature_rate = mfe = mfe_median = mae_median = 0.0

    pre_alert = total

    # 漏斗：每层附“相对上一阶段”转化率
    stages = [
        ("启动预警", pre_alert),
        ("记录到3分钟", p3),
        ("进入6分钟节点", p6),
        ("进入15分钟节点", p15),
        ("正式确认", confirmed),
    ]
    rows = []
    prev = None
    for name, cnt in stages:
        pct = cnt / pre_alert * 100 if pre_alert else 0.0
        step = (cnt / prev * 100) if (prev and prev > 0) else (100.0 if name == "启动预警" else 0.0)
        rows.append([name, int(cnt), round(pct, 2), round(step, 2)])
        prev = cnt
    funnel = pd.DataFrame(rows, columns=["阶段", "数量", "占启动预警比例%", "相对上一阶段%"])

    result_rows = [
        ["最终确认（未被推翻）", confirmed, round(confirmed / pre_alert * 100, 2) if pre_alert else 0],
        ["确认后失效/横盘/超时", confirm_then_fail,
         round(confirm_then_fail / pre_alert * 100, 2) if pre_alert else 0],
        ["失败（未确认即失效）", failed, round(failed / pre_alert * 100, 2) if pre_alert else 0],
        ["横盘退出", flat, round(flat / pre_alert * 100, 2) if pre_alert else 0],
        ["跟踪超时", expired, round(expired / pre_alert * 100, 2) if pre_alert else 0],
        ["仍待处理", pending, round(pending / pre_alert * 100, 2) if pre_alert else 0],
    ]
    results_df = pd.DataFrame(result_rows, columns=["结果", "数量", "占比%"])

    summary_rows = [
        ["启动预警数", pre_alert],
        ["确认率（全样本）%", round(confirm_rate, 2)],
        ["确认率（仅已成熟样本）%", round(mature_rate, 2)],
        ["已成熟样本数", int((df["成熟度"] == "已成熟").sum()) if total else 0],
        ["跟踪中（未满24h）", int((df["成熟度"] == "跟踪中").sum()) if total else 0],
        ["确认后失效率%", round(confirm_then_fail / confirmed * 100, 2) if confirmed else 0],
        ["最大涨幅 MFE 均值%", round(mfe, 2)],
        ["最大涨幅 MFE 中位数%", round(mfe_median, 2)],
        ["最大回撤 MAE 中位数%", round(mae_median, 2)],
    ]
    summary_df = pd.DataFrame(summary_rows, columns=["指标", "数值"])

    failure_df = pd.DataFrame(columns=["失败原因分类", "数量", "占失败比例%"])
    if total:
        fails = df.loc[df["最终结果"].isin(["FAILED", "FLAT", "EXPIRED"])]
        if not fails.empty:
            vc = fails["失败原因分类"].value_counts()
            failure_df = pd.DataFrame({"失败原因分类": vc.index, "数量": vc.values})
            failure_df["占失败比例%"] = (failure_df["数量"] / max(1, len(fails)) * 100).round(2)

    best_df = pd.DataFrame()
    if total:
        best_df = df.sort_values(["最大涨幅%", "正式评分"], ascending=[False, False]).head(10).copy()
        best_df = best_df[["币种","启动时间","启动价","最高价","最大涨幅%",
                           "最大回撤%","最终结果","正式评分","推进等级","成熟度"]]

    ch_df = pd.DataFrame(
        [[CHANNEL_CN.get(r[0], r[0]), int(r[1]), int(r[2])] for r in ch_rows],
        columns=["通道", "条数", "覆盖币种数"]
    ) if ch_rows else pd.DataFrame([["当天没有对外通知"]], columns=["说明"])

    event_cols = ["币种","启动ID","事件时间","事件类型","价格","变化%","回撤%","推进等级","说明"]
    events_df = pd.DataFrame(event_rows, columns=event_cols)
    if not events_df.empty:
        events_df["事件时间"] = (pd.to_datetime(events_df["事件时间"], errors="coerce", utc=True)
                                 .dt.tz_convert("Asia/Shanghai").dt.tz_localize(None))

    if event_stats:
        ev_stat_df = pd.DataFrame(
            [[r[0], int(r[1]), round(float(r[2] or 0), 2), round(float(r[3] or 0), 2)]
             for r in event_stats],
            columns=["事件类型", "数量", "平均变化%", "平均回撤%"]
        )
    else:
        ev_stat_df = pd.DataFrame([["当天没有跟踪事件"]], columns=["说明"])

    fname = f"日报_{report_date.strftime('%Y%m%d')}.xlsx"

    try:
        with pd.ExcelWriter(fname, engine="openpyxl") as writer:
            summary_df.to_excel(writer, sheet_name="概览", index=False)
            funnel.to_excel(writer, sheet_name="启动漏斗", index=False)
            results_df.to_excel(writer, sheet_name="结果分布", index=False)
            ch_df.to_excel(writer, sheet_name="通道统计", index=False)

            if df.empty:
                pd.DataFrame([["当天没有启动预警记录"]], columns=["说明"]).to_excel(
                    writer, sheet_name="信号明细", index=False)
            else:
                df.to_excel(writer, sheet_name="信号明细", index=False)

            if failure_df.empty:
                pd.DataFrame([["当天没有失败记录"]], columns=["说明"]).to_excel(
                    writer, sheet_name="失败分析", index=False)
            else:
                failure_df.to_excel(writer, sheet_name="失败分析", index=False)

            if best_df.empty:
                pd.DataFrame([["当天没有最佳信号"]], columns=["说明"]).to_excel(
                    writer, sheet_name="最佳信号", index=False)
            else:
                best_df.to_excel(writer, sheet_name="最佳信号", index=False)

            ev_stat_df.to_excel(writer, sheet_name="事件统计", index=False)

            if events_df.empty:
                pd.DataFrame([["当天没有跟踪事件"]], columns=["说明"]).to_excel(
                    writer, sheet_name="跟踪事件", index=False)
            else:
                events_df.to_excel(writer, sheet_name="跟踪事件", index=False)

    except Exception as e:
        logger.error(f"📊 日报Excel生成失败: {e}")
        send_tg(f"❌ 日报Excel生成失败: {e}")
        return False

    top_text = "无"
    if not best_df.empty:
        top = best_df.iloc[0]
        top_text = f"{top['币种']}，启动后最大涨幅 {float(top['最大涨幅%']):+.2f}%"

    body = (
        f"📊 盘面日报 {report_date}\n\n"
        f"启动预警：{pre_alert}\n"
        f"正式确认：{confirmed}\n"
        f"确认后失效：{confirm_then_fail}\n"
        f"失败：{failed}\n"
        f"横盘退出：{flat}\n"
        f"超时：{expired}\n"
        f"仍待处理：{pending}\n\n"
        f"确认率（全样本）：{confirm_rate:.2f}%\n"
        f"确认率（仅已成熟）：{mature_rate:.2f}%\n"
        f"确认后失效率：{(confirm_then_fail / confirmed * 100) if confirmed else 0:.2f}%\n"
        f"MFE 中位数：{mfe_median:+.2f}%\n\n"
        f"今日最佳：{top_text}\n\n"
        f"附件包含：概览、启动漏斗、结果分布、通道统计、信号明细、失败分析、最佳信号、事件统计、跟踪事件。"
    )

    email_ok = send_email_attach(f"📊 [盘面日报] {report_date}", body, fname)

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
        """, (str(report_date), now_beijing().isoformat(), fname,
              1 if email_ok else 0, 1, "" if email_ok else "邮件发送失败"))
        db_conn.commit()

    logger.info(f"📊 {report_date} 日报生成完成：{fname}，预警{pre_alert}，确认{confirmed}，邮件{'成功' if email_ok else '失败'}")

    tg_text = (
        f"📊 {report_date} 盘面日报\n"
        f"启动预警: {pre_alert}\n"
        f"正式确认: {confirmed}\n"
        f"确认后失效: {confirm_then_fail}\n"
        f"失败: {failed}\n"
        f"横盘: {flat}\n"
        f"超时: {expired}\n"
        f"确认率: {confirm_rate:.2f}%（成熟样本 {mature_rate:.2f}%）\n"
        f"最佳: {top_text}\n"
        f"邮箱日报: {'✅' if email_ok else '❌'}"
    )
    send_tg(tg_text)

    return email_ok

def report_already_sent(report_date):
    with db_lock:
        cur = db_conn.execute(
            "SELECT email_ok FROM daily_report_status WHERE report_date=?",
            (str(report_date),))
        row = cur.fetchone()
        return bool(row and row[0] == 1)

def maybe_send_daily_report():
    """[FIX-8] 失败不再每 30 秒重试：最多 3 次，间隔 10 分钟。"""
    now = now_beijing()
    if now.hour < DAILY_REPORT_HOUR:
        return
    if now.hour == DAILY_REPORT_HOUR and now.minute < DAILY_REPORT_MINUTE:
        return

    report_date = now.date() - timedelta(days=1)

    with db_lock:
        row = db_conn.execute(
            "SELECT email_ok, attempts, generated_at FROM daily_report_status WHERE report_date=?",
            (str(report_date),)).fetchone()

    if row:
        email_ok, attempts, generated_at = row
        if email_ok:
            return
        if (attempts or 0) >= DAILY_REPORT_MAX_ATTEMPTS:
            return
        if generated_at:
            try:
                if (now - parse_ts(generated_at)).total_seconds() < DAILY_REPORT_RETRY_SEC:
                    return
            except Exception:
                pass

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
            """, (str(report_date), now_beijing().isoformat(), "", 0, 1, str(e)))
            db_conn.commit()
        send_tg(f"❌ 日报任务异常: {e}")

def cleanup_old_data():
    """[FIX-9] 原函数从未被调用；现在每日执行一次，并清理过期 xlsx。"""
    cutoff = now_beijing() - timedelta(days=REPORT_KEEP_DAYS)
    cutoff_iso = cutoff.isoformat()
    deleted_files = 0
    try:
        with db_lock:
            db_conn.execute("DELETE FROM sent_log WHERE sent_at < ?", (cutoff_iso,))
            db_conn.execute("DELETE FROM trend_log WHERE sent_at < ?", (cutoff_iso,))
            db_conn.execute("DELETE FROM channel_log WHERE sent_at < ?", (cutoff_iso,))
            db_conn.execute("DELETE FROM tracking_events WHERE event_time < ?", (cutoff_iso,))
            db_conn.execute("DELETE FROM early_signal_log WHERE trigger_time < ?", (cutoff_iso,))
            db_conn.execute("DELETE FROM daily_report_status WHERE report_date < ?",
                           (str(now_beijing().date() - timedelta(days=REPORT_KEEP_DAYS)),))
            db_conn.commit()
        limit_date = now_beijing().date() - timedelta(days=REPORT_KEEP_DAYS)
        for p in Path(".").glob("日报_*.xlsx"):
            try:
                stem = p.stem.replace("日报_", "")
                d = datetime.strptime(stem, "%Y%m%d").date()
                if d < limit_date:
                    p.unlink()
                    deleted_files += 1
            except Exception:
                continue
        logger.info(f"🗑️ 已清理{REPORT_KEEP_DAYS}天前的历史数据，删除旧日报 {deleted_files} 个")
    except Exception as e:
        logger.error(f"🗑️ 清理旧数据失败: {e}")

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
    if not all(v < vol_ma20 for v in recent_vols):
        return None
    change_30d = (price_now - closes[-30]) / closes[-30] * 100 if len(closes) >= 30 else 0
    return {"change_7d": change_7d, "change_30d": change_30d,
            "dist_to_low": dist_to_low, "low_30d": low_30d, "vol_ma20": vol_ma20}

def check_stage2_surge(klines, stage1):
    if not klines or len(klines) < 10:
        return None
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
        if (k["qv"] >= avg5 * VOL_SURGE_MULT and
            SURGE_CHANGE_MIN <= change <= SURGE_CHANGE_MAX and k["c"] > prev["c"]):
            return {"surge_day": i, "surge_vol_ratio": k["qv"] / avg5,
                    "surge_change": change, "surge_low": k["l"], "surge_close": k["c"]}
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
    if any(k["l"] < surge_low for k in after):
        return None
    lows_rising = all(after[i]["l"] >= after[i-1]["l"] * 0.998 for i in range(1, len(after)))
    return {"consolidation_days": len(after), "range_pct": range_pct*100,
            "lows_rising": lows_rising, "held_above_surge_low": True}

def check_oi_and_funding(symbol):
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
        lines += ["", "📊 底部放量",
                  f" 放量倍数: {stage2['surge_vol_ratio']:.1f}x",
                  f" 放量当日涨幅: {stage2['surge_change']:.1f}%"]
    if stage3:
        lines += ["", "📐 横盘确认",
                  f" 横盘天数: {stage3['consolidation_days']}天",
                  f" 波动幅度: {stage3['range_pct']:.1f}%",
                  f" 低点抬高: {'是' if stage3['lows_rising'] else '否'}"]
    lines += ["", "📈 OI/资金费率",
              f" OI变化: {oi_data['oi_change']:+.1f}%",
              f" OI增加: {'是' if oi_data['oi_increasing'] else '否'}",
              f" 资金费率: {oi_data['funding_avg']:.4f}%"]
    return "\n".join(lines)

def scan_building_signals():
    logger.info("📐 建仓通道开始扫描")
    candidates = [sym for sym, data in ticker_cache.items()
                  if data['volume'] >= MIN_VOLUME_24H_BUILD and sym not in BUILD_BLACKLIST]
    if not candidates:
        logger.info("📐 无候选币种")
        return
    count = {"observe": 0, "watch": 0, "entry": 0}
    for sym in candidates:
        try:
            # [FIX-11] 只用已收盘日线，避免日线结构重绘
            klines = get_klines_parsed(sym, "1d", 40)
            if not klines or len(klines) < 11:
                continue
            klines = klines[:-1]
            if len(klines) < 10:
                continue
            price = klines[-1]["c"]
            stage1 = check_stage1_decline(klines)
            if not stage1:
                continue
            stage2 = check_stage2_surge(klines, stage1)
            stage3 = check_stage3_consolidation(klines, stage2) if stage2 else None
            oi_data = check_oi_and_funding(sym)
            if not oi_data["funding_ok"]:
                continue
            level = classify_signal(stage1, stage2, stage3, oi_data)
            if not level:
                continue
            if level == "observe":
                with db_lock:
                    db_conn.execute("INSERT OR REPLACE INTO building_state VALUES (?,?,?)",
                                    (sym, "observe", now_beijing()))
                    db_conn.commit()
                count["observe"] += 1
                continue
            if time.time() - startup_time < SILENT_PERIOD_SEC:
                logger.info(f"⏳ 静默期，跳过建仓通知: {sym}")
                continue
            if not should_alert(sym, "LONG", channel="building", max_count=3):
                continue
            with db_lock:
                cur = db_conn.execute("SELECT level FROM building_state WHERE symbol=?", (sym,))
                row = cur.fetchone()
                prev_level = row[0] if row else None
            if prev_level == level:
                continue
            msg = format_building_signal(sym, level, price, stage1, stage2, stage3, oi_data)
            notify_all(msg)
            mark_alerted(sym, "LONG", channel="building")
            record_channel(sym, "building")
            with db_lock:
                db_conn.execute("INSERT OR REPLACE INTO building_state VALUES (?,?,?)",
                                (sym, level, now_beijing()))
                db_conn.commit()
            count[level] += 1
            logger.info(f"📐 建仓推送: {sym} {level}")
        except Exception as e:
            logger.error(f"📐 {sym} 建仓检测失败: {e}")
    logger.info(f"📐 建仓扫描完成: 观察{count['observe']} 关注{count['watch']} 介入{count['entry']}")

# ===================== 主循环 =====================
def main_loop():
    global startup_time, market_state_cache
    startup_time = time.time()

    logger.info("🚀 融合监控系统启动（启动雷达 + 四通道 + 动态日报 · 修复版）")

    startup_notice = (
        "✅ 融合监控系统启动\n"
        "🟡 通道0：启动雷达（1M + 3M）\n"
        "🚀 通道1：启动确认（3~6分钟，慢启动延长至15分钟）\n"
        "⚡ 通道2：快速拉升（3M HH ≥ 2%）\n"
        "📈 通道3：趋势信号（1H）\n"
        "📐 通道4：建仓信号（日线结构）\n"
        "📊 日报：北京时间00:05，SQLite重算后发送Excel\n"
        "📮 实时邮件：正式启动/速报/趋势/建仓关注与介入\n"
        "📱 TG：推进、日报摘要（内部信号静默）"
    )
    notify_all(startup_notice, "监控系统启动")

    symbols = get_symbols()
    if not symbols or not high_volume_symbols:
        logger.error("无可用币种或无法获取成交额数据，退出")
        notify_all("❌ 系统启动失败：无法获取币种列表或成交额数据", "系统启动失败")
        return

    # [FIX-10] 重启后恢复未完成的状态机
    recover_state()

    last_refresh = time.time()
    last_market_state_scan = 0.0
    last_trend_submit = 0.0
    last_build_submit = 0.0
    last_cleanup_day = None
    market_state_cache = {"btc_change_15m": 0.0, "market_down_ratio": 0.0}

    try:
        import openpyxl  # noqa: F401
    except ImportError:
        logger.error("❌ 缺少openpyxl库，日报功能将不可用。请执行: pip install openpyxl")
        send_tg("❌ 缺少openpyxl库，日报功能将不可用。")

    # [FIX-15] 建仓/趋势扫描后台执行，不阻塞 3/6/15 分钟节点
    bg_executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="bg")
    trend_future = None
    build_future = None

    next_cycle = time.monotonic()

    try:
        while True:
            try:
                maybe_send_daily_report()

                now_bj = now_beijing()
                today = now_bj.date()

                # [FIX-9] 每日一次数据清理
                if last_cleanup_day != today and now_bj.hour >= DAILY_REPORT_HOUR:
                    if now_bj.hour > DAILY_REPORT_HOUR or now_bj.minute >= DAILY_REPORT_MINUTE:
                        cleanup_old_data()
                        last_cleanup_day = today

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
                        # 清理已下架币种的缓存
                        sym_set = set(symbols)
                        for s in [x for x in list(state_b.keys())
                                  if x not in sym_set and x not in pending_early
                                  and x not in active_tracking]:
                            state_b.pop(s, None)

                if time.time() - last_market_state_scan >= 120:
                    try:
                        market_state_cache = get_market_state(symbols)
                    except Exception as e:
                        logger.warning(f"市场状态更新失败: {e}")
                    last_market_state_scan = time.time()

                # [FIX-14] 每轮一次预加载黑名单
                blocked = build_scan_blocklist()

                with ThreadPoolExecutor(max_workers=MAX_WORKERS_TRIGGER) as ex:
                    list(ex.map(lambda s: scan_trigger(s, blocked), symbols))
                logger.info(f"🟡 启动雷达完成，观察池 {len(pending_early)}，跟踪池 {len(active_tracking)}")

                process_early_validation()

                # [FIX-15] 趋势扫描后台跑
                if (time.time() - last_trend_submit >= TREND_SCAN_INTERVAL
                        and (trend_future is None or trend_future.done())):
                    trend_future = bg_executor.submit(scan_trends, list(symbols))
                    last_trend_submit = time.time()

                # [FIX-15] 建仓扫描后台跑
                if (time.time() - last_build_submit >= BUILD_SCAN_INTERVAL
                        and (build_future is None or build_future.done())):
                    build_future = bg_executor.submit(scan_building_signals)
                    last_build_submit = time.time()

                if active_tracking:
                    track_active()

                # [FIX-17] 定拍，避免扫描耗时累积漂移
                next_cycle += SCAN_INTERVAL
                delay = next_cycle - time.monotonic()
                if delay > 0:
                    time.sleep(delay)
                else:
                    next_cycle = time.monotonic()

            except KeyboardInterrupt:
                logger.info("🛑 手动退出")
                break
            except Exception as e:
                logger.error(f"主循环异常: {e}")
                time.sleep(10)
                next_cycle = time.monotonic()
    finally:
        bg_executor.shutdown(wait=False)
        try:
            db_conn.commit()
            db_conn.close()
        except Exception:
            pass
        logger.info("👋 已退出")

if __name__ == "__main__":
    main_loop()