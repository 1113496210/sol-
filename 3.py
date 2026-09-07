# -*- coding: utf-8 -*-
"""
币圈 USDT 永续 LONG ONLY 多周期交易雷达（修正版）

交易框架（本程序全新设计，不继承旧系统）：
    4H = 交易级别 / 大周期
    1H  = 趋势确认
    15M = 右侧入场

核心思想：
    历史K线建立结构背景 -> 当前已收盘K线确认 -> 早期右侧入场。
    只做多；不抄底；不追极端拉升；不因为单一指标而发信号。

硬门槛：
    24H USDT 永续成交额 >= 10,000,000 USDT
    OI 市值 >= 10,000,000 USDT

正式 LONG ENTRY 必须同时满足：
    1) 4H：结构转强（Higher Low + Higher High）
    2) 1H：趋势确认（HH + HL + 价格站上1H VWAP）
    3) 15M：突破 -> 回踩 -> 再启动
    4) 15M：真实成交量 + Taker Buy 支持
    5) OI：1H 同步增加
    6) Funding：不能极端拥挤
    7) RSI：4H / 15M 不处于严重过热区，且无明显顶背离
    8) ATR：不能已经脱离突破位过远

通知：
    Telegram + QQ 邮箱（可分别开关）
    正式通知：LONG ENTRY / LONG INVALIDATED / DAILY REPORT
    默认不发送内部 WATCH / FILTER 噪音

日报：
    每天北京时间 00:05 发送前一日信号摘要
    SQLite 保存全部信号，便于后续统计真实命中率与优化参数

依赖：
    pip install requests
"""

import os
import csv
import time
import math
import sqlite3
import logging
import smtplib
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from email.mime.text import MIMEText
from email.utils import formataddr
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from typing import Dict, List, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# ========================= 配置区 =========================
BINANCE_API = "https://fapi.binance.com"

# Telegram / QQ 凭据
# 兼容当前文件中的写法；实际部署也支持环境变量覆盖。
TELEGRAM_TOKEN = os.getenv(
    "TELEGRAM_TOKEN",
    "7874342652:AAFQKWIrSVszpi1z60ixnr-VYpXf26rG8UY",
)
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "5408890841")

ENABLE_EMAIL = True
QQ_EMAIL = os.getenv("QQ_EMAIL", "1113496210@qq.com")
QQ_AUTH_CODE = os.getenv("QQ_AUTH_CODE", "gnlxwgxduzexgeag")
EMAIL_TO = os.getenv("EMAIL_TO", "1113496210@qq.com")
SMTP_HOST = "smtp.qq.com"
SMTP_PORT = 465

# 通知开关
ENABLE_TELEGRAM = True
ENABLE_EMAIL = True
NOTIFY_ENTRY = True
NOTIFY_INVALIDATED = True
NOTIFY_DAILY_REPORT = True

# 扫描节奏
SCAN_INTERVAL_SEC = 60
SYMBOL_REFRESH_SEC = 3600
REQUEST_TIMEOUT = 12
MAX_WORKERS = 16
MAX_FAILURE_RETRIES = 2

# Binance USDⓈ-M Futures 当前 REQUEST_WEIGHT 上限：2400 / 1分钟。
# 按用户要求，不追求极限，而是固定留出约15%安全距离。
BINANCE_REQUEST_WEIGHT_LIMIT = 2400
BINANCE_WEIGHT_UTILIZATION = 0.85
BINANCE_SAFE_WEIGHT_PER_MIN = int(BINANCE_REQUEST_WEIGHT_LIMIT * BINANCE_WEIGHT_UTILIZATION)  # 2040
BINANCE_WEIGHT_INTERVAL_SEC = 60.0 / BINANCE_SAFE_WEIGHT_PER_MIN  # 约29.41ms / weight

# ========================= 市场门槛 =========================
MIN_24H_VOLUME_USDT = 10_000_000       # 1000万
MIN_OI_VALUE_USDT = 10_000_000          # 1000万

# 仅监控 USDT 永续；稳定币/主要锚定币默认排除。
BLACKLIST = {
    "USDCUSDT", "FDUSDUSDT", "TUSDUSDT", "USDEUSDT", "DAIUSDT",
    "EURUSDT", "GBPUSDT", "JPYUSDT", "AUDUSDT",
}

# ========================= 周期参数 =========================
# 4H：90根约 15 天，用于大背景；20~30根看近期结构；最后 5~8 根看当前状态。
TF_4H_LIMIT = 90
TF_1H_LIMIT = 120
TF_15M_LIMIT = 96

# 结构识别采用 confirmed swing，不使用当前未收盘K线。
SWING_LEFT = 2
SWING_RIGHT = 2

# 4H：允许“结构转强”而不要求多年/数周新高。
FOUR_H_BREAK_LOOKBACK = 18
FOUR_H_MAX_DISTANCE_FROM_SWING_HIGH = 0.012   # 允许离关键高点最多 1.2%

# 1H趋势
ONE_H_MIN_SWING_IMPROVEMENT = 0.0015          # 低点/高点至少改善 0.15%
ONE_H_VWAP_WINDOW = 48

# 15M入场：最近结构阻力 + 回踩 + 再启动
ENTRY_RESISTANCE_LOOKBACK = 12
ENTRY_PULLBACK_BARS = 4
ENTRY_BREAK_BUFFER = 0.0015                   # 收盘必须至少站上阻力 0.15%
ENTRY_CLOSE_POSITION = 0.65                    # 收盘位置 >= 65%
ENTRY_BODY_PCT = 0.0020                        # 当前15M实体至少 +0.20%（小数形式）
ENTRY_VOLUME_RATIO = 1.50                      # 15M量能 / 前20根平均
ENTRY_TAKER_BUY_RATIO = 0.58                   # buy/(buy+sell)

# ========================= 合约资金确认 =========================
OI_MIN_CHANGE_1H = 0.02                        # 1H OI 至少 +2%
OI_STRONG_CHANGE_1H = 0.05                     # +5% 以上认为更强

# Funding：使用当前资金费率做“拥挤风险过滤”
FUNDING_SOFT_MAX = 0.00030                    # +0.03%
FUNDING_HARD_MAX = 0.00080                    # +0.08%

# ========================= RSI / ATR =========================
RSI_PERIOD = 14
ATR_PERIOD = 14

FOUR_H_RSI_MIN = 50.0
FOUR_H_RSI_MAX = 72.0
ENTRY_15M_RSI_MIN = 50.0
ENTRY_15M_RSI_MAX = 73.0

# RSI 顶背离：价格创新高，但 RSI 不创新高
DIVERGENCE_LOOKBACK = 24
DIVERGENCE_PRICE_EPS = 0.001
DIVERGENCE_RSI_EPS = 1.0

# 15M：突破后不得已经远离阻力位太多
MAX_ENTRY_EXTENSION_ATR = 1.50
MAX_15M_ATR_PCT = 0.045                     # 单根15M ATR > 4.5%视为极端波动

# ========================= 信号评分 =========================
# 评分只用于质量分级；硬条件仍然必须全部满足。
ENTRY_SCORE_MIN = 80
A_PLUS_SCORE_MIN = 90

# ========================= 交易状态 =========================
# 同一个币一旦 ENTRY，在没有失效前不重复发。
ENTRY_COOLDOWN_HOURS = 8
SETUP_COOLDOWN_HOURS = 12
INVALIDATION_COOLDOWN_HOURS = 2

# 1H/4H 失效规则
INVALIDATE_ON_1H_HL_BREAK = True
INVALIDATE_4H_STRUCTURE_BREAK = True

# ========================= 数据与日志 =========================
DB_PATH = "long_only_radar.db"
LOG_FILE = "long_only_radar.log"
DAILY_REPORT_DIR = "daily_reports"
KEEP_DAYS = 45

TZ_BJ = timezone(timedelta(hours=8))


# ========================= 日志 =========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("LONG_ONLY_RADAR")


# ========================= HTTP =========================
session = requests.Session()
retry = Retry(
    total=MAX_FAILURE_RETRIES,
    backoff_factor=0.5,
    status_forcelist=(418, 429, 500, 502, 503, 504),
    allowed_methods=("GET", "POST"),
    raise_on_status=False,
)
adapter = HTTPAdapter(
    max_retries=retry,
    pool_connections=MAX_WORKERS,
    pool_maxsize=MAX_WORKERS,
)
session.mount("https://", adapter)
session.mount("http://", adapter)

# Binance 全局 REQUEST_WEIGHT 节流。
# 不是简单限制“请求次数/秒”，而是按 Binance 的 REQUEST_WEIGHT 计费。
# 目标固定在官方2400/min的85%=2040 weight/min，约34 weight/s，保留15%余量。
_rate_lock = Lock()
_next_weight_slot = time.monotonic()

def _request_weight(path: str, params: Optional[dict]) -> int:
    params = params or {}

    # USDⓈ-M Futures 常用接口的权重；K线按 limit 分档。
    if path == "/fapi/v1/klines":
        limit = int(params.get("limit", 500))
        if limit < 100:
            return 1
        if limit < 500:
            return 2
        if limit <= 1000:
            return 5
        return 10
    if path == "/fapi/v1/ticker/24hr":
        return 1 if params.get("symbol") else 40
    if path == "/fapi/v1/premiumIndex":
        return 1 if params.get("symbol") else 10
    if path in (
        "/fapi/v1/exchangeInfo",
        "/fapi/v1/openInterest",
        "/futures/data/openInterestHist",
        "/futures/data/takerlongshortRatio",
    ):
        return 1
    # 本程序目前不应依赖未知高权重接口；未知接口按1计，便于扩展。
    return 1


def throttle_binance(weight: int = 1):
    global _next_weight_slot
    weight = max(int(weight), 1)
    with _rate_lock:
        now = time.monotonic()
        start = max(now, _next_weight_slot)
        _next_weight_slot = start + weight * BINANCE_WEIGHT_INTERVAL_SEC
        wait = start - now
    if wait > 0:
        time.sleep(wait)


# ========================= 数据库 =========================
db_lock = Lock()
db = sqlite3.connect(DB_PATH, check_same_thread=False)

with db:
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            signal_type TEXT NOT NULL,
            signal_time TEXT NOT NULL,
            price REAL,
            score INTEGER,
            quality TEXT,
            volume_24h REAL,
            oi_value REAL,
            oi_change_1h REAL,
            volume_ratio REAL,
            taker_buy_ratio REAL,
            funding REAL,
            rsi_4h REAL,
            rsi_15m REAL,
            atr_15m_pct REAL,
            entry_resistance REAL,
            entry_extension_atr REAL,
            reason TEXT,
            invalidated_at TEXT,
            invalidation_reason TEXT,
            active INTEGER DEFAULT 1
        )
        """
    )
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS symbol_state (
            symbol TEXT PRIMARY KEY,
            state TEXT DEFAULT 'IDLE',
            last_setup_at TEXT,
            last_entry_at TEXT,
            last_invalidated_at TEXT,
            last_entry_id INTEGER,
            last_entry_price REAL,
            last_1h_hl REAL,
            last_4h_hl REAL,
            updated_at TEXT
        )
        """
    )
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS oi_snapshots (
            symbol TEXT NOT NULL,
            ts TEXT NOT NULL,
            oi_value REAL NOT NULL,
            PRIMARY KEY(symbol, ts)
        )
        """
    )
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS daily_reports (
            report_date TEXT PRIMARY KEY,
            generated_at TEXT NOT NULL,
            filepath TEXT,
            sent_tg INTEGER DEFAULT 0,
            sent_email INTEGER DEFAULT 0
        )
        """
    )


# ========================= 工具 =========================
def now_bj() -> datetime:
    return datetime.now(TZ_BJ)


def iso_now() -> str:
    return now_bj().isoformat()


def to_float(value, default=0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def pct(a: float, b: float) -> float:
    if b == 0:
        return 0.0
    return (a - b) / b


def clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def last_closed(raw: List[list]) -> List[list]:
    """Binance Kline 最后一根通常是当前未收盘K线；正式逻辑全部去掉它。"""
    return raw[:-1] if len(raw) >= 2 else []


def candle_dicts(raw: List[list]) -> List[dict]:
    out = []
    for k in last_closed(raw):
        out.append({
            "t": int(k[0]),
            "o": to_float(k[1]),
            "h": to_float(k[2]),
            "l": to_float(k[3]),
            "c": to_float(k[4]),
            "v": to_float(k[5]),
            "qv": to_float(k[7]),
        })
    return out


# ========================= Binance API =========================
def api_get(path: str, params: Optional[dict] = None):
    weight = _request_weight(path, params)
    throttle_binance(weight)
    r = session.get(f"{BINANCE_API}{path}", params=params, timeout=REQUEST_TIMEOUT)
    if r.status_code >= 400:
        raise RuntimeError(f"Binance {r.status_code}: {r.text[:300]}")
    return r.json()


def get_exchange_symbols() -> List[str]:
    data = api_get("/fapi/v1/exchangeInfo")
    out = []
    for s in data.get("symbols", []):
        if (
            s.get("contractType") == "PERPETUAL"
            and s.get("quoteAsset") == "USDT"
            and s.get("status") == "TRADING"
            and s.get("symbol") not in BLACKLIST
        ):
            out.append(s["symbol"])
    return out


def get_24h_tickers() -> Dict[str, dict]:
    data = api_get("/fapi/v1/ticker/24hr")
    return {
        x["symbol"]: {
            "volume": to_float(x.get("quoteVolume")),
            "change_pct": to_float(x.get("priceChangePercent")),
            "price": to_float(x.get("lastPrice")),
        }
        for x in data
        if x.get("symbol")
    }


def get_klines(symbol: str, interval: str, limit: int) -> List[list]:
    return api_get(
        "/fapi/v1/klines",
        {"symbol": symbol, "interval": interval, "limit": limit},
    )


def get_oi_history(symbol: str, period: str = "1h", limit: int = 3) -> List[dict]:
    return api_get(
        "/futures/data/openInterestHist",
        {"symbol": symbol, "period": period, "limit": limit},
    )


def get_premium_index_all() -> Dict[str, dict]:
    data = api_get("/fapi/v1/premiumIndex")
    return {
        x["symbol"]: {
            "funding": to_float(x.get("lastFundingRate")),
            "mark": to_float(x.get("markPrice")),
            "index": to_float(x.get("indexPrice")),
        }
        for x in data
        if x.get("symbol")
    }


def _completed_start_rows(rows: List[dict], period_ms: int) -> List[dict]:
    """用于 timestamp=周期开始时间 的接口：排除当前未结束周期。"""
    if not rows:
        return []
    now_ms = int(time.time() * 1000)
    current_bucket_start = (now_ms // period_ms) * period_ms
    return [
        row for row in rows
        if int(row.get("timestamp", 0) or 0) > 0
        and int(row.get("timestamp", 0) or 0) < current_bucket_start
    ]


def _completed_end_rows(rows: List[dict], period_ms: int) -> List[dict]:
    """用于 timestamp=周期结束时间 的接口：只保留已结束到当前整周期边界的数据。"""
    if not rows:
        return []
    now_ms = int(time.time() * 1000)
    current_bucket_start = (now_ms // period_ms) * period_ms
    return [
        row for row in rows
        if int(row.get("timestamp", 0) or 0) > 0
        and int(row.get("timestamp", 0) or 0) <= current_bucket_start
    ]


def get_taker_15m(symbol: str) -> Optional[dict]:
    data = api_get(
        "/futures/data/takerlongshortRatio",
        {"symbol": symbol, "period": "15m", "limit": 3},
    )
    completed = _completed_start_rows(data or [], 15 * 60 * 1000)
    if not completed:
        return None
    x = completed[-1]
    buy = to_float(x.get("buyVol"))
    sell = to_float(x.get("sellVol"))
    total = buy + sell
    ratio = buy / total if total > 0 else 0.5
    return {
        "buy": buy,
        "sell": sell,
        "buy_ratio": ratio,
        "buy_sell_ratio": to_float(x.get("buySellRatio")),
        "timestamp": int(x.get("timestamp", 0)),
    }


# ========================= 指标 =========================
def rsi(values: List[float], period: int = RSI_PERIOD) -> float:
    if len(values) < period + 1:
        return 50.0
    gains = []
    losses = []
    for i in range(1, len(values)):
        diff = values[i] - values[i - 1]
        gains.append(max(diff, 0.0))
        losses.append(max(-diff, 0.0))
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - 100.0 / (1.0 + rs)


def atr(candles: List[dict], period: int = ATR_PERIOD) -> float:
    if len(candles) < period + 1:
        return 0.0
    trs = []
    prev_close = candles[0]["c"]
    for c in candles[1:]:
        tr = max(
            c["h"] - c["l"],
            abs(c["h"] - prev_close),
            abs(c["l"] - prev_close),
        )
        trs.append(tr)
        prev_close = c["c"]
    return sum(trs[-period:]) / period


def rolling_vwap(candles: List[dict], window: int) -> float:
    if not candles:
        return 0.0
    data = candles[-window:]
    pv = 0.0
    vv = 0.0
    for c in data:
        typical = (c["h"] + c["l"] + c["c"]) / 3.0
        pv += typical * c["v"]
        vv += c["v"]
    return pv / vv if vv else data[-1]["c"]


def swing_points(candles: List[dict], left: int = SWING_LEFT, right: int = SWING_RIGHT) -> Tuple[List[Tuple[int, float]], List[Tuple[int, float]]]:
    highs = []
    lows = []
    n = len(candles)
    for i in range(left, n - right):
        hi = candles[i]["h"]
        lo = candles[i]["l"]
        left_highs = [candles[j]["h"] for j in range(i - left, i)]
        right_highs = [candles[j]["h"] for j in range(i + 1, i + right + 1)]
        left_lows = [candles[j]["l"] for j in range(i - left, i)]
        right_lows = [candles[j]["l"] for j in range(i + 1, i + right + 1)]
        if hi > max(left_highs) and hi >= max(right_highs):
            highs.append((i, hi))
        if lo < min(left_lows) and lo <= min(right_lows):
            lows.append((i, lo))
    return highs, lows


def has_rsi_bearish_divergence(candles: List[dict]) -> bool:
    if len(candles) < max(20, DIVERGENCE_LOOKBACK):
        return False
    window = candles[-DIVERGENCE_LOOKBACK:]
    closes = [c["c"] for c in candles]
    # 用两个局部高点做简化确认，避免“单纯最后两根”造成噪音。
    highs, _ = swing_points(window, 2, 2)
    if len(highs) < 2:
        return False
    (i1, p1), (i2, p2) = highs[-2], highs[-1]
    full_rsi = []
    for i in range(len(closes)):
        if i < RSI_PERIOD:
            full_rsi.append(50.0)
        else:
            full_rsi.append(rsi(closes[: i + 1], RSI_PERIOD))
    # 将窗口索引映射回全序列。
    offset = len(candles) - len(window)
    r1 = full_rsi[offset + i1]
    r2 = full_rsi[offset + i2]
    price_new_high = p2 > p1 * (1.0 + DIVERGENCE_PRICE_EPS)
    rsi_not_new_high = r2 <= r1 + DIVERGENCE_RSI_EPS
    return price_new_high and rsi_not_new_high


# ========================= 4H结构 =========================
def analyze_4h(candles: List[dict]) -> dict:
    if len(candles) < 40:
        return {"ok": False, "reason": "4H数据不足"}

    highs, lows = swing_points(candles)
    if len(highs) < 2 or len(lows) < 2:
        return {"ok": False, "reason": "4H摆动点不足"}

    prev_h = highs[-2][1]
    last_h = highs[-1][1]
    prev_l = lows[-2][1]
    last_l = lows[-1][1]
    current = candles[-1]["c"]

    hh = last_h > prev_h * (1.0 + ONE_H_MIN_SWING_IMPROVEMENT)
    hl = last_l > prev_l * (1.0 + ONE_H_MIN_SWING_IMPROVEMENT)

    recent_high = max(c["h"] for c in candles[-FOUR_H_BREAK_LOOKBACK:])
    # 这里不要求突破远期最高，只要求接近最近重要结构高点。
    near_break = current >= last_h * (1.0 - FOUR_H_MAX_DISTANCE_FROM_SWING_HIGH)
    reclaim = current >= recent_high * 0.998

    closes = [c["c"] for c in candles]
    rsi4 = rsi(closes)
    rsi_ok = FOUR_H_RSI_MIN <= rsi4 <= FOUR_H_RSI_MAX

    vwap4 = rolling_vwap(candles, min(36, len(candles)))
    vwap_ok = current >= vwap4 * 0.997

    # 最近5~8根不能已经变成明显连续下跌。
    recent_bear = sum(1 for c in candles[-6:] if c["c"] < c["o"])
    recent_structure = recent_bear <= 3

    ok = hh and hl and near_break and vwap_ok and rsi_ok and recent_structure
    return {
        "ok": ok,
        "hh": hh,
        "hl": hl,
        "last_high": last_h,
        "last_low": last_l,
        "rsi": rsi4,
        "vwap": vwap4,
        "current": current,
        "near_break": near_break,
        "reclaim": reclaim,
    }


# ========================= 1H趋势 =========================
def analyze_1h(candles: List[dict]) -> dict:
    if len(candles) < 50:
        return {"ok": False, "reason": "1H数据不足"}

    highs, lows = swing_points(candles)
    if len(highs) < 3 or len(lows) < 3:
        return {"ok": False, "reason": "1H摆动点不足"}

    prev_h = highs[-2][1]
    last_h = highs[-1][1]
    prev_l = lows[-2][1]
    last_l = lows[-1][1]
    current = candles[-1]["c"]

    hh = last_h > prev_h * (1.0 + ONE_H_MIN_SWING_IMPROVEMENT)
    hl = last_l > prev_l * (1.0 + ONE_H_MIN_SWING_IMPROVEMENT)
    vwap1 = rolling_vwap(candles, ONE_H_VWAP_WINDOW)
    vwap_ok = current > vwap1

    rsi1 = rsi([c["c"] for c in candles])
    # 1H不要求RSI进入极端，仅要求不明显转弱。
    rsi_ok = 52.0 <= rsi1 <= 75.0

    # 最近8根至少有4根收阳/实体偏强，避免“高位钝化”。
    bull_count = sum(1 for c in candles[-8:] if c["c"] >= c["o"])
    momentum_ok = bull_count >= 4

    ok = hh and hl and vwap_ok and rsi_ok and momentum_ok
    return {
        "ok": ok,
        "hh": hh,
        "hl": hl,
        "last_high": last_h,
        "last_low": last_l,
        "rsi": rsi1,
        "vwap": vwap1,
        "current": current,
        "bull_count": bull_count,
    }


# ========================= 15M入场 =========================
def analyze_15m_entry(candles: List[dict], taker: Optional[dict], oi_change: float, funding: float) -> dict:
    if len(candles) < 35:
        return {"ok": False, "reason": "15M数据不足"}
    if not taker:
        return {"ok": False, "reason": "Taker数据不足"}

    recent = candles[-ENTRY_RESISTANCE_LOOKBACK - 2:-2]
    if len(recent) < 6:
        return {"ok": False, "reason": "15M阻力样本不足"}

    resistance = max(c["h"] for c in recent)
    prev_close = candles[-2]["c"]
    last = candles[-1]

    # “回踩”发生在最近4根（不含最终突破K）
    pullback_window = candles[-ENTRY_PULLBACK_BARS - 1:-1]
    pullback_low = min(c["l"] for c in pullback_window)
    touched_resistance = pullback_low <= resistance * 1.006
    held_structure = min(c["c"] for c in pullback_window) >= resistance * 0.985

    breakout = last["c"] > resistance * (1.0 + ENTRY_BREAK_BUFFER)
    body_pct = pct(last["c"], last["o"])
    full_range = max(last["h"] - last["l"], 1e-12)
    close_pos = (last["c"] - last["l"]) / full_range

    base_vols = [c["v"] for c in candles[-22:-2]]
    base_vol = sum(base_vols) / len(base_vols) if base_vols else 0.0
    volume_ratio = last["v"] / base_vol if base_vol else 0.0
    volume_ok = volume_ratio >= ENTRY_VOLUME_RATIO

    taker_ok = taker["buy_ratio"] >= ENTRY_TAKER_BUY_RATIO

    rsi15 = rsi([c["c"] for c in candles])
    rsi_ok = ENTRY_15M_RSI_MIN <= rsi15 <= ENTRY_15M_RSI_MAX

    atr_abs = atr(candles, ATR_PERIOD)
    atr_pct = atr_abs / last["c"] if last["c"] > 0 else 0.0
    extension = (last["c"] - resistance) / atr_abs if atr_abs > 0 else 999.0
    atr_ok = atr_pct <= MAX_15M_ATR_PCT and extension <= MAX_ENTRY_EXTENSION_ATR

    funding_ok = funding <= FUNDING_SOFT_MAX
    funding_hard_reject = funding >= FUNDING_HARD_MAX

    divergence = has_rsi_bearish_divergence(candles)

    ok = all([
        touched_resistance,
        held_structure,
        breakout,
        body_pct >= ENTRY_BODY_PCT,
        close_pos >= ENTRY_CLOSE_POSITION,
        volume_ok,
        taker_ok,
        oi_change >= OI_MIN_CHANGE_1H,
        rsi_ok,
        atr_ok,
        funding_ok,
        not divergence,
        not funding_hard_reject,
    ])

    return {
        "ok": ok,
        "resistance": resistance,
        "pullback_low": pullback_low,
        "touched_resistance": touched_resistance,
        "held_structure": held_structure,
        "breakout": breakout,
        "body_pct": body_pct,
        "close_pos": close_pos,
        "volume_ratio": volume_ratio,
        "taker_buy_ratio": taker["buy_ratio"],
        "rsi": rsi15,
        "atr_abs": atr_abs,
        "atr_pct": atr_pct,
        "extension_atr": extension,
        "funding_ok": funding_ok,
        "funding_hard_reject": funding_hard_reject,
        "divergence": divergence,
        "current": last["c"],
        "high": last["h"],
        "low": last["l"],
    }


# ========================= OI =========================
def normalize_oi_history(rows: List[dict]) -> Tuple[float, float, float]:
    """返回最近两个已完成1H周期的 OI value 与变化。"""
    completed = _completed_end_rows(rows or [], 60 * 60 * 1000)
    if len(completed) < 2:
        return 0.0, 0.0, 0.0

    def value(row):
        v = to_float(row.get("sumOpenInterestValue"), 0.0)
        if v > 0:
            return v
        return to_float(row.get("sumOpenInterest"), 0.0)

    prev = value(completed[-2])
    cur = value(completed[-1])
    change = pct(cur, prev) if prev > 0 else 0.0
    return cur, prev, change


def save_oi_snapshots_batch(items: List[Tuple[str, float]]):
    """每轮批量写入，避免高并发逐币抢 SQLite 写锁。"""
    if not items:
        return
    ts = now_bj().replace(second=0, microsecond=0).isoformat()
    rows = [(symbol, ts, oi_value) for symbol, oi_value in items if oi_value > 0]
    if not rows:
        return
    with db_lock, db:
        db.executemany(
            "INSERT OR REPLACE INTO oi_snapshots(symbol, ts, oi_value) VALUES (?,?,?)",
            rows,
        )


def get_state(symbol: str) -> dict:
    with db_lock:
        row = db.execute(
            "SELECT state,last_setup_at,last_entry_at,last_invalidated_at,last_entry_id,last_entry_price,last_1h_hl,last_4h_hl,updated_at FROM symbol_state WHERE symbol=?",
            (symbol,),
        ).fetchone()
    if not row:
        return {"state": "IDLE"}
    keys = [
        "state", "last_setup_at", "last_entry_at", "last_invalidated_at",
        "last_entry_id", "last_entry_price", "last_1h_hl", "last_4h_hl", "updated_at",
    ]
    return dict(zip(keys, row))


def upsert_state(symbol: str, **kwargs):
    state = get_state(symbol)
    state.update(kwargs)
    state.setdefault("state", "IDLE")
    state["updated_at"] = iso_now()
    with db_lock, db:
        db.execute(
            """
            INSERT INTO symbol_state
                (symbol,state,last_setup_at,last_entry_at,last_invalidated_at,
                 last_entry_id,last_entry_price,last_1h_hl,last_4h_hl,updated_at)
            VALUES (?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(symbol) DO UPDATE SET
                state=excluded.state,
                last_setup_at=excluded.last_setup_at,
                last_entry_at=excluded.last_entry_at,
                last_invalidated_at=excluded.last_invalidated_at,
                last_entry_id=excluded.last_entry_id,
                last_entry_price=excluded.last_entry_price,
                last_1h_hl=excluded.last_1h_hl,
                last_4h_hl=excluded.last_4h_hl,
                updated_at=excluded.updated_at
            """,
            (
                symbol,
                state.get("state"),
                state.get("last_setup_at"),
                state.get("last_entry_at"),
                state.get("last_invalidated_at"),
                state.get("last_entry_id"),
                state.get("last_entry_price"),
                state.get("last_1h_hl"),
                state.get("last_4h_hl"),
                state.get("updated_at"),
            ),
        )


def hours_since(ts: Optional[str]) -> float:
    if not ts:
        return 999999.0
    try:
        dt = datetime.fromisoformat(ts)
        return (now_bj() - dt).total_seconds() / 3600.0
    except Exception:
        return 999999.0


def can_setup(symbol: str) -> bool:
    st = get_state(symbol)
    if st.get("state") == "ACTIVE":
        return False
    return hours_since(st.get("last_setup_at")) >= SETUP_COOLDOWN_HOURS


def can_entry(symbol: str) -> bool:
    st = get_state(symbol)
    if st.get("state") == "ACTIVE":
        return False
    return hours_since(st.get("last_entry_at")) >= ENTRY_COOLDOWN_HOURS


# ========================= 评分 =========================
def score_entry(f4: dict, f1: dict, f15: dict, oi_change: float, oi_value: float, funding: float) -> Tuple[int, str, List[str]]:
    score = 0
    reasons = []

    # 4H结构 25分
    if f4.get("hh") and f4.get("hl"):
        score += 25
        reasons.append("4H_HH_HL")
    elif f4.get("hl"):
        score += 14
        reasons.append("4H_HL")

    # 1H趋势 20分
    if f1.get("hh") and f1.get("hl") and f1.get("current", 0) > f1.get("vwap", float("inf")):
        score += 20
        reasons.append("1H趋势+VWAP")
    elif f1.get("hh") and f1.get("hl"):
        score += 14
        reasons.append("1H_HH_HL")

    # 15M突破/回踩 20分
    if f15.get("breakout") and f15.get("touched_resistance") and f15.get("held_structure"):
        score += 20
        reasons.append("15M突破回踩再启动")

    # 量能 10分
    vr = f15.get("volume_ratio", 0.0)
    if vr >= 2.5:
        score += 10
        reasons.append("量能>2.5x")
    elif vr >= ENTRY_VOLUME_RATIO:
        score += 7
        reasons.append("量能>1.5x")

    # Taker 10分
    tr = f15.get("taker_buy_ratio", 0.5)
    if tr >= 0.65:
        score += 10
        reasons.append("TakerBuy>=65%")
    elif tr >= ENTRY_TAKER_BUY_RATIO:
        score += 7
        reasons.append("TakerBuy>=58%")

    # OI 10分
    if oi_change >= OI_STRONG_CHANGE_1H:
        score += 10
        reasons.append("OI_1H>=5%")
    elif oi_change >= OI_MIN_CHANGE_1H:
        score += 7
        reasons.append("OI_1H>=2%")

    # Funding / RSI / ATR风险修正合计 15分
    risk_points = 15
    if funding > FUNDING_SOFT_MAX:
        risk_points -= 5
    if f15.get("rsi", 50) > 68:
        risk_points -= 3
    if f4.get("rsi", 50) > 68:
        risk_points -= 3
    if f15.get("extension_atr", 0) > 1.0:
        risk_points -= 4
    score += max(0, risk_points)

    if score >= A_PLUS_SCORE_MIN:
        quality = "A+"
    elif score >= ENTRY_SCORE_MIN:
        quality = "A"
    else:
        quality = "B"

    return min(100, score), quality, reasons


# ========================= 信号记录 =========================
def record_signal(payload: dict) -> int:
    with db_lock, db:
        cur = db.execute(
            """
            INSERT INTO signals (
                symbol, signal_type, signal_time, price, score, quality,
                volume_24h, oi_value, oi_change_1h, volume_ratio,
                taker_buy_ratio, funding, rsi_4h, rsi_15m,
                atr_15m_pct, entry_resistance, entry_extension_atr,
                reason, active
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1)
            """,
            (
                payload["symbol"], payload["signal_type"], payload.get("signal_time", iso_now()),
                payload.get("price"), payload.get("score"), payload.get("quality"),
                payload.get("volume_24h"), payload.get("oi_value"), payload.get("oi_change_1h"),
                payload.get("volume_ratio"), payload.get("taker_buy_ratio"), payload.get("funding"),
                payload.get("rsi_4h"), payload.get("rsi_15m"), payload.get("atr_15m_pct"),
                payload.get("entry_resistance"), payload.get("entry_extension_atr"),
                payload.get("reason", ""),
            ),
        )
        return cur.lastrowid


def mark_invalidation(entry_id: int, reason: str):
    with db_lock, db:
        db.execute(
            "UPDATE signals SET active=0, invalidated_at=?, invalidation_reason=? WHERE id=?",
            (iso_now(), reason, entry_id),
        )


# ========================= 通知 =========================
def send_tg(text: str) -> bool:
    if not ENABLE_TELEGRAM or not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return False
    try:
        r = session.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": text},
            timeout=REQUEST_TIMEOUT,
        )
        if r.status_code >= 400:
            logger.error("TG发送失败: %s", r.text[:300])
            return False
        return True
    except Exception as e:
        logger.error("TG发送异常: %s", e)
        return False


def send_email(subject: str, body: str) -> bool:
    if not ENABLE_EMAIL or not QQ_EMAIL or not QQ_AUTH_CODE or not EMAIL_TO:
        return False
    server = None
    try:
        msg = MIMEText(body, "plain", "utf-8")
        msg["From"] = formataddr(("LONG ONLY 雷达", QQ_EMAIL))
        msg["To"] = EMAIL_TO
        msg["Subject"] = subject
        server = smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, timeout=REQUEST_TIMEOUT)
        server.login(QQ_EMAIL, QQ_AUTH_CODE)
        recipients = [x.strip() for x in EMAIL_TO.split(",") if x.strip()]
        server.sendmail(QQ_EMAIL, recipients, msg.as_string())
        return True
    except Exception as e:
        logger.error("邮件发送异常: %s", e)
        return False
    finally:
        if server:
            try:
                server.quit()
            except Exception:
                pass


def notify(text: str, subject: str, email: bool = True):
    tg_ok = send_tg(text)
    email_ok = send_email(subject, text) if email else False
    return tg_ok, email_ok


def format_entry(payload: dict) -> str:
    return (
        f"🟢 LONG ENTRY · {payload['symbol']}\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"交易级别：4H LONG\n"
        f"当前价格：{payload['price']:.8g}\n\n"
        f"【周期】\n"
        f"4H：结构转强 ✅\n"
        f"1H：趋势确认 ✅\n"
        f"15M：突破→回踩→再启动 ✅\n\n"
        f"【合约资金】\n"
        f"24H成交额：{payload['volume_24h']/1e6:.2f}M USDT\n"
        f"OI：{payload['oi_value']/1e6:.2f}M USDT\n"
        f"OI 1H：{payload['oi_change_1h']*100:+.2f}%\n"
        f"Taker Buy：{payload['taker_buy_ratio']*100:.1f}%\n"
        f"Funding：{payload['funding']*100:.4f}%\n\n"
        f"【动能/风险】\n"
        f"4H RSI：{payload['rsi_4h']:.1f}\n"
        f"15M RSI：{payload['rsi_15m']:.1f}\n"
        f"15M量能：{payload['volume_ratio']:.2f}x\n"
        f"15M ATR：{payload['atr_15m_pct']*100:.2f}%\n"
        f"突破位：{payload['entry_resistance']:.8g}\n"
        f"当前距突破位：{payload['entry_extension_atr']:.2f} ATR\n\n"
        f"综合评分：{payload['score']}/100 · {payload['quality']}\n"
        f"结论：右侧多头入场窗口成立。\n\n"
        f"⚠️ 这不是“看到通知就追涨”。\n"
        f"重点观察4H/1H趋势是否继续成立；若跌破关键1H结构或4H结构，信号失效。\n"
        f"时间：{now_bj().strftime('%Y-%m-%d %H:%M:%S')}"
    )


def format_invalidation(symbol: str, price: float, reason: str) -> str:
    return (
        f"🔴 LONG INVALIDATED · {symbol}\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"交易级别：4H LONG\n"
        f"当前价格：{price:.8g}\n"
        f"失效原因：{reason}\n\n"
        f"含义：原多头交易逻辑不再成立。\n"
        f"不要因为15M短暂反弹而重新把旧信号当成有效。\n"
        f"时间：{now_bj().strftime('%Y-%m-%d %H:%M:%S')}"
    )


# ========================= 交易状态检查 =========================
def get_active_entries() -> Dict[str, dict]:
    with db_lock:
        rows = db.execute(
            """
            SELECT s.id,s.symbol,s.price,s.entry_resistance,s.signal_time,s.score,
                   st.last_1h_hl,st.last_4h_hl
            FROM signals s
            LEFT JOIN symbol_state st ON st.symbol=s.symbol
            WHERE s.signal_type='ENTRY' AND s.active=1
            """
        ).fetchall()
    return {
        r[1]: {
            "id": r[0], "symbol": r[1], "price": r[2], "entry_resistance": r[3],
            "signal_time": r[4], "score": r[5],
            "last_1h_hl": r[6], "last_4h_hl": r[7],
        }
        for r in rows
    }


def check_active_invalidations(active_map: Dict[str, dict]):
    """
    ENTRY 后不重新跑 RSI/VWAP/动能条件。
    失效只依据入场时锁定的 1H / 4H Higher Low，避免正常波动误杀。
    """
    if not active_map:
        return

    for symbol, entry in active_map.items():
        try:
            k1 = candle_dicts(get_klines(symbol, "1h", 4))
            k4 = candle_dicts(get_klines(symbol, "4h", 4))
            if not k1 or not k4:
                continue

            close_1h = k1[-1]["c"]
            close_4h = k4[-1]["c"]
            hl_1h = to_float(entry.get("last_1h_hl"), 0.0)
            hl_4h = to_float(entry.get("last_4h_hl"), 0.0)

            reason = None
            invalid_price = close_1h

            if INVALIDATE_ON_1H_HL_BREAK and hl_1h > 0:
                threshold_1h = hl_1h * 0.998
                if close_1h < threshold_1h:
                    reason = (
                        f"1H已收盘跌破入场Higher Low："
                        f"close={close_1h:.8g} < {threshold_1h:.8g}"
                    )
                    invalid_price = close_1h

            if reason is None and INVALIDATE_4H_STRUCTURE_BREAK and hl_4h > 0:
                threshold_4h = hl_4h * 0.998
                if close_4h < threshold_4h:
                    reason = (
                        f"4H已收盘跌破入场Higher Low："
                        f"close={close_4h:.8g} < {threshold_4h:.8g}"
                    )
                    invalid_price = close_4h

            if reason:
                mark_invalidation(entry["id"], reason)
                upsert_state(
                    symbol,
                    state="INVALIDATED",
                    last_invalidated_at=iso_now(),
                )
                if NOTIFY_INVALIDATED:
                    notify(
                        format_invalidation(symbol, invalid_price, reason),
                        f"🔴 [LONG失效] {symbol}",
                        email=True,
                    )
                logger.info("🔴 %s LONG失效: %s", symbol, reason)
        except Exception as e:
            logger.warning("%s 失效检查失败: %s", symbol, e)


# ========================= 单币完整分析 =========================
def analyze_symbol(symbol: str, ticker: dict, premium: dict) -> Optional[dict]:
    volume24 = ticker.get("volume", 0.0)
    if volume24 < MIN_24H_VOLUME_USDT:
        return None

    try:
        # 分层拉取：先4H，过关再取1H；只有4H+1H都成立才继续请求资金/15M数据。
        k4 = candle_dicts(get_klines(symbol, "4h", TF_4H_LIMIT))
        if len(k4) < 40:
            return None
        f4 = analyze_4h(k4)
        if not f4.get("ok"):
            return None

        k1 = candle_dicts(get_klines(symbol, "1h", TF_1H_LIMIT))
        if len(k1) < 50:
            return None
        f1 = analyze_1h(k1)
        if not f1.get("ok"):
            return None

        oi_rows = get_oi_history(symbol, "1h", 3)
        oi_value, _, oi_change = normalize_oi_history(oi_rows)
        if oi_value < MIN_OI_VALUE_USDT:
            return None

        k15 = candle_dicts(get_klines(symbol, "15m", TF_15M_LIMIT))
        if len(k15) < 35:
            return None

        funding = premium.get("funding", 0.0)
        taker = get_taker_15m(symbol)
        f15 = analyze_15m_entry(k15, taker, oi_change, funding)

        # 只允许正式右侧入场，不发送“看起来不错”的普通评分。
        if not f15.get("ok"):
            return {
                "setup": True,
                "entry": False,
                "symbol": symbol,
                "f4": f4,
                "f1": f1,
                "f15": f15,
                "oi_value": oi_value,
                "oi_snapshot": (symbol, oi_value),
                "oi_change": oi_change,
                "funding": funding,
                "volume24": volume24,
                "premium": premium,
            }

        score, quality, reasons = score_entry(f4, f1, f15, oi_change, oi_value, funding)
        hard_ok = score >= ENTRY_SCORE_MIN

        if not hard_ok:
            return {
                "setup": True,
                "entry": False,
                "symbol": symbol,
                "f4": f4,
                "f1": f1,
                "f15": f15,
                "oi_value": oi_value,
                "oi_snapshot": (symbol, oi_value),
                "oi_change": oi_change,
                "funding": funding,
                "volume24": volume24,
                "premium": premium,
                "score": score,
                "quality": quality,
                "reasons": reasons,
            }

        payload = {
            "setup": True,
            "entry": True,
            "symbol": symbol,
            "price": f15["current"],
            "volume_24h": volume24,
            "oi_value": oi_value,
            "oi_snapshot": (symbol, oi_value),
            "oi_change_1h": oi_change,
            "volume_ratio": f15["volume_ratio"],
            "taker_buy_ratio": f15["taker_buy_ratio"],
            "funding": funding,
            "rsi_4h": f4["rsi"],
            "rsi_15m": f15["rsi"],
            "atr_15m_pct": f15["atr_pct"],
            "entry_resistance": f15["resistance"],
            "entry_extension_atr": f15["extension_atr"],
            "score": score,
            "quality": quality,
            "reasons": reasons,
            "f4": f4,
            "f1": f1,
            "f15": f15,
        }
        return payload
    except Exception as e:
        logger.warning("%s 分析失败: %s", symbol, e)
        return None


# ========================= 日报 =========================
def report_already_sent(report_date: str) -> bool:
    with db_lock:
        row = db.execute(
            "SELECT 1 FROM daily_reports WHERE report_date=?",
            (report_date,),
        ).fetchone()
    return row is not None


def generate_daily_report(report_date: Optional[str] = None) -> Tuple[str, str]:
    if report_date is None:
        report_date = (now_bj().date() - timedelta(days=1)).isoformat()

    start = datetime.fromisoformat(report_date).replace(tzinfo=TZ_BJ)
    end = start + timedelta(days=1)

    with db_lock:
        rows = db.execute(
            """
            SELECT id,symbol,signal_type,signal_time,price,score,quality,
                   volume_24h,oi_value,oi_change_1h,volume_ratio,taker_buy_ratio,
                   funding,rsi_4h,rsi_15m,atr_15m_pct,entry_resistance,
                   entry_extension_atr,reason,active,invalidated_at,invalidation_reason
            FROM signals
            WHERE signal_time>=? AND signal_time<?
            ORDER BY signal_time ASC
            """,
            (start.isoformat(), end.isoformat()),
        ).fetchall()

    entry_rows = [r for r in rows if r[2] == "ENTRY"]
    active_count = sum(1 for r in entry_rows if r[19] == 1)
    invalidated_count = sum(1 for r in entry_rows if r[19] == 0)
    scores = [to_float(r[5]) for r in entry_rows if r[5] is not None]
    best = max(entry_rows, key=lambda x: to_float(x[5]), default=None)

    os.makedirs(DAILY_REPORT_DIR, exist_ok=True)
    filepath = os.path.join(DAILY_REPORT_DIR, f"LONG日报_{report_date.replace('-', '')}.csv")

    headers = [
        "ID", "Symbol", "Type", "Time", "Price", "Score", "Quality",
        "24HVolume", "OI", "OI1HChange", "VolumeRatio", "TakerBuyRatio",
        "Funding", "RSI4H", "RSI15M", "ATR15M", "Resistance", "ExtensionATR",
        "Reason", "Active", "InvalidatedAt", "InvalidationReason"
    ]
    with open(filepath, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(rows)

    best_text = "无"
    if best:
        best_text = f"{best[1]} · {best[5]}/100 · {best[6]}"

    summary = (
        f"📊 LONG ONLY 日报 · {report_date}\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"正式ENTRY：{len(entry_rows)}\n"
        f"当前仍有效：{active_count}\n"
        f"已失效：{invalidated_count}\n"
        f"平均评分：{(sum(scores)/len(scores)) if scores else 0:.1f}\n"
        f"最高评分：{best_text}\n\n"
        f"交易框架：4H级别 / 1H趋势 / 15M入场\n"
        f"硬门槛：24H成交额≥10M · OI≥10M USDT\n"
        f"数据文件：{filepath}\n"
        f"生成时间：{now_bj().strftime('%Y-%m-%d %H:%M:%S')}"
    )

    return summary, filepath


def maybe_send_daily_report():
    now = now_bj()
    # 只在北京时间 00:05 <= time < 00:15 尝试，其他时间直接返回。
    if now.hour != 0 or not (5 <= now.minute < 15):
        return
    report_date = (now.date() - timedelta(days=1)).isoformat()
    if report_already_sent(report_date):
        return

    try:
        summary, filepath = generate_daily_report(report_date)
        tg_ok = send_tg(summary) if NOTIFY_DAILY_REPORT else False
        email_ok = send_email(f"📊 [LONG ONLY日报] {report_date}", summary) if NOTIFY_DAILY_REPORT else False
        with db_lock, db:
            db.execute(
                "INSERT OR REPLACE INTO daily_reports(report_date,generated_at,filepath,sent_tg,sent_email) VALUES (?,?,?,?,?)",
                (report_date, iso_now(), filepath, 1 if tg_ok else 0, 1 if email_ok else 0),
            )
        logger.info("📊 日报完成: %s", report_date)
    except Exception as e:
        logger.error("日报异常: %s", e)


def cleanup_db():
    cutoff = (now_bj() - timedelta(days=KEEP_DAYS)).isoformat()
    with db_lock, db:
        db.execute("DELETE FROM oi_snapshots WHERE ts<?", (cutoff,))
        db.execute("DELETE FROM signals WHERE signal_time<?", (cutoff,))
        db.execute("DELETE FROM symbol_state WHERE updated_at<?", (cutoff,))
    logger.info("🧹 已清理 %d 天前数据库历史", KEEP_DAYS)


# ========================= 主循环 =========================
def startup_notice() -> str:
    return (
        "✅ LONG ONLY 多周期雷达启动\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "交易级别：4H\n"
        "趋势确认：1H\n"
        "右侧入场：15M\n"
        "方向：LONG ONLY\n"
        "24H成交额门槛：≥10M USDT\n"
        "OI门槛：≥10M USDT\n"
        "正式通知：LONG ENTRY / LONG INVALIDATED / 日报\n"
        "持仓失效：只看入场时锁定的1H/4H Higher Low，不重跑RSI/VWAP\n"
        "原则：只用已收盘K线确认，不抄底，不追极端拉升。"
    )


def main():
    logger.info("🚀 LONG ONLY 雷达启动")
    notify(startup_notice(), "LONG ONLY 雷达启动", email=False)

    last_refresh = 0.0
    last_cleanup = 0.0
    symbols: List[str] = []

    while True:
        cycle_start = time.time()
        try:
            if time.time() - last_refresh >= SYMBOL_REFRESH_SEC or not symbols:
                symbols = get_exchange_symbols()
                last_refresh = time.time()
                logger.info("合约列表：%d 个 USDT 永续", len(symbols))

            tickers = get_24h_tickers()
            candidates = [
                s for s in symbols
                if tickers.get(s, {}).get("volume", 0.0) >= MIN_24H_VOLUME_USDT
            ]

            # 从成交额排序；不是为了择优，而是让高流动性先完成。
            candidates.sort(
                key=lambda s: tickers.get(s, {}).get("volume", 0.0),
                reverse=True,
            )

            premium = get_premium_index_all()
            logger.info("候选池：%d（24H成交额≥10M）", len(candidates))

            # 先处理旧ENTRY的失效；持仓层级只看4H/1H。
            active = get_active_entries()
            check_active_invalidations(active)

            entries = []
            setups = 0
            oi_snapshots = []
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
                futures = {
                    ex.submit(analyze_symbol, s, tickers[s], premium.get(s, {})): s
                    for s in candidates
                }
                for fut in as_completed(futures):
                    symbol = futures[fut]
                    try:
                        result = fut.result()
                    except Exception as e:
                        logger.warning("%s future异常: %s", symbol, e)
                        continue
                    if not result:
                        continue
                    if result.get("oi_snapshot"):
                        oi_snapshots.append(result["oi_snapshot"])
                    setups += 1
                    if result.get("entry") and can_entry(symbol):
                        entries.append(result)
                        upsert_state(
                            symbol,
                            state="ACTIVE",
                            last_entry_at=iso_now(),
                            last_entry_price=result["price"],
                            last_1h_hl=result["f1"].get("last_low"),
                            last_4h_hl=result["f4"].get("last_low"),
                        )

            # OI只对通过4H+1H并进入深度分析的币批量落库。
            save_oi_snapshots_batch(oi_snapshots)

            # 一轮可能出现多个币同时满足；按评分从高到低通知。
            entries.sort(key=lambda x: x.get("score", 0), reverse=True)
            for payload in entries:
                entry_id = record_signal({
                    **payload,
                    "signal_type": "ENTRY",
                    "signal_time": iso_now(),
                    "reason": ",".join(payload.get("reasons", [])),
                })
                upsert_state(
                    payload["symbol"],
                    state="ACTIVE",
                    last_entry_id=entry_id,
                    last_entry_at=iso_now(),
                    last_entry_price=payload["price"],
                )
                text = format_entry(payload)
                if NOTIFY_ENTRY:
                    notify(text, f"🟢 [LONG ENTRY] {payload['symbol']} · {payload['quality']}", email=True)
                logger.info(
                    "🟢 ENTRY %s score=%d quality=%s price=%s",
                    payload["symbol"], payload["score"], payload["quality"], payload["price"],
                )

            if time.time() - last_cleanup >= 6 * 3600:
                cleanup_db()
                last_cleanup = time.time()

            maybe_send_daily_report()

            elapsed = time.time() - cycle_start
            logger.info("本轮完成：候选=%d，满足4H+1H的setup=%d，ENTRY=%d，耗时=%.1fs", len(candidates), setups, len(entries), elapsed)

            sleep_for = max(2.0, SCAN_INTERVAL_SEC - elapsed)
            time.sleep(sleep_for)

        except KeyboardInterrupt:
            logger.info("🛑 手动退出")
            break
        except Exception as e:
            logger.exception("主循环异常: %s", e)
            time.sleep(10)


if __name__ == "__main__":
    main()
