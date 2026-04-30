#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
币安永续合约多因子信号监控 – 精简版（无黑名单、无结算）
- 相对分值阈值
- 缓存市值数据
- 合理的跨周期确认
- 保留单币种频率限制（每4小时同方向最多1次）
"""

import sqlite3
import time
import logging
import requests
import smtplib
import statistics
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.header import Header
from email.utils import formataddr
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

# ======================== 配置 ========================
TELEGRAM_TOKEN = "8557301222:AAHj1rSQ63zJGFXVxxuTniwRP2Y1tj3QsAs"
TELEGRAM_CHAT_ID = "5408890841"

ENABLE_EMAIL = True
QQ_EMAIL = "1113496210@qq.com"
QQ_AUTH_CODE = "hzshvazrbnyzfhdf"
EMAIL_TO = "1113496210@qq.com"
SMTP_HOST = "smtp.qq.com"
SMTP_PORT = 465

SCAN_INTERVAL = 180
SYMBOL_REFRESH_INTERVAL = 3600
OI_CHANGE_THRESHOLD = 0.10
DEDUP_HOURS = 1

# 多因子参数
OI_STRONG = 0.15
VOLUME_MULTIPLIER_LARGE = 1.5
VOLUME_MULTIPLIER_SMALL = 3.0
PRICE_CHANGE_MIN = 0.02
FUNDING_RATE_MAX = 0.001
MIN_FACTORS_RATIO = 0.6          # 相对阈值：总分达到最高分的60%才发信号
CROSS_PERIOD_CONFIRM = True

# 权重配置
WEIGHT_OI = 1.5
WEIGHT_VOLUME = 1.5
WEIGHT_BREAKOUT = 1.5
WEIGHT_CANDLE = 0.8
WEIGHT_FUNDING = 0.8

# 其他过滤
MIN_24H_VOLUME_USDT = 10_000_000
MAX_WORKERS_QUICK = 20
MAX_WORKERS_DETAIL = 10
REQUEST_TIMEOUT = 10

# 频率限制：同一币种同方向最小间隔（小时）
MAX_SIGNAL_PER_SYMBOL_HOURS = 4
LOG_FILE = "monitor.log"
# =========================================================

BINANCE_FAPI_BASE = "https://fapi.binance.com"

# ---------- 全局缓存 ----------
SYMBOL_VOLUME_CACHE = {}   # symbol -> 24h quoteVolume

# ---------- Session ----------
session = requests.Session()
retry = Retry(total=2, backoff_factor=0.5, status_forcelist=[500, 502, 503, 504])
adapter = HTTPAdapter(pool_connections=30, pool_maxsize=30, max_retries=retry)
session.mount("https://", adapter)
session.mount("http://", adapter)

_original_request = session.request
def _patched_request(method, url, **kwargs):
    if 'timeout' not in kwargs:
        kwargs['timeout'] = REQUEST_TIMEOUT
    return _original_request(method, url, **kwargs)
session.request = _patched_request

# ---------- 日志 ----------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(LOG_FILE, encoding="utf-8"), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# ---------- 数据库初始化（无黑名单相关表）----------
sqlite3.register_adapter(datetime, lambda dt: dt.isoformat())

def init_db():
    conn = sqlite3.connect("signals.db")
    # 去重表（1小时内重复过滤）
    conn.execute("""CREATE TABLE IF NOT EXISTS sent_log (
        symbol TEXT, direction TEXT, sent_at TIMESTAMP, UNIQUE(symbol, direction, sent_at)
    )""")
    # 历史信号表（仅记录，不做回测）
    conn.execute("""CREATE TABLE IF NOT EXISTS signals_history (
        symbol TEXT, direction TEXT, score REAL, price REAL,
        oi_change REAL, price_change REAL, volume_ratio REAL,
        funding REAL, sent_at TIMESTAMP
    )""")
    # 单币种频率限制表
    conn.execute("""CREATE TABLE IF NOT EXISTS symbol_last_alert (
        symbol TEXT, direction TEXT, last_at TIMESTAMP, UNIQUE(symbol, direction)
    )""")
    conn.commit()
    return conn

# ---------- 频率限制 ----------
def can_alert_frequency(symbol, direction):
    conn = init_db()
    cutoff = datetime.now() - timedelta(hours=MAX_SIGNAL_PER_SYMBOL_HOURS)
    cur = conn.execute("SELECT 1 FROM symbol_last_alert WHERE symbol=? AND direction=? AND last_at > ?",
                       (symbol, direction, cutoff))
    if cur.fetchone():
        conn.close()
        return False
    conn.execute("INSERT OR REPLACE INTO symbol_last_alert VALUES (?, ?, ?)",
                 (symbol, direction, datetime.now()))
    conn.commit()
    conn.close()
    return True

# ---------- 币安 API ----------
def get_all_symbols():
    global SYMBOL_VOLUME_CACHE
    try:
        r = session.get(f"{BINANCE_FAPI_BASE}/fapi/v1/exchangeInfo")
        r.raise_for_status()
        info = r.json()
        all_symbols = [s['symbol'] for s in info['symbols']
                       if s['contractType'] == 'PERPETUAL' and s['quoteAsset'] == 'USDT']
        tickers = session.get(f"{BINANCE_FAPI_BASE}/fapi/v1/ticker/24hr").json()
        high_volume = set()
        for t in tickers:
            try:
                vol = float(t.get('quoteVolume', 0))
                if vol > MIN_24H_VOLUME_USDT:
                    sym = t['symbol']
                    high_volume.add(sym)
                    SYMBOL_VOLUME_CACHE[sym] = vol   # 缓存
            except:
                continue
        active = [s for s in all_symbols if s in high_volume]
        logger.info(f"总: {len(all_symbols)} → 活跃: {len(active)}")
        return active
    except Exception as e:
        logger.error(f"获取交易对失败: {e}")
        return []

def get_oi_history(symbol, period="1h", limit=2):
    url = f"{BINANCE_FAPI_BASE}/futures/data/openInterestHist"
    r = session.get(url, params={"symbol": symbol, "period": period, "limit": limit})
    r.raise_for_status()
    return r.json()

def get_klines(symbol, interval="15m", limit=20):
    url = f"{BINANCE_FAPI_BASE}/fapi/v1/klines"
    r = session.get(url, params={"symbol": symbol, "interval": interval, "limit": limit})
    r.raise_for_status()
    return r.json()

def get_funding_rate(symbol):
    url = f"{BINANCE_FAPI_BASE}/fapi/v1/premiumIndex"
    r = session.get(url, params={"symbol": symbol})
    r.raise_for_status()
    data = r.json()
    return float(data['lastFundingRate']), data.get('nextFundingTime', 0)

def get_long_short_ratio(symbol, period="1h"):
    url = f"{BINANCE_FAPI_BASE}/futures/data/topLongShortPositionRatio"
    r = session.get(url, params={"symbol": symbol, "period": period, "limit": 2})
    r.raise_for_status()
    return r.json()

# ---------- 多因子分析 ----------
def analyze_symbol(symbol):
    try:
        # 1. OI
        oi_hist = get_oi_history(symbol, "1h", 2)
        if len(oi_hist) < 2:
            return None
        oi_prev = float(oi_hist[0]['sumOpenInterest'])
        oi_now = float(oi_hist[-1]['sumOpenInterest'])
        oi_change = (oi_now - oi_prev) / oi_prev

        # 2. K线
        klines = get_klines(symbol, "15m", 20)
        closes = [float(k[4]) for k in klines]
        volumes = [float(k[5]) for k in klines]
        if len(closes) < 5:
            return None
        price_change_1h = (closes[-1] - closes[-5]) / closes[-5]

        # 4h K线
        klines_4h = get_klines(symbol, "1h", 4)
        closes_4h = [float(k[4]) for k in klines_4h]
        if len(closes_4h) >= 2:
            price_change_4h = (closes_4h[-1] - closes_4h[0]) / closes_4h[0]
        else:
            price_change_4h = price_change_1h

        # 3. 成交量（使用缓存市值）
        if len(volumes) < 5:
            return None
        avg_volume = statistics.mean(volumes[:-2])
        recent_volume = statistics.mean(volumes[-2:])
        volume_ratio = recent_volume / avg_volume if avg_volume > 0 else 0
        quote_vol = SYMBOL_VOLUME_CACHE.get(symbol, 0)
        is_large = quote_vol >= 100_000_000
        vol_threshold = VOLUME_MULTIPLIER_LARGE if is_large else VOLUME_MULTIPLIER_SMALL
        volume_valid = volume_ratio > vol_threshold

        # 4. K线形态
        bullish_candles = sum(1 for k in klines[-3:] if float(k[4]) > float(k[1]))
        candle_score = bullish_candles / 3.0

        # 5. 资金费率
        funding_now, _ = get_funding_rate(symbol)
        funding_bull = 0 < funding_now < FUNDING_RATE_MAX
        funding_bear = funding_now < 0 or funding_now > 0.001

        # 6. 价格突破
        recent_high = max(closes[-20:-1])
        recent_low = min(closes[-20:-1])
        breakout_up = closes[-1] > recent_high
        breakout_down = closes[-1] < recent_low

        # 7. 多空比
        try:
            ls_data = get_long_short_ratio(symbol)
            if len(ls_data) >= 2:
                ls_ratio = float(ls_data[-1]['longShortRatio'])
                ls_change = (ls_ratio - float(ls_data[0]['longShortRatio'])) / float(ls_data[0]['longShortRatio'])
            else:
                ls_ratio, ls_change = 1.0, 0.0
        except:
            ls_ratio, ls_change = 1.0, 0.0

        # ========== 计算总分 ==========
        max_possible = WEIGHT_OI + WEIGHT_VOLUME + WEIGHT_BREAKOUT + WEIGHT_CANDLE + WEIGHT_FUNDING + 0.5
        min_threshold = max_possible * MIN_FACTORS_RATIO

        bull_score = 0.0
        bear_score = 0.0

        # OI
        if oi_change > OI_STRONG:
            if price_change_1h > 0:
                bull_score += WEIGHT_OI
            else:
                bear_score += WEIGHT_OI

        # 成交量
        if volume_valid:
            if price_change_1h > PRICE_CHANGE_MIN:
                bull_score += WEIGHT_VOLUME
            elif price_change_1h < -PRICE_CHANGE_MIN:
                bear_score += WEIGHT_VOLUME

        # K线形态
        if candle_score >= 0.67:
            bull_score += WEIGHT_CANDLE
        elif candle_score <= 0.33:
            bear_score += WEIGHT_CANDLE

        # 资金费率
        if funding_bull:
            bull_score += WEIGHT_FUNDING
        if funding_bear:
            bear_score += WEIGHT_FUNDING

        # 突破
        if breakout_up:
            bull_score += WEIGHT_BREAKOUT
        if breakout_down:
            bear_score += WEIGHT_BREAKOUT

        # 多空比
        if ls_ratio > 1.5 and ls_change > 0.1:
            bear_score += 0.5
        elif ls_ratio < 0.7 and ls_change < -0.1:
            bull_score += 0.5

        # ========== 跨周期调整（只削弱相反方向） ==========
        if CROSS_PERIOD_CONFIRM:
            if price_change_4h > 0:   # 4h上涨趋势
                bear_score *= 0.5     # 空头信号减半
            elif price_change_4h < 0: # 4h下跌趋势
                bull_score *= 0.5     # 多头信号减半

        # 最终判断
        if bull_score >= min_threshold and bull_score > bear_score:
            return {
                "symbol": symbol, "direction": "LONG", "score": round(bull_score, 1),
                "oi_change": oi_change, "price_change": price_change_1h,
                "volume_ratio": volume_ratio, "funding": funding_now,
                "price": closes[-1]
            }
        if bear_score >= min_threshold and bear_score > bull_score:
            return {
                "symbol": symbol, "direction": "SHORT", "score": round(bear_score, 1),
                "oi_change": oi_change, "price_change": price_change_1h,
                "volume_ratio": volume_ratio, "funding": funding_now,
                "price": closes[-1]
            }
        return None

    except Exception as e:
        logger.error(f"{symbol} 分析异常: {e}")
        return None

# ---------- 发送推送 ----------
def send_telegram(signal):
    emoji = "🚀" if signal['direction'] == "LONG" else "📉"
    text = f"""{emoji} {signal['symbol']} — {signal['direction']} 信号

💰 价格: ${signal['price']:.4f}
📊 OI 1h: {signal['oi_change']*100:+.2f}%
📈 价格 1h: {signal['price_change']*100:+.2f}%
🔊 成交量: {signal['volume_ratio']:.2f}x
💵 资金费率: {signal['funding']*100:.4f}%

⭐ 信号强度: {signal['score']}
"""
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    try:
        session.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": text})
    except Exception as e:
        logger.error(f"Telegram发送失败: {e}")

def send_email(signal):
    if not ENABLE_EMAIL:
        return
    direction_cn = "做多" if signal['direction'] == "LONG" else "做空"
    direction_tag = "[LONG多头]" if signal['direction'] == 'LONG' else "[SHORT空头]"
    html = f"""
    <html><body>
        <h2 style="color:{'#00aa44' if signal['direction']=='LONG' else '#cc3344'}">
            {signal['symbol']} — {direction_cn}
        </h2>
        <table border=1 cellpadding=5>
            <tr><td>💰 价格</td><td>${signal['price']:.4f}</td></tr>
            <tr><td>📊 OI 1h</td><td>{signal['oi_change']*100:+.2f}%</td></tr>
            <tr><td>📈 价格1h</td><td>{signal['price_change']*100:+.2f}%</td></tr>
            <tr><td>🔊 成交量倍数</td><td>{signal['volume_ratio']:.2f}x</td></tr>
            <tr><td>💵 资金费率</td><td>{signal['funding']*100:.4f}%</td></tr>
            <tr><td>⭐ 信号强度</td><td>{signal['score']}</td></tr>
        </table>
    </body></html>
    """
    msg = MIMEMultipart('alternative')
    msg['From'] = formataddr((str(Header('OI监控', 'utf-8')), QQ_EMAIL))
    msg['To'] = EMAIL_TO
    msg['Subject'] = Header(f"{direction_tag} {signal['symbol']} 信号(强度{signal['score']})", 'utf-8').encode()
    msg.attach(MIMEText(html, 'html', 'utf-8'))
    try:
        server = smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, timeout=REQUEST_TIMEOUT)
        server.login(QQ_EMAIL, QQ_AUTH_CODE)
        recipients = [e.strip() for e in EMAIL_TO.split(',')]
        server.sendmail(QQ_EMAIL, recipients, msg.as_string())
        server.quit()
        logger.info(f"📧 邮件发送: {signal['symbol']}")
    except Exception as e:
        logger.error(f"邮件失败: {e}")

# ---------- 去重和入库 ----------
def should_alert(symbol, direction):
    conn = init_db()
    cutoff = datetime.now() - timedelta(hours=DEDUP_HOURS)
    cur = conn.execute("SELECT 1 FROM sent_log WHERE symbol=? AND direction=? AND sent_at > ?",
                       (symbol, direction, cutoff))
    if cur.fetchone():
        conn.close()
        return False
    conn.execute("INSERT INTO sent_log VALUES (?, ?, ?)",
                 (symbol, direction, datetime.now()))
    conn.commit()
    conn.close()
    return True

def save_signal_to_db(signal):
    conn = init_db()
    conn.execute("""INSERT INTO signals_history 
        (symbol, direction, score, price, oi_change, price_change, volume_ratio, funding, sent_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (signal['symbol'], signal['direction'], signal['score'], signal['price'],
         signal['oi_change'], signal['price_change'], signal['volume_ratio'],
         signal['funding'], datetime.now()))
    conn.commit()
    conn.close()

# ---------- 粗筛 ----------
def _check_one(sym):
    try:
        oi = get_oi_history(sym, "5m", 13)
        if len(oi) < 13:
            return None
        oi_prev = float(oi[0]['sumOpenInterest'])
        oi_now = float(oi[-1]['sumOpenInterest'])
        change = (oi_now - oi_prev) / oi_prev
        return sym if abs(change) > OI_CHANGE_THRESHOLD else None
    except Exception:
        return None

def quick_filter(symbols):
    if not symbols:
        return []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS_QUICK) as ex:
        results = list(ex.map(_check_one, symbols))
    return [s for s in results if s]

# ---------- 主循环 ----------
def main_loop():
    logger.info("🚀 精简版启动（无黑名单，保留频率限制）")
    symbols = get_all_symbols()
    if not symbols:
        logger.error("无币种，退出")
        return

    # 全局线程池（用于精筛）
    detail_pool = ThreadPoolExecutor(max_workers=MAX_WORKERS_DETAIL)
    import atexit
    atexit.register(detail_pool.shutdown, wait=False)

    last_refresh = time.time()

    while True:
        try:
            # 刷新币种列表
            if time.time() - last_refresh > SYMBOL_REFRESH_INTERVAL:
                new_syms = get_all_symbols()
                if new_syms:
                    symbols = new_syms
                    last_refresh = time.time()
                    logger.info("🔄 币种列表刷新")

            logger.info(f"⏰ 扫描开始... (总币种 {len(symbols)})")
            start = time.time()
            candidates = quick_filter(symbols)
            elapsed = time.time() - start
            logger.info(f"🔍 OI异动入围: {len(candidates)} 个 ({elapsed:.2f}s)")

            if candidates:
                futures = [detail_pool.submit(analyze_symbol, sym) for sym in candidates]
                for future in as_completed(futures):
                    signal = future.result()
                    if signal and should_alert(signal['symbol'], signal['direction']) and can_alert_frequency(signal['symbol'], signal['direction']):
                        logger.info(f"✅ 新信号: {signal['symbol']} {signal['direction']} 强度{signal['score']}")
                        save_signal_to_db(signal)
                        send_telegram(signal)
                        send_email(signal)
            else:
                logger.info("无入围币种")

            logger.info(f"💤 休眠 {SCAN_INTERVAL}s")
            time.sleep(SCAN_INTERVAL)

        except KeyboardInterrupt:
            logger.info("手动退出")
            detail_pool.shutdown(wait=False)
            break
        except Exception as e:
            logger.error(f"主循环异常: {e}")
            time.sleep(30)

if __name__ == "__main__":
    main_loop()