#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
币安永续合约多因子信号监控 – 最终修复版
- 邮件头编码兼容 QQ 邮箱
- 请求连接池优化
- sqlite3 datetime 适配
- Telegram + QQ 邮箱双推送
"""

import sqlite3
import time
import logging
import requests
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.header import Header
from email.utils import formataddr
from requests.adapters import HTTPAdapter
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

# ======================== 配置区域 ========================
# ---------- Telegram ----------
TELEGRAM_TOKEN = "8557301222:AAHj1rSQ63zJGFXVxxuTniwRP2Y1tj3QsAs"          # 替换为真实 Token
TELEGRAM_CHAT_ID = "5408890841"          # 替换为真实 Chat ID

# ---------- 邮箱配置 ----------
ENABLE_EMAIL = True                              # 是否启用邮箱通知
QQ_EMAIL = "1113496210@qq.com"                    # 例如 12345678@qq.com
QQ_AUTH_CODE = "hzshvazrbnyzfhdf"                 # QQ邮箱授权码（不是密码）
EMAIL_TO = "1113496210@qq.com"                    # 收件邮箱，多个用逗号隔开
SMTP_HOST = "smtp.qq.com"
SMTP_PORT = 465

# ---------- 监控参数 ----------
SCAN_INTERVAL = 180                      # 扫描间隔（秒）
SYMBOL_REFRESH_INTERVAL = 3600           # 币种列表刷新间隔（秒）
OI_CHANGE_THRESHOLD = 0.10               # OI 1h 变化 >10% 进入粗筛
DEDUP_HOURS = 1                          # 同一币种同方向1小时内不重复推送

OI_STRONG = 0.15                         # OI 强信号阈值
VOLUME_MULTIPLIER = 2.0                  # 成交量放大倍数
PRICE_CHANGE_MIN = 0.02                  # 1h 价格变化 >2%
FUNDING_RATE_MAX = 0.001                 # 资金费率上限 0.1%
MIN_FACTORS_TO_ALERT = 3                 # 至少4个因子同向才推送

# 流动性过滤：只监控 24h 成交额 > MIN_24H_VOLUME_USDT 的币
MIN_24H_VOLUME_USDT = 10_000_000         # 1000万 USDT

# 并发配置
MAX_WORKERS_QUICK = 20                   # 粗筛并发数
MAX_WORKERS_DETAIL = 10                  # 精筛并发数

# 全局请求超时(秒)
REQUEST_TIMEOUT = 10

# 日志配置
LOG_FILE = "monitor.log"
# ==========================================================

BINANCE_FAPI_BASE = "https://fapi.binance.com"

# ---------- 全局 Session 与超时 patch + 连接池优化 ----------
session = requests.Session()
# 配置连接池
adapter = HTTPAdapter(pool_connections=30, pool_maxsize=30)
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
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ---------- 数据库（信号全量存储）----------
# 修复 datetime 弃用警告
sqlite3.register_adapter(datetime, lambda dt: dt.isoformat())

def init_db():
    conn = sqlite3.connect("signals.db")
    # 去重表（简化版，只存 symbol/direction/sent_at 用于去重）
    conn.execute("""CREATE TABLE IF NOT EXISTS sent_log (
        symbol TEXT,
        direction TEXT,
        sent_at TIMESTAMP
    )""")
    # 历史信号表（全量因子，用于回测）
    conn.execute("""CREATE TABLE IF NOT EXISTS signals_history (
        symbol TEXT,
        direction TEXT,
        score INTEGER,
        price REAL,
        oi_change REAL,
        price_change REAL,
        volume_ratio REAL,
        funding REAL,
        sent_at TIMESTAMP
    )""")
    conn.commit()
    return conn

def should_alert(symbol, direction):
    """检查是否重复推送（基于 sent_log 表）"""
    conn = init_db()
    cutoff = datetime.now() - timedelta(hours=DEDUP_HOURS)
    cur = conn.execute(
        "SELECT 1 FROM sent_log WHERE symbol=? AND direction=? AND sent_at > ?",
        (symbol, direction, cutoff)
    )
    if cur.fetchone():
        conn.close()
        return False
    # 记录本次推送
    conn.execute("INSERT INTO sent_log VALUES (?, ?, ?)",
                 (symbol, direction, datetime.now()))
    conn.commit()
    conn.close()
    return True

def save_signal_to_db(signal):
    """将完整信号存入 signals_history 表"""
    conn = init_db()
    conn.execute("""INSERT INTO signals_history 
        (symbol, direction, score, price, oi_change, price_change, volume_ratio, funding, sent_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (signal['symbol'], signal['direction'], signal['score'],
         signal['price'], signal['oi_change'], signal['price_change'],
         signal['volume_ratio'], signal['funding'], datetime.now()))
    conn.commit()
    conn.close()

# ---------- 币安 API 封装（全部使用 patched session，自动带超时）----------
def get_all_symbols():
    """获取所有 USDT 永续合约，并过滤低流动性"""
    try:
        # 1. 获取合约基本信息
        r = session.get(f"{BINANCE_FAPI_BASE}/fapi/v1/exchangeInfo")
        r.raise_for_status()
        info = r.json()
        all_symbols = [s['symbol'] for s in info['symbols']
                       if s['contractType'] == 'PERPETUAL' and s['quoteAsset'] == 'USDT']
        
        # 2. 获取24小时ticker（成交额）
        tickers = session.get(f"{BINANCE_FAPI_BASE}/fapi/v1/ticker/24hr").json()
        high_volume_symbols = set()
        for t in tickers:
            try:
                if float(t.get('quoteVolume', 0)) > MIN_24H_VOLUME_USDT:
                    high_volume_symbols.add(t['symbol'])
            except:
                continue
        
        # 3. 取交集
        active_symbols = [s for s in all_symbols if s in high_volume_symbols]
        logger.info(f"总永续合约: {len(all_symbols)} → 过滤后活跃币种: {len(active_symbols)}")
        return active_symbols
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
    return float(r.json()['lastFundingRate'])

# ---------- 信号引擎 ----------
def analyze_symbol(symbol):
    """多因子分析，返回信号字典或 None"""
    try:
        # 1. OI 变化（1小时）
        oi_hist = get_oi_history(symbol, "1h", 2)
        if len(oi_hist) < 2:
            return None
        oi_prev = float(oi_hist[0]['sumOpenInterest'])
        oi_now = float(oi_hist[-1]['sumOpenInterest'])
        oi_change = (oi_now - oi_prev) / oi_prev

        # 2. K线及价格
        klines = get_klines(symbol, "15m", 20)
        closes = [float(k[4]) for k in klines]
        volumes = [float(k[5]) for k in klines]
        # 最近1小时价格变化（最近4根15m K线）
        price_change_1h = (closes[-1] - closes[-5]) / closes[-5]

        # 3. 成交量放大倍数（最近3根 vs 之前的平均值）
        if len(volumes) < 4:
            return None
        avg_volume = sum(volumes[:-3]) / len(volumes[:-3])
        recent_volume = sum(volumes[-3:]) / 3
        volume_ratio = recent_volume / avg_volume if avg_volume > 0 else 0

        # 4. K线形态（最近3根中阳线数量）
        bullish_candles = sum(1 for k in klines[-3:] if float(k[4]) > float(k[1]))

        # 5. 资金费率
        funding = get_funding_rate(symbol)

        # ========== 因子打分 ==========
        bull, bear = 0, 0

        # 因子1：OI 大涨
        if oi_change > OI_STRONG:
            if price_change_1h > 0:
                bull += 1
            else:
                bear += 1

        # 因子2：成交量放大 + 价格方向
        if volume_ratio > VOLUME_MULTIPLIER:
            if price_change_1h > PRICE_CHANGE_MIN:
                bull += 1
            elif price_change_1h < -PRICE_CHANGE_MIN:
                bear += 1

        # 因子3：K线方向（修正：阳线≥2看涨，=0看跌，=1中性不计）
        if bullish_candles >= 2:
            bull += 1
        elif bullish_candles == 0:
            bear += 1

        # 因子4：资金费率
        if 0 < funding < FUNDING_RATE_MAX:
            bull += 1
        elif funding < 0:
            bear += 1
        elif funding > 0.001:   # 过高费率，拥挤看空
            bear += 1

        # 因子5：价格突破
        recent_high = max(closes[-20:-1])
        recent_low = min(closes[-20:-1])
        if closes[-1] > recent_high:
            bull += 1
        elif closes[-1] < recent_low:
            bear += 1

        # 返回信号
        if bull >= MIN_FACTORS_TO_ALERT:
            return {
                "symbol": symbol, "direction": "LONG", "score": bull,
                "oi_change": oi_change, "price_change": price_change_1h,
                "volume_ratio": volume_ratio, "funding": funding,
                "price": closes[-1]
            }
        if bear >= MIN_FACTORS_TO_ALERT:
            return {
                "symbol": symbol, "direction": "SHORT", "score": bear,
                "oi_change": oi_change, "price_change": price_change_1h,
                "volume_ratio": volume_ratio, "funding": funding,
                "price": closes[-1]
            }
        return None

    except Exception as e:
        logger.error(f"{symbol} 分析失败: {e}")
        return None

# ---------- Telegram 推送（纯文本）----------
def send_telegram(signal):
    emoji = "🚀" if signal['direction'] == "LONG" else "📉"
    text = f"""{emoji} {signal['symbol']} — {signal['direction']} 信号

💰 价格: ${signal['price']:.4f}
📊 OI 1h: {signal['oi_change']*100:+.2f}%
📈 价格 1h: {signal['price_change']*100:+.2f}%
🔊 成交量: {signal['volume_ratio']:.2f}x
💵 资金费率: {signal['funding']*100:.4f}%

⭐ 信号强度: {signal['score']}/5
"""
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    try:
        session.post(url, json={
            "chat_id": TELEGRAM_CHAT_ID,
            "text": text,
        })
    except Exception as e:
        logger.error(f"Telegram 发送失败: {e}")

# ---------- QQ 邮箱推送（修复版）----------
def send_email(signal):
    """发送信号到 QQ 邮箱（修复头编码）"""
    if not ENABLE_EMAIL:
        return
    
    direction_cn = "做多" if signal['direction'] == "LONG" else "做空"
    direction_tag = "[LONG多头]" if signal['direction'] == 'LONG' else "[SHORT空头]"
    
    html = f"""
    <html><body style="font-family: Arial, sans-serif;">
        <h2 style="color: {'#00aa44' if signal['direction'] == 'LONG' else '#cc3344'};">
            {signal['symbol']} — {direction_cn} {'🚀' if signal['direction'] == 'LONG' else '📉'}
        </h2>
        <table style="border-collapse: collapse; width: 100%; max-width: 500px;">
            <tr><td style="padding: 8px; border: 1px solid #ddd;"><b>💰 价格</b></td>
                <td style="padding: 8px; border: 1px solid #ddd;">${signal['price']:.4f}</td>
            </tr>
            <tr><td style="padding: 8px; border: 1px solid #ddd;"><b>📊 OI 1h变化</b></td>
                <td style="padding: 8px; border: 1px solid #ddd;">{signal['oi_change']*100:+.2f}%</td>
            </tr>
            <tr><td style="padding: 8px; border: 1px solid #ddd;"><b>📈 价格1h变化</b></td>
                <td style="padding: 8px; border: 1px solid #ddd;">{signal['price_change']*100:+.2f}%</td>
            </tr>
            <tr><td style="padding: 8px; border: 1px solid #ddd;"><b>🔊 成交量倍数</b></td>
                <td style="padding: 8px; border: 1px solid #ddd;">{signal['volume_ratio']:.2f}x</td>
            </tr>
            <tr><td style="padding: 8px; border: 1px solid #ddd;"><b>💵 资金费率</b></td>
                <td style="padding: 8px; border: 1px solid #ddd;">{signal['funding']*100:.4f}%</td>
            </tr>
            <tr><td style="padding: 8px; border: 1px solid #ddd;"><b>⭐ 信号强度</b></td>
                <td style="padding: 8px; border: 1px solid #ddd;">{signal['score']}/5</td>
            </tr>
            <tr><td style="padding: 8px; border: 1px solid #ddd;"><b>⏰ 时间</b></td>
                <td style="padding: 8px; border: 1px solid #ddd;">{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</td>
            </tr>
        </table>
        <p style="color: #888; font-size: 12px; margin-top: 20px;">
            本信号仅供参考，不构成投资建议。请结合K线、大盘、风险偏好综合判断。
        </p>
    </body></html>
    """
    
    msg = MIMEMultipart('alternative')
    # 关键修复：发件人使用 formataddr，收件人直接字符串
    msg['From'] = formataddr((str(Header('OI监控', 'utf-8')), QQ_EMAIL))
    msg['To'] = EMAIL_TO
    # 主题去掉 emoji，并强制编码
    msg['Subject'] = Header(
        f"{direction_tag} {signal['symbol']} 信号(强度{signal['score']}/5)",
        'utf-8'
    ).encode()
    msg.attach(MIMEText(html, 'html', 'utf-8'))
    
    try:
        server = smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, timeout=REQUEST_TIMEOUT)
        server.login(QQ_EMAIL, QQ_AUTH_CODE)
        # 支持多个收件人（逗号分隔）
        recipients = [e.strip() for e in EMAIL_TO.split(',')]
        server.sendmail(QQ_EMAIL, recipients, msg.as_string())
        server.quit()
        logger.info(f"📧 邮件已发送: {signal['symbol']}")
    except Exception as e:
        logger.error(f"邮件发送失败: {e}")

# ---------- 并发粗筛 ----------
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
    logger.info("🚀 币安永续合约监控启动 (最终修复版)")
    symbols = get_all_symbols()
    if not symbols:
        logger.error("❌ 无法获取交易对，程序退出")
        return
    
    last_refresh = time.time()
    
    while True:
        try:
            # 定期刷新币种列表
            if time.time() - last_refresh > SYMBOL_REFRESH_INTERVAL:
                new_symbols = get_all_symbols()
                if new_symbols:
                    symbols = new_symbols
                    last_refresh = time.time()
                    logger.info("🔄 币种列表已刷新")
            
            logger.info(f"⏰ 开始扫描... (总币种: {len(symbols)})")
            start = time.time()
            candidates = quick_filter(symbols)
            elapsed = time.time() - start
            logger.info(f"🔍 OI异动入围: {len(candidates)} 个 (耗时 {elapsed:.2f}秒)")
            
            if candidates:
                # 精筛并发
                with ThreadPoolExecutor(max_workers=MAX_WORKERS_DETAIL) as ex:
                    future_to_sym = {ex.submit(analyze_symbol, sym): sym for sym in candidates}
                    for future in as_completed(future_to_sym):
                        signal = future.result()
                        if signal and should_alert(signal['symbol'], signal['direction']):
                            logger.info(f"✅ 新信号: {signal['symbol']} {signal['direction']} (强度 {signal['score']})")
                            save_signal_to_db(signal)   # 存全量数据
                            send_telegram(signal)       # Telegram 推送
                            send_email(signal)          # 邮箱推送
            else:
                logger.info("无 OI 异动币种")
            
            logger.info(f"💤 休眠 {SCAN_INTERVAL} 秒")
            time.sleep(SCAN_INTERVAL)
            
        except KeyboardInterrupt:
            logger.info("🛑 手动退出")
            break
        except Exception as e:
            logger.error(f"❌ 主循环异常: {e}")
            time.sleep(30)

if __name__ == "__main__":
    main_loop()