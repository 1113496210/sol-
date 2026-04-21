import json
import time
import requests
import smtplib
from email.mime.text import MIMEText
from datetime import datetime

BASE_URL = "https://fapi.binance.com"
HEADERS = {"User-Agent": "Mozilla/5.0"}

TG_TOKEN = "8557301222:AAHj1rSQ63zJGFXVxxuTniwRP2Y1tj3QsAs"
TG_CHAT_ID = "5408890841"
QQ_EMAIL = "1113496210@qq.com"
QQ_AUTH_CODE = "hzshvazrbnyzfhdf"
EMAIL_TO = "1113496210@qq.com"

MIN_VOLUME_24H = 3_000_000
SCAN_INTERVAL = 3600

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

BLACKLIST = [
    "USDCUSDT", "FDUSDUSDT", "TUSDUSDT", "DAIUSDT",
    "EURUSDT", "GBPUSDT", "JPYUSDT", "AUDUSDT",
]

signal_history = {}

def send_tg(msg):
    try:
        url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
        requests.post(url, data={"chat_id": TG_CHAT_ID, "text": msg}, timeout=10)
    except:
        pass

def send_email(msg):
    try:
        smtp = smtplib.SMTP_SSL("smtp.qq.com", 465)
        smtp.login(QQ_EMAIL, QQ_AUTH_CODE)
        m = MIMEText(msg, "plain", "utf-8")
        m["Subject"] = "Building Signal"
        m["From"] = QQ_EMAIL
        m["To"] = EMAIL_TO
        smtp.sendmail(QQ_EMAIL, EMAIL_TO, m.as_string())
        smtp.quit()
    except:
        pass

def notify(msg):
    print(msg)
    send_tg(msg)
    send_email(msg)

# API函数
def get_all_symbols():
    try:
        url = f"{BASE_URL}/fapi/v1/exchangeInfo"
        data = requests.get(url, headers=HEADERS, timeout=10).json()
        symbols = []
        for s in data["symbols"]:
            if (s["contractType"] == "PERPETUAL"
                    and s["quoteAsset"] == "USDT"
                    and s["status"] == "TRADING"
                    and s["symbol"] not in BLACKLIST):
                symbols.append(s["symbol"])
        return symbols
    except:
        return []

def get_24h_tickers():
    try:
        url = f"{BASE_URL}/fapi/v1/ticker/24hr"
        data = requests.get(url, headers=HEADERS, timeout=10).json()
        result = {}
        for t in data:
            result[t["symbol"]] = {
                "volume": float(t["quoteVolume"]),
                "change": float(t["priceChangePercent"]),
            }
        return result
    except:
        return {}

def get_klines(symbol, interval="1d", limit=35):
    try:
        url = f"{BASE_URL}/fapi/v1/klines"
        params = {"symbol": symbol, "interval": interval, "limit": limit}
        data = requests.get(url, headers=HEADERS, params=params, timeout=10).json()
        if isinstance(data, list):
            return [{
                "o": float(k[1]),
                "h": float(k[2]),
                "l": float(k[3]),
                "c": float(k[4]),
                "v": float(k[5]),
                "t": k[0],
            } for k in data]
    except:
        pass
    return None

def get_oi(symbol):
    try:
        url = f"{BASE_URL}/fapi/v1/openInterest"
        data = requests.get(url, headers=HEADERS, params={"symbol": symbol}, timeout=10).json()
        return float(data["openInterest"])
    except:
        return None

def get_oi_history(symbol, period="1d", limit=5):
    try:
        url = f"{BASE_URL}/futures/data/openInterestHist"
        params = {"symbol": symbol, "period": period, "limit": limit}
        data = requests.get(url, headers=HEADERS, params=params, timeout=10).json()
        if isinstance(data, list):
            return [float(d["sumOpenInterest"]) for d in data]
    except:
        pass
    return None

def get_funding_rate(symbol, limit=10):
    try:
        url = f"{BASE_URL}/fapi/v1/fundingRate"
        params = {"symbol": symbol, "limit": limit}
        data = requests.get(url, headers=HEADERS, params=params, timeout=10).json()
        if isinstance(data, list):
            return [float(d["fundingRate"]) * 100 for d in data]
    except:
        pass
    return None

# 三阶段检测逻辑
def check_stage1_decline(klines):
    if not klines or len(klines) < 30:
        return None

    closes = [k["c"] for k in klines]
    vols = [k["v"] for k in klines]

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

    for i in range(len(klines) - 5, len(klines)):
        if i < 1:
            continue

        k = klines[i]
        prev = klines[i - 1]
        prev5_vols = [klines[j]["v"] for j in range(max(0, i - 5), i)]

        if not prev5_vols:
            continue

        avg5 = sum(prev5_vols) / len(prev5_vols)
        change = (k["c"] - prev["c"]) / prev["c"] * 100

        if (k["v"] >= avg5 * VOL_SURGE_MULT
                and SURGE_CHANGE_MIN <= change <= SURGE_CHANGE_MAX
                and k["c"] > prev["c"]):
            return {
                "surge_day": i,
                "surge_vol_ratio": k["v"] / avg5,
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

    range_pct = (max(highs) - min(lows)) / min(lows)

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
    funding = get_funding_rate(symbol, 10)

    result = {"oi_change": 0, "oi_increasing": False, "funding_avg": 0, "funding_ok": True}

    if oi_hist and len(oi_hist) >= 3:
        oi_recent = oi_hist[-1]
        oi_before = oi_hist[-3]
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
    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    lines = [
        f"{icon} 建仓信号 [{label}]",
        f"币种: {sym}",
        f"时间: {now}",
        f"价格: {price:.6f}",
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

def scan_once():
    print(f"[{datetime.now().strftime('%H:%M')}] 开始扫描...")

    tickers = get_24h_tickers()
    all_symbols = get_all_symbols()

    symbols = [s for s in all_symbols if s in tickers and tickers[s]["volume"] >= MIN_VOLUME_24H]

    print(f" 符合流动性: {len(symbols)} 币种")

    count = {"observe": 0, "watch": 0, "entry": 0}

    for i, sym in enumerate(symbols):
        try:
            klines = get_klines(sym, "1d", 35)
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

            prev_level = signal_history.get(sym)
            if prev_level == level:
                continue

            signal_history[sym] = level
            count[level] += 1

            msg = format_building_signal(sym, level, price, stage1, stage2, stage3, oi_data)
            notify(msg)

        except Exception as e:
            print(f" {sym} 错误: {e}")

        if (i + 1) % 50 == 0:
            time.sleep(2)

    print(f" 完成: 观察{count['observe']} 关注{count['watch']} 介入{count['entry']}")

if __name__ == "__main__":
    print("🔍 建仓扫描系统启动")
    notify("🔍 建仓扫描系统启动")

    while True:
        try:
            scan_once()
        except Exception as e:
            print(f"扫描异常: {e}")

        print(f"下次扫描: {SCAN_INTERVAL // 60}分钟后")
        time.sleep(SCAN_INTERVAL)