import json
import time
import threading
import requests
import websocket
import smtplib
import os
from email.mime.text import MIMEText
from datetime import datetime
from collections import defaultdict

# ========== 配置 ==========
BASE_URL = "https://fapi.binance.com"
WS_URL = "wss://fstream.binance.com/stream?streams="
HEADERS = {"User-Agent": "Mozilla/5.0"}
WS_BATCH = 150
TOP_N = 250
MIN_VOLUME_24H = 1_000_000
TG_TOKEN = "8557301222:AAHj1rSQ63zJGFXVxxuTniwRP2Y1tj3QsAs"
TG_CHAT_ID = "5408890841"
QQ_EMAIL = "1113496210@qq.com"
QQ_AUTH_CODE = "hzshvazrbnyzfhdf"
EMAIL_TO = "1113496210@qq.com"
BTC_EXTREME_ATR_PCT = 2.5
BTC_VETO_PCT = 1.5
MA_FAST = 5
MA_SLOW = 20
ZONE_BINS = 50
ZONE_LOOKBACK = 100
ZONE_TOLERANCE = 0.005
ZONE_MAX_WIDTH = 0.30
ZONE_MAX_WAIT = 10
SHRINK_RATIO = 0.6
ZONE_VETO_VOL = 1.5
ZONE_VETO_MOVE = 0.003
PIN_SHADOW_RATIO = 2.0
ENGULF_VOL_RATIO = 1.5
BREAKOUT_PCT = 0.005
BREAKOUT_VOL_RATIO = 2.0
BIG_ORDER_MULT = 5
BIG_ORDER_BIAS = 0.6
SCORE_A = 8
SCORE_B = 6
ATR_PERIOD = 14
ATR_CAP_MULT = 1.5
SWING_LOOKBACK = 10
TP1_MULT = 1.5
TP2_MULT = 3.0
SLIPPAGE_LONG = 1.002
SLIPPAGE_SHORT = 0.998
MIN_RISK_PCT = 1.0
MAX_CONSECUTIVE_LOSS = 2
PAUSE_HOURS = 6
COIN_BLACKLIST_HOURS = 24
COOLDOWN_MINUTES = 30

# ========== 全局状态 ==========
kline_cache = {}
btc_cache = {"1h": []}
zone_states = {}
cooldowns = {}
blacklist = {}
loss_counter = {"count": 0, "pause_until": 0}
coin_losses = {}
ema_state = {}
trade_cache = defaultdict(list)   # 存储 aggTrade 成交数据

# ========== 通知函数 ==========
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
        m["Subject"] = "Trading Signal"
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

# ========== 指标计算 ==========
def calc_ema_update(new_val, prev, period):
    if prev is None:
        return new_val
    k = 2 / (period + 1)
    return new_val * k + prev * (1 - k)

def calc_ema_full(values, period):
    if len(values) < period:
        return None
    ema = sum(values[:period]) / period
    for v in values[period:]:
        ema = calc_ema_update(v, ema, period)
    return ema

def calc_atr(klines, period=ATR_PERIOD):
    if len(klines) < period + 1:
        return None
    trs = []
    for i in range(1, len(klines)):
        h = klines[i]["h"]
        l = klines[i]["l"]
        pc = klines[i - 1]["c"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    if len(trs) < period:
        return None
    atr_sma = sum(trs[-period:]) / period
    atr_ema = trs[0]
    for t in trs[1:]:
        atr_ema = calc_ema_update(t, atr_ema, 10)
    return min(atr_sma, atr_ema * ATR_CAP_MULT)

# ========== 密集区和 ZoneState ==========
def calc_volume_profile(klines):
    if len(klines) < 10:
        return None
    prices = [(k["h"] + k["l"] + k["c"]) / 3 for k in klines]
    vols = [k["v"] for k in klines]
    p_min = min(prices)
    p_max = max(prices)
    if p_max <= p_min:
        return None
    step = (p_max - p_min) / ZONE_BINS
    hist = [0.0] * ZONE_BINS
    for p, v in zip(prices, vols):
        idx = min(int((p - p_min) / step), ZONE_BINS - 1)
        hist[idx] += v
    total = sum(hist)
    if total <= 0:
        return None
    mid_idx = hist.index(max(hist))
    left = right = mid_idx
    cum = hist[mid_idx]
    while cum < total * 0.7:
        el = hist[left - 1] if left > 0 else 0
        er = hist[right + 1] if right < ZONE_BINS - 1 else 0
        if el >= er and left > 0:
            left -= 1
            cum += hist[left]
        elif right < ZONE_BINS - 1:
            right += 1
            cum += hist[right]
        else:
            break
        if (right - left + 1) / ZONE_BINS > ZONE_MAX_WIDTH:
            break
    low = p_min + left * step
    high = p_min + (right + 1) * step
    return {"low": low, "high": high, "mid": (low + high) / 2}

class ZoneState:
    def __init__(self):
        self.in_zone = False
        self.bar_count = 0
        self.zone = None
    def enter(self):
        if not self.in_zone:
            self.in_zone = True
            self.bar_count = 0
    def tick(self):
        if self.in_zone:
            self.bar_count += 1
    def is_expired(self):
        return self.in_zone and self.bar_count > ZONE_MAX_WAIT
    def reset(self):
        self.in_zone = False
        self.bar_count = 0
        self.zone = None

# ========== 市场状态与趋势 ==========
def check_market_state():
    btc = btc_cache["1h"]
    if len(btc) < 30:
        return None
    atr = calc_atr(btc)
    if not atr:
        return None
    atr_pct = atr / btc[-1]["c"] * 100
    if atr_pct > BTC_EXTREME_ATR_PCT:
        return "extreme"
    closes = [k["c"] for k in btc]
    ma5 = calc_ema_full(closes, MA_FAST)
    ma20 = calc_ema_full(closes, MA_SLOW)
    if ma5 and ma20:
        if abs(ma5 - ma20) / ma20 * 100 < 0.3:
            return "range"
    return "trend"

def check_btc_veto(direction):
    btc = btc_cache["1h"]
    if len(btc) < 2:
        return False
    change = (btc[-1]["c"] - btc[-2]["c"]) / btc[-2]["c"] * 100
    if direction == "long" and change < -BTC_VETO_PCT:
        return True
    if direction == "short" and change > BTC_VETO_PCT:
        return True
    return False

def check_trend(sym):
    klines = kline_cache.get(sym, {}).get("1h", [])
    if len(klines) < MA_SLOW + 5:
        return None
    closes = [k["c"] for k in klines]
    price = closes[-1]
    if sym not in ema_state:
        ema_state[sym] = {
            "fast": calc_ema_full(closes, MA_FAST),
            "slow": calc_ema_full(closes, MA_SLOW),
        }
    else:
        es = ema_state[sym]
        es["fast"] = calc_ema_update(price, es["fast"], MA_FAST)
        es["slow"] = calc_ema_update(price, es["slow"], MA_SLOW)
    es = ema_state[sym]
    if es["fast"] is None or es["slow"] is None:
        return None
    if es["fast"] > es["slow"] and price > es["fast"]:
        return "long"
    if es["fast"] < es["slow"] and price < es["fast"]:
        return "short"
    return None

def check_zone(sym, direction):
    klines = kline_cache.get(sym, {}).get("15m", [])
    if len(klines) < ZONE_LOOKBACK:
        return False
    zone = calc_volume_profile(klines[-ZONE_LOOKBACK:])
    if not zone:
        return False
    price = klines[-1]["c"]
    in_zone = abs(price - zone["mid"]) / zone["mid"] <= ZONE_TOLERANCE
    if sym not in zone_states:
        zone_states[sym] = ZoneState()
    zs = zone_states[sym]
    if not in_zone:
        zs.reset()
        return False
    zs.enter()
    zs.tick()
    if zs.is_expired():
        zs.reset()
        return False
    closes = [k["c"] for k in klines[-30:]]
    vols = [k["v"] for k in klines[-30:]]
    if len(closes) < 10:
        return False
    recent_high_val = -float("inf")
    recent_high_idx = -1
    for i in range(len(closes) - 1, max(len(closes) - 20, -1), -1):
        if closes[i] > recent_high_val:
            recent_high_val = closes[i]
            recent_high_idx = i
    if recent_high_idx < 3 or recent_high_idx >= len(closes) - 1:
        return False
    vol_up = vols[max(0, recent_high_idx - 5):recent_high_idx]
    vol_pull = vols[recent_high_idx:]
    if not vol_up or not vol_pull:
        return False
    avg_up = sum(vol_up) / len(vol_up)
    avg_pull = sum(vol_pull) / len(vol_pull)
    vol_ma = sum(vols) / len(vols)
    if not (avg_pull <= avg_up * SHRINK_RATIO and avg_pull < vol_ma):
        return False
    last_vol = klines[-1]["v"]
    last_move = abs(klines[-1]["c"] - klines[-1]["o"]) / klines[-1]["o"] if klines[-1]["o"] > 0 else 0
    if last_vol > vol_ma * ZONE_VETO_VOL and last_move < ZONE_VETO_MOVE:
        zs.reset()
        return False
    zs.zone = zone
    return True

# ========== 入场信号检测 ==========
def check_entry(sym, direction):
    kl = kline_cache.get(sym, {}).get("3m", [])
    if len(kl) < 10:
        return None
    last = kl[-1]
    prev = kl[-2]
    prev5 = kl[-6:-1]
    body = abs(last["c"] - last["o"])
    if body == 0:
        body = 1e-8
    avg_vol = sum(k["v"] for k in prev5) / 5 if prev5 else 1
    if direction == "long":
        lower = min(last["o"], last["c"]) - last["l"]
        in_upper = last["c"] > (last["h"] + last["l"]) / 2
        if lower >= body * PIN_SHADOW_RATIO and in_upper and last["v"] >= avg_vol * BREAKOUT_VOL_RATIO:
            return "Pin Bar"
    if direction == "short":
        upper = last["h"] - max(last["o"], last["c"])
        in_lower = last["c"] < (last["h"] + last["l"]) / 2
        if upper >= body * PIN_SHADOW_RATIO and in_lower and last["v"] >= avg_vol * BREAKOUT_VOL_RATIO:
            return "Shooting Star"
    if direction == "long":
        if (last["c"] > last["o"] and prev["c"] < prev["o"]
                and last["c"] >= prev["o"] and last["o"] <= prev["c"]
                and last["v"] >= prev["v"] * ENGULF_VOL_RATIO):
            return "Bullish Engulf"
    if direction == "short":
        if (last["c"] < last["o"] and prev["c"] > prev["o"]
                and last["o"] >= prev["c"] and last["c"] <= prev["o"]
                and last["v"] >= prev["v"] * ENGULF_VOL_RATIO):
            return "Bearish Engulf"
    change = (last["c"] - last["o"]) / last["o"] if last["o"] > 0 else 0
    if direction == "long" and change >= BREAKOUT_PCT and last["v"] >= avg_vol * BREAKOUT_VOL_RATIO:
        return "Volume Breakout"
    if direction == "short" and change <= -BREAKOUT_PCT and last["v"] >= avg_vol * BREAKOUT_VOL_RATIO:
        return "Volume Breakdown"
    return None

def check_big_orders(trades, direction):
    if not trades or len(trades) < 10:
        return False
    sizes = sorted([t["qty"] * t["price"] for t in trades])
    median = sizes[len(sizes) // 2]
    if median <= 0:
        return False
    big = [t for t in trades if t["qty"] * t["price"] > median * BIG_ORDER_MULT]
    if not big:
        return False
    buy = sum(t["qty"] * t["price"] for t in big if not t["isBuyerMaker"])
    sell = sum(t["qty"] * t["price"] for t in big if t["isBuyerMaker"])
    total = buy + sell
    if total <= 0:
        return False
    if direction == "long" and buy / total > BIG_ORDER_BIAS:
        return True
    if direction == "short" and sell / total > BIG_ORDER_BIAS:
        return True
    return False

def calc_score(direction, pattern, big, btc_sync, zone_hit):
    score = 0
    details = []
    score += 2
    details.append("1H趋势 +2")
    if btc_sync:
        score += 2
        details.append("BTC同步 +2")
    else:
        details.append("BTC同步 +0")
    if zone_hit:
        score += 2
        details.append("15M密集区 +2")
    score += 1
    details.append("缩量回调 +1")
    if pattern:
        score += 1
        details.append(f"{pattern} +1")
    score += 1
    details.append("放量确认 +1")
    if big:
        score += 1
        details.append("大单 +1")
    else:
        details.append("大单 +0")
    return score, details

def calc_risk(sym, direction, entry_price):
    k15 = kline_cache.get(sym, {}).get("15m", [])
    if len(k15) < SWING_LOOKBACK:
        return None
    atr = calc_atr(k15)
    if atr is None or atr <= 0:
        return None
    zs = zone_states.get(sym)
    if zs is None or zs.zone is None:
        return None
    zone = zs.zone
    if direction == "long":
        swing_low = min(k["l"] for k in k15[-SWING_LOOKBACK:])
        stop = max(zone["low"] - atr, swing_low - atr * 0.5)
        entry = entry_price * SLIPPAGE_LONG
        risk = entry - stop
        if risk <= 0 or (risk / entry * 100) < MIN_RISK_PCT:
            return None
        tp1 = entry + risk * TP1_MULT
        recent_high = max(k["h"] for k in k15[-30:])
        tp2 = max(recent_high, entry + risk * TP2_MULT)
    else:
        swing_high = max(k["h"] for k in k15[-SWING_LOOKBACK:])
        stop = min(zone["high"] + atr, swing_high + atr * 0.5)
        entry = entry_price * SLIPPAGE_SHORT
        risk = stop - entry
        if risk <= 0 or (risk / entry * 100) < MIN_RISK_PCT:
            return None
        tp1 = entry - risk * TP1_MULT
        recent_low = min(k["l"] for k in k15[-30:])
        tp2 = min(recent_low, entry - risk * TP2_MULT)
    return {
        "entry": entry, "stop": stop, "risk": risk,
        "risk_pct": risk / entry * 100,
        "tp1": tp1, "tp2": tp2, "rr1": TP1_MULT,
    }

def is_cooled(sym):
    now = time.time()
    if sym in cooldowns and now < cooldowns[sym]:
        return True
    if sym in blacklist and now < blacklist[sym]:
        return True
    if now < loss_counter.get("pause_until", 0):
        return True
    return False

def record_cooldown(sym):
    cooldowns[sym] = time.time() + COOLDOWN_MINUTES * 60

def record_loss(sym):
    now = time.time()
    loss_counter["count"] = loss_counter.get("count", 0) + 1
    if loss_counter["count"] >= MAX_CONSECUTIVE_LOSS:
        loss_counter["pause_until"] = now + PAUSE_HOURS * 3600
        loss_counter["count"] = 0
    if sym not in coin_losses:
        coin_losses[sym] = {"count": 0}
    coin_losses[sym]["count"] += 1
    if coin_losses[sym]["count"] >= MAX_CONSECUTIVE_LOSS:
        blacklist[sym] = now + COIN_BLACKLIST_HOURS * 3600
        coin_losses[sym]["count"] = 0

def record_win():
    loss_counter["count"] = 0

def generate_signal(sym, trades):
    if is_cooled(sym):
        return None
    market = check_market_state()
    if market == "extreme":
        return None
    direction = check_trend(sym)
    if direction is None:
        return None
    if check_btc_veto(direction):
        return None
    btc_sync = False
    btc = btc_cache["1h"]
    if len(btc) >= MA_SLOW + 5:
        bc = [k["c"] for k in btc]
        bma5 = calc_ema_full(bc, MA_FAST)
        bma20 = calc_ema_full(bc, MA_SLOW)
        bmom = (btc[-1]["c"] - btc[-2]["c"]) / btc[-2]["c"] if len(btc) >= 2 else 0
        if bma5 and bma20:
            if direction == "long" and bma5 > bma20 and bmom > 0:
                btc_sync = True
            elif direction == "short" and bma5 < bma20 and bmom < 0:
                btc_sync = True
    if not check_zone(sym, direction):
        return None
    pat = check_entry(sym, direction)
    if not pat:
        return None
    big = check_big_orders(trades, direction) if trades else False
    score, detail = calc_score(direction, pat, big, btc_sync, True)
    if market == "range" and score < SCORE_A:
        return None
    if score < SCORE_B:
        return None
    grade = "A" if score >= SCORE_A else "B"
    price = kline_cache[sym]["3m"][-1]["c"]
    risk = calc_risk(sym, direction, price)
    if not risk:
        return None
    record_cooldown(sym)
    return {
        "sym": sym,
        "direction": direction,
        "grade": grade,
        "score": score,
        "details": detail,
        "pattern": pat,
        "market": market,
        **risk,
    }

def format_signal(sig):
    d = "做多" if sig["direction"] == "long" else "做空"
    icon = "🔵" if sig["direction"] == "long" else "🔴"
    now = datetime.now().strftime("%H:%M")
    lines = [
        f"{icon} {d}信号 [{sig['grade']} {sig['score']}分]",
        f"币种: {sig['sym']}",
        f"时间: {now}",
        "",
    ]
    for x in sig["details"]:
        lines.append(f"  {x}")
    lines += [
        "",
        f"入场: {sig['entry']:.6f}",
        f"止损: {sig['stop']:.6f} (-{sig['risk_pct']:.2f}%)",
        f"TP1: {sig['tp1']:.6f} (+{sig['risk_pct'] * TP1_MULT:.2f}%) -> 平50%",
        f"TP2: {sig['tp2']:.6f} -> 平25%",
        f"TP3: 移动止损 -> 剩25%",
    ]
    return "\n".join(lines)

# ========== 数据获取（带缓存） ==========
SYMBOLS_CACHE_FILE = "/root/001/my_trading_bot/symbols_cache.json"
CACHE_TTL = 86400

def get_top_symbols():
    if os.path.exists(SYMBOLS_CACHE_FILE):
        mtime = os.path.getmtime(SYMBOLS_CACHE_FILE)
        if time.time() - mtime < CACHE_TTL:
            try:
                with open(SYMBOLS_CACHE_FILE, 'r') as f:
                    symbols = json.load(f)
                    if symbols:
                        print(f"从缓存加载币种: {len(symbols)}")
                        return symbols
            except:
                pass
    try:
        url = f"{BASE_URL}/fapi/v1/ticker/24hr"
        data = requests.get(url, headers=HEADERS, timeout=10).json()
        data = [x for x in data if x["symbol"].endswith("USDT")]
        data.sort(key=lambda x: float(x["quoteVolume"]), reverse=True)
        result = []
        for x in data:
            if float(x["quoteVolume"]) >= MIN_VOLUME_24H:
                result.append(x["symbol"])
            if len(result) >= TOP_N:
                break
        with open(SYMBOLS_CACHE_FILE, 'w') as f:
            json.dump(result, f)
        print(f"从API获取币种: {len(result)}")
        return result
    except Exception as e:
        print(f"获取币种失败: {e}")
        return []

def get_klines_rest(symbol, interval, limit=50):
    try:
        url = f"{BASE_URL}/fapi/v1/klines"
        params = {"symbol": symbol, "interval": interval, "limit": limit}
        data = requests.get(url, headers=HEADERS, params=params, timeout=10).json()
        if isinstance(data, list):
            return [{"o": float(k[1]), "h": float(k[2]), "l": float(k[3]), "c": float(k[4]), "v": float(k[5])} for k in data]
    except:
        pass
    return None

def load_history(symbols):
    print("加载历史K线...")
    data = get_klines_rest("BTCUSDT", "1h", 100)
    if data:
        btc_cache["1h"] = data
        print("  BTC ✅")
    for i, sym in enumerate(symbols):
        kline_cache[sym] = {}
        for tf, limit in [("3m", 50), ("15m", 120), ("1h", 100)]:
            data = get_klines_rest(sym, tf, limit)
            kline_cache[sym][tf] = data if data else []
        if (i + 1) % 50 == 0:
            print(f" {i + 1}/{len(symbols)}")
        time.sleep(2)
    print(f"加载完成: {len(kline_cache)} 币种")

# ========== WebSocket 处理 ==========
def on_message(ws, message):
    try:
        data = json.loads(message)
        # 组合流格式
        if "data" in data and "stream" in data:
            stream = data["stream"]
            result = data["data"]
        else:
            # 可能是单流格式（非组合流），直接判断事件类型
            if "e" in data and data["e"] == "aggTrade":
                sym = data["s"]
                trade = {
                    "price": float(data["p"]),
                    "qty": float(data["q"]),
                    "isBuyerMaker": data["m"],
                    "time": data["T"]
                }
                trades = trade_cache[sym]
                trades.append(trade)
                if len(trades) > 500:
                    trades.pop(0)
                return
            # 其他格式忽略
            return

        # 处理 K线数据
        if "@kline" in stream:
            k = result.get("k")
            if not k:
                return
            sym = k["s"]
            tf = k["i"]
            closed = k["x"]
            bar = {
                "o": float(k["o"]),
                "h": float(k["h"]),
                "l": float(k["l"]),
                "c": float(k["c"]),
                "v": float(k["v"]),
            }
            if sym == "BTCUSDT" and tf == "1h":
                if closed:
                    btc_cache["1h"].append(bar)
                    if len(btc_cache["1h"]) > 200:
                        btc_cache["1h"] = btc_cache["1h"][-200:]
                elif btc_cache["1h"]:
                    btc_cache["1h"][-1] = bar
                return
            if sym not in kline_cache:
                return
            kc = kline_cache[sym].get(tf, [])
            if closed:
                kc.append(bar)
                max_len = {"3m": 50, "15m": 150, "1h": 200}
                if len(kc) > max_len.get(tf, 200):
                    kline_cache[sym][tf] = kc[-max_len.get(tf, 200):]
                if tf == "3m" and len(kc) >= 20:
                    trades = trade_cache.get(sym, [])
                    sig = generate_signal(sym, trades)
                    if sig:
                        notify(format_signal(sig))
            elif kc:
                kc[-1] = bar

        # 处理 aggTrade 流（组合流模式）
        elif "@aggTrade" in stream:
            sym = result.get("s")
            if not sym:
                return
            trade = {
                "price": float(result["p"]),
                "qty": float(result["q"]),
                "isBuyerMaker": result["m"],
                "time": result["T"]
            }
            trades = trade_cache[sym]
            trades.append(trade)
            if len(trades) > 500:
                trades.pop(0)

    except Exception as e:
        print(f"WS处理错误: {e}")

def on_error(ws, error):
    print(f"WS错误: {error}")

def on_close(ws, close_status_code, close_msg):
    print("WS连接关闭")

def on_open(ws):
    print("WS连接打开")

def start_ws(symbols):
    all_streams = ["btcusdt@kline_1h"]
    for sym in symbols:
        s = sym.lower()
        all_streams.append(f"{s}@kline_3m")
        all_streams.append(f"{s}@kline_15m")
        all_streams.append(f"{s}@kline_1h")
        all_streams.append(f"{s}@aggTrade")
    for i in range(0, len(all_streams), WS_BATCH):
        batch = all_streams[i:i + WS_BATCH]
        url = WS_URL + "/".join(batch)
        def run(u=url, idx=i // WS_BATCH):
            while True:
                try:
                    ws = websocket.WebSocketApp(u,
                                                on_open=on_open,
                                                on_message=on_message,
                                                on_error=on_error,
                                                on_close=on_close)
                    print(f"[WS-{idx}] 连接中...")
                    ws.run_forever(ping_interval=60, ping_timeout=30)
                except Exception as e:
                    print(f"[WS-{idx}] 错误: {e}")
                time.sleep(5)
        threading.Thread(target=run, daemon=True).start()

# ========== 主程序 ==========
if __name__ == "__main__":
    print("🚀 启动完整WebSocket版策略（含aggTrade）")
    notify("🚀 启动完整WebSocket版策略（含aggTrade）")
    symbols = get_top_symbols()
    print(f"监控币种: {len(symbols)}")
    if not symbols:
        print("⚠️ 未获取到币种，退出")
        notify("⚠️ 未获取到币种，退出")
        exit(1)
    load_history(symbols)
    start_ws(symbols)
    notify(f"✅ 策略启动，监控 {len(symbols)} 个币种")
    while True:
        time.sleep(300)
        active = sum(1 for z in zone_states.values() if z.in_zone)
        paused = time.time() < loss_counter.get("pause_until", 0)
        status = "⏸️ 暂停" if paused else "✅ 运行"
        print(f"[{status}] 追踪:{active} | {datetime.now().strftime('%H:%M')}")
MAINEOF