# -*- coding: utf-8 -*-
# =====================================================
# Binance USDT 永续合约监控系统 v2
# WS(C系统,前200) + REST(A/B/补漏/回扫) + 日报 + SQLite
# 依赖: pip install requests pandas openpyxl websocket-client
# =====================================================

import time
import json
import sqlite3
import threading
import requests
import smtplib
import pandas as pd
import websocket
from collections import defaultdict, deque
from datetime import datetime, timezone, timedelta, date
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.header import Header
from email.utils import formataddr

# ================= 基础配置 =================
BINANCE_API = "https://fapi.binance.com"
BINANCE_WS = "wss://fstream.binance.com/stream"
SCAN_INTERVAL = 60  # REST 主扫描间隔(秒)
TOP_N = 200  # WS 订阅前 N 名
TOP_REFRESH_SEC = 3600  # 前 N 名重排间隔(秒)
DB_FILE = "monitor.db"

# ================= 系统 A(启动哨兵) =================
A_PCT = 1.8  # 1M 涨幅阈值
A_VOL_MULT = 1.5  # 量能倍数
# 每币每日只播一次(逻辑里控制)

# ================= 系统 B(3M 推进) =================
B_HH_BARS = 3  # 连续 HH 根数
B_MIN_TOTAL_PCT = 2.0  # 启动最小累计涨幅
B_PROGRESS_PCT = 4.0  # 有效推进最小涨幅(相对上次推进价)
B_PROGRESS_INTERVAL = 300  # 两次推进最小间隔(秒)
B_DRAWDOWN_FAIL = 0.07  # 回撤退出
B_MAX_DAILY_ROUND = 2  # 每日最大轮数
B_MAX_PUSH = 5  # 单轮最大推进次数

# ================= 系统 C(15M 主升浪) =================
C_POS_MIN_RATIO = 0.33  # 当前价距 24h 低 / 24h 区间 ≥ 此值
C_HIGH_GAP_PCT = 8.0  # 当前价距 24h 高 至少剩此空间(%)
C_TRIG_A_VOL_MULT = 2.0  # 触发a:量倍数
C_TRIG_B_BULL_BARS = 4  # 触发b:多头排列持续根数
C_TRIG_B_VOL_MULT = 1.5  # 触发b:量倍数
C_PROGRESS_PCT = 4.0
C_PROGRESS_INTERVAL = 300
C_MAX_DAILY_ROUND = 2
C_MAX_PUSH = 5

# ================= 区间补漏 =================
RANGE_THRESHOLDS = [
    ("30M", 30, 5.0),
    ("1H", 60, 8.0),
    ("4H", 240, 15.0),
]

# ================= 启动回扫 =================
BOOT_LOOKBACK_HOURS = 4
BOOT_RISE_PCT = 8.0  # 4H 累计涨幅 ≥ 此值视为已在涨势

# ================= 全市场噪音 =================
NOISE_PCT = 1.2
NOISE_RATIO = 0.6

# ================= 通知(硬编码,自己换) =================
BOT_TOKEN = "8557301222:AAHj1rSQ63zJGFXVxxuTniwRP2Y1tj3QsAs"
CHAT_ID = "5408890841"

EMAIL_USER = "1113496210@qq.com"
EMAIL_PASS = "hzshvazrbnyzfhdf"
EMAIL_TO = "1113496210@qq.com"

# ================= 时区 =================
TZ_CN = timezone(timedelta(hours=8))

def now_cn():
    return datetime.now(TZ_CN)

def today_cn():
    return now_cn().date()

# ================= 通知模块 =================
_notify_lock = threading.Lock()
_a_buffer = []  # A 系统合并缓冲
_a_buffer_lock = threading.Lock()
_a_flush_timer = None

def send_tg(text):
    try:
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        requests.post(url, json={"chat_id": CHAT_ID, "text": text}, timeout=10)
    except Exception as e:
        print(f"[TG ERROR] {e}")

def send_email_text(subject, content):
    try:
        msg = MIMEText(content, "plain", "utf-8")
        msg["From"] = formataddr(("盘面监控", EMAIL_USER))
        msg["To"] = EMAIL_TO
        msg["Subject"] = Header(subject, "utf-8")
        s = smtplib.SMTP_SSL("smtp.qq.com", 465, timeout=15)
        s.login(EMAIL_USER, EMAIL_PASS)
        s.sendmail(EMAIL_USER, [EMAIL_TO], msg.as_string())
        s.quit()
    except Exception as e:
        print(f"[MAIL ERROR] {e}")

def send_email_attachment(subject, body, filepath):
    try:
        msg = MIMEMultipart()
        msg["From"] = formataddr(("盘面监控", EMAIL_USER))
        msg["To"] = EMAIL_TO
        msg["Subject"] = Header(subject, "utf-8")
        msg.attach(MIMEText(body, "plain", "utf-8"))
        with open(filepath, "rb") as f:
            part = MIMEApplication(f.read(), Name=filepath)
            part["Content-Disposition"] = f'attachment; filename="{filepath}"'
            msg.attach(part)
        s = smtplib.SMTP_SSL("smtp.qq.com", 465, timeout=30)
        s.login(EMAIL_USER, EMAIL_PASS)
        s.sendmail(EMAIL_USER, [EMAIL_TO], msg.as_string())
        s.quit()
    except Exception as e:
        print(f"[MAIL ATTACH ERROR] {e}")

def notify(text):
    """通用即时通知:TG + 邮件"""
    with _notify_lock:
        send_tg(text)
        send_email_text("实时监控提示", text)

def notify_a_merge(symbol, price, pct, vol_mult):
    """A 系统合并通知:同分钟批量入 buffer,1.5 秒后统一发出"""
    global _a_flush_timer
    item = {"sym": symbol, "price": price, "pct": pct, "vol": vol_mult, "t": now_cn()}
    with _a_buffer_lock:
        _a_buffer.append(item)
        if _a_flush_timer is None:
            _a_flush_timer = threading.Timer(1.5, _a_flush)
            _a_flush_timer.start()

def _a_flush():
    global _a_flush_timer
    with _a_buffer_lock:
        items = list(_a_buffer)
        _a_buffer.clear()
        _a_flush_timer = None
    if not items:
        return
    if len(items) == 1:
        it = items[0]
        msg = (f"🟢 主力启动\n"
               f"时间:{it['t'].strftime('%Y-%m-%d %H:%M:%S')}\n"
               f"币种:{it['sym']}\n"
               f"当前价:{it['price']:.6f}\n"
               f"1M 涨幅:+{it['pct']:.2f}%\n"
               f"量能:{it['vol']:.2f}× 均量\n"
               f"判定:主力介入 / 吸筹")
    else:
        items.sort(key=lambda x: -x["pct"])
        lines = [f"🟢 主力启动 · 批量信号({len(items)} 个)",
                 f"时间:{items[0]['t'].strftime('%Y-%m-%d %H:%M:%S')}", ""]
        for it in items:
            lines.append(f"• {it['sym']:14s} {it['price']:.6f} +{it['pct']:.2f}% 量{it['vol']:.2f}×")
        msg = "\n".join(lines)
    notify(msg)

# ================= Binance REST =================
def rest_get(path, params=None, timeout=10):
    r = requests.get(BINANCE_API + path, params=params, timeout=timeout)
    r.raise_for_status()
    return r.json()

def get_symbols():
    j = rest_get("/fapi/v1/exchangeInfo")
    return [s["symbol"] for s in j["symbols"]
            if s.get("contractType") == "PERPETUAL"
            and s.get("quoteAsset") == "USDT"
            and s.get("status") == "TRADING"]

def get_klines(symbol, interval, limit):
    return rest_get("/fapi/v1/klines",
                    params={"symbol": symbol, "interval": interval, "limit": limit})

def get_24h_tickers():
    return rest_get("/fapi/v1/ticker/24hr")

def get_24h_one(symbol):
    try:
        return rest_get("/fapi/v1/ticker/24hr", params={"symbol": symbol}, timeout=5)
    except:
        return None

# ================= 状态机 =================
# 通用每币状态(用于日报)
def new_daily():
    return {
        "first_time": None,
        "first_price": None,
        "first_src": None,
        "second_time": None,
        "second_price": None,
        "third_time": None,
        "third_price": None,
        "high": 0.0,
        "low": 1e18,
        "progress_count": 0,  # 推进次数(B+C 累计)
    }

state_daily = defaultdict(new_daily)
state_daily_day = today_cn()
state_lock = threading.Lock()

def daily_update_price(symbol, price):
    """实时更新当日最高最低"""
    with state_lock:
        d = state_daily[symbol]
        if d["first_price"] is not None:
            if price > d["high"]:
                d["high"] = price
            if price < d["low"]:
                d["low"] = price

def daily_record_alert(symbol, price, src):
    """记录提醒事件,自动归类首次/二次/三次"""
    with state_lock:
        d = state_daily[symbol]
        t = now_cn()
        if d["first_price"] is None:
            d["first_time"] = t
            d["first_price"] = price
            d["first_src"] = src
            d["high"] = price
            d["low"] = price
        elif d["second_price"] is None:
            d["second_time"] = t
            d["second_price"] = price
        elif d["third_price"] is None:
            d["third_time"] = t
            d["third_price"] = price
        if src in ("B_PROG", "C_PROG"):
            d["progress_count"] += 1
        if price > d["high"]:
            d["high"] = price
        if price < d["low"]:
            d["low"] = price

# 系统 A:每币每日只播一次
state_a_played = set()  # 当日已播过的 symbol

# 系统 B
def new_b():
    return {"active": False, "last_high": None, "base_low": None,
            "push_count": 0, "daily_round": 0, "start_pct": 0.0,
            "last_progress_price": None, "last_progress_ts": 0, "day": None}
state_b = defaultdict(new_b)

# 系统 C
def new_c():
    return {"active": False, "last_high": None, "base_price": None,
            "push_count": 0, "daily_round": 0, "start_pct": 0.0,
            "last_progress_price": None, "last_progress_ts": 0, "day": None,
            "trigger_type": None}
state_c = defaultdict(new_c)

# 区间补漏:同币同区间一天只播一次
state_range_played = defaultdict(set)  # symbol -> {"30M","1H","4H"}

# ================= WebSocket 客户端 =================
class WSClient:
    """ 单连接订阅前 200 的 kline_15m + kline_3m。
        收到已收盘 K 线后回调 on_kline_closed(symbol, interval, kline_dict)。 """
    def __init__(self, on_kline_closed):
        self.ws = None
        self.thread = None
        self.subscribed = set()  # 已订阅的 stream 名
        self.want = set()  # 期望订阅
        self.lock = threading.Lock()
        self.on_kline_closed = on_kline_closed
        self.req_id = 0
        self.alive = False

    def _streams_for(self, symbols):
        s = set()
        for sym in symbols:
            low = sym.lower()
            s.add(f"{low}@kline_15m")
            s.add(f"{low}@kline_3m")
        return s

    def set_top_symbols(self, symbols):
        """更新前 200 名单,自动 subscribe / unsubscribe 差量"""
        new_want = self._streams_for(symbols)
        with self.lock:
            self.want = new_want
        if self.alive:
            self._sync_subscriptions()

    def _sync_subscriptions(self):
        with self.lock:
            to_add = list(self.want - self.subscribed)
            to_del = list(self.subscribed - self.want)

        def chunks(lst, n=100):
            for i in range(0, len(lst), n):
                yield lst[i:i+n]

        try:
            for batch in chunks(to_add, 100):
                self.req_id += 1
                self.ws.send(json.dumps({"method":"SUBSCRIBE","params":batch,"id":self.req_id}))
                with self.lock:
                    self.subscribed.update(batch)
                time.sleep(0.2)
            for batch in chunks(to_del, 100):
                self.req_id += 1
                self.ws.send(json.dumps({"method":"UNSUBSCRIBE","params":batch,"id":self.req_id}))
                with self.lock:
                    self.subscribed.difference_update(batch)
                time.sleep(0.2)
        except Exception as e:
            print(f"[WS SUB ERROR] {e}")

    def _on_open(self, ws):
        print("[WS] connected")
        self.alive = True
        self._sync_subscriptions()

    def _on_close(self, ws, code, msg):
        print(f"[WS] closed code={code} msg={msg}")
        self.alive = False

    def _on_error(self, ws, err):
        print(f"[WS] error {err}")

    def _on_message(self, ws, raw):
        try:
            data = json.loads(raw)
            if "stream" not in data:
                return
            d = data["data"]
            if d.get("e") != "kline":
                return
            k = d["k"]
            if not k.get("x"):
                return  # 只吃已收盘
            sym = d["s"]
            interval = k["i"]
            kd = {
                "t": k["t"], "T": k["T"],
                "o": float(k["o"]), "h": float(k["h"]),
                "l": float(k["l"]), "c": float(k["c"]),
                "v": float(k["v"]), "q": float(k["q"]),
                "n": k["n"]
            }
            try:
                self.on_kline_closed(sym, interval, kd)
            except Exception as e:
                print(f"[WS CB ERROR] {sym} {interval} {e}")
        except Exception as e:
            print(f"[WS MSG ERROR] {e}")

    def _run(self):
        while True:
            try:
                self.ws = websocket.WebSocketApp(
                    BINANCE_WS,
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close,
                )
                self.subscribed.clear()
                self.ws.run_forever(ping_interval=180, ping_timeout=10)
            except Exception as e:
                print(f"[WS RUN ERROR] {e}")
                self.alive = False
            print("[WS] reconnecting in 5s...")
            time.sleep(5)

    def start(self):
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

# ================= 工具:EMA =================
def ema(values, period):
    if len(values) < period:
        return None
    k = 2 / (period + 1)
    e = sum(values[:period]) / period
    for v in values[period:]:
        e = v * k + e * (1 - k)
    return e

def ema_series(values, period):
    if len(values) < period:
        return [None] * len(values)
    out = [None] * (period - 1)
    seed = sum(values[:period]) / period
    out.append(seed)
    k = 2 / (period + 1)
    e = seed
    for v in values[period:]:
        e = v * k + e * (1 - k)
        out.append(e)
    return out

# ================= 系统 A(REST 1M) =================
def system_a_check(sym):
    try:
        k1 = get_klines(sym, "1m", 3)
        if len(k1) < 3:
            return
        o = float(k1[-1][1])
        c = float(k1[-1][4])
        v = float(k1[-1][5])
        v2 = float(k1[-2][5])
        v3 = float(k1[-3][5])
        prev_avg = (v2 + v3) / 2 if (v2 + v3) > 0 else 0
        if prev_avg <= 0:
            return
        pct = (c - o) / o * 100
        vol_mult = v / prev_avg
        daily_update_price(sym, c)
        if pct >= A_PCT and vol_mult >= A_VOL_MULT:
            if sym in state_a_played:
                return
            state_a_played.add(sym)
            daily_record_alert(sym, c, "A")
            notify_a_merge(sym, c, pct, vol_mult)
    except Exception:
        pass

# ================= 系统 B(REST 3M) =================
def system_b_check(sym):
    try:
        sb = state_b[sym]
        td = today_cn()
        if sb["day"] != td:
            sb.update(new_b())
            sb["day"] = td
        if sb["daily_round"] >= B_MAX_DAILY_ROUND and not sb["active"]:
            return
        k = get_klines(sym, "3m", 6)
        if len(k) < 4:
            return
        highs = [float(x[2]) for x in k]
        lows = [float(x[3]) for x in k]
        price = float(k[-1][4])
        daily_update_price(sym, price)

        if not sb["active"]:
            hh = highs[-3] < highs[-2] < highs[-1]
            base_low = min(lows[-3:])
            start_pct = (highs[-1] - base_low) / base_low * 100
            if hh and start_pct >= B_MIN_TOTAL_PCT:
                sb["active"] = True
                sb["last_high"] = highs[-1]
                sb["base_low"] = base_low
                sb["push_count"] = 1
                sb["daily_round"] += 1
                sb["start_pct"] = start_pct
                sb["last_progress_price"] = price
                sb["last_progress_ts"] = time.time()
                daily_record_alert(sym, price, "B_START")
                ch24 = (get_24h_one(sym) or {}).get("priceChangePercent", "0")
                notify(f"🟢 3M 拉盘启动({sb['daily_round']}/{B_MAX_DAILY_ROUND})\n"
                       f"时间:{now_cn():%Y-%m-%d %H:%M:%S}\n"
                       f"币种:{sym}\n当前价:{price:.6f}\n"
                       f"24h:{ch24}%\n启动涨幅:+{start_pct:.2f}%\n"
                       f"结构:3M HH×3")
        else:
            drawdown = (sb["last_high"] - lows[-1]) / sb["last_high"]
            if drawdown >= B_DRAWDOWN_FAIL:
                sb["active"] = False
                return
            if highs[-1] > sb["last_high"]:
                sb["last_high"] = highs[-1]
            # 有效推进:相对上次推进价 ≥ 4% 且间隔 ≥ 5 分钟
            if sb["last_progress_price"]:
                prog_pct = (price - sb["last_progress_price"]) / sb["last_progress_price"] * 100
                if (prog_pct >= B_PROGRESS_PCT and
                    time.time() - sb["last_progress_ts"] >= B_PROGRESS_INTERVAL and
                    sb["push_count"] < B_MAX_PUSH):
                    sb["push_count"] += 1
                    sb["last_progress_price"] = price
                    sb["last_progress_ts"] = time.time()
                    cur_pct = (price - sb["base_low"]) / sb["base_low"] * 100
                    daily_record_alert(sym, price, "B_PROG")
                    notify(f"🚀 3M 有效推进({sb['push_count']})\n"
                           f"时间:{now_cn():%Y-%m-%d %H:%M:%S}\n"
                           f"币种:{sym}\n当前价:{price:.6f}\n"
                           f"本次推进:+{prog_pct:.2f}%\n"
                           f"累计涨幅:+{cur_pct:.2f}%")
    except Exception:
        pass

# ================= 系统 C(WS 触发,15M+3M) =================
# C 触发用 WS 收盘事件;推进也用 WS 收盘事件触发判定
c_kline_cache_15m = defaultdict(lambda: deque(maxlen=120))  # symbol -> deque of close
c_kline_cache_3m = defaultdict(lambda: deque(maxlen=60))   # symbol -> deque of (h,l,c,v)

def c_evaluate(symbol, price):
    """C 系统触发与推进判定"""
    sc = state_c[symbol]
    td = today_cn()
    if sc["day"] != td:
        sc.update(new_c())
        sc["day"] = td
    if sc["daily_round"] >= C_MAX_DAILY_ROUND and not sc["active"]:
        return

    closes15 = list(c_kline_cache_15m[symbol])
    bars3 = list(c_kline_cache_3m[symbol])
    if len(closes15) < 100 or len(bars3) < 10:
        return

    # 24h 区间(用 15m 的 max/min 近似)
    recent15 = closes15[-96:] if len(closes15) >= 96 else closes15
    h24 = max(recent15)
    l24 = min(recent15)
    if h24 <= l24:
        return
    pos_ratio = (price - l24) / (h24 - l24)
    high_gap_pct = (h24 - price) / price * 100

    # 前置条件
    e7 = ema(closes15, 7)
    e25 = ema(closes15, 25)
    e99 = ema(closes15, 99)
    if not (e7 and e25 and e99):
        return
    bull = e7 > e25 > e99
    if not bull:
        return
    if pos_ratio < C_POS_MIN_RATIO:
        return
    if high_gap_pct < C_HIGH_GAP_PCT:
        return

    daily_update_price(symbol, price)

    # 触发判定
    if not sc["active"]:
        # 触发 a:3M EMA7 上穿 EMA25(过去 2 根内)+ 量 ≥ 5 根均量 2 倍
        closes3 = [b["c"] for b in bars3]
        vols3   = [b["v"] for b in bars3]
        es3_7  = ema_series(closes3, 7)
        es3_25 = ema_series(closes3, 25)
        trig_a = False
        if es3_7[-1] and es3_25[-1] and es3_7[-2] and es3_25[-2]:
            cross_now = es3_7[-1] > es3_25[-1]
            cross_prev = es3_7[-2] > es3_25[-2]
            cross_prev2 = (es3_7[-3] is not None and es3_25[-3] is not None
                           and es3_7[-3] > es3_25[-3])
            crossed = cross_now and (not cross_prev or not cross_prev2)
            avg5v = sum(vols3[-6:-1]) / 5 if len(vols3) >= 6 else 0
            if crossed and avg5v > 0 and vols3[-1] >= avg5v * C_TRIG_A_VOL_MULT:
                trig_a = True

        # 触发 b:15M 多头持续≥4 根 + 最近 3 根 3M 收盘递增 + 最近一根量 ≥ 均量 1.5 倍
        trig_b = False
        es15_7  = ema_series(closes15, 7)
        es15_25 = ema_series(closes15, 25)
        es15_99 = ema_series(closes15, 99)
        bull_count = 0
        for i in range(len(closes15)-1, -1, -1):
            if (es15_7[i] and es15_25[i] and es15_99[i]
                and es15_7[i] > es15_25[i] > es15_99[i]):
                bull_count += 1
            else:
                break
        if bull_count >= C_TRIG_B_BULL_BARS:
            closes3 = [b["c"] for b in bars3]
            vols3   = [b["v"] for b in bars3]
            if len(closes3) >= 4:
                rising = closes3[-3] < closes3[-2] < closes3[-1]
                avg_v = sum(vols3[-6:-1]) / 5 if len(vols3) >= 6 else 0
                if rising and avg_v > 0 and vols3[-1] >= avg_v * C_TRIG_B_VOL_MULT:
                    trig_b = True

        if trig_a or trig_b:
            sc["active"] = True
            sc["base_price"] = price
            sc["last_high"] = price
            sc["push_count"] = 1
            sc["daily_round"] += 1
            sc["last_progress_price"] = price
            sc["last_progress_ts"] = time.time()
            sc["trigger_type"] = "启动型(EMA上穿+爆量)" if trig_a else "推进型(多头持续+放量)"
            daily_record_alert(symbol, price, "C_START")
            notify(f"🌊 主升浪探测({sc['daily_round']}/{C_MAX_DAILY_ROUND})\n"
                   f"时间:{now_cn():%Y-%m-%d %H:%M:%S}\n"
                   f"币种:{symbol}\n当前价:{price:.6f}\n"
                   f"触发:{sc['trigger_type']}\n"
                   f"位置:24h 区间 {pos_ratio*100:.0f}%(距高 {high_gap_pct:.1f}%)\n"
                   f"15M 结构:多头排列")
    else:
        # 推进
        if price > sc["last_high"]:
            sc["last_high"] = price
        if sc["last_progress_price"]:
            prog_pct = (price - sc["last_progress_price"]) / sc["last_progress_price"] * 100
            if (prog_pct >= C_PROGRESS_PCT and
                time.time() - sc["last_progress_ts"] >= C_PROGRESS_INTERVAL and
                sc["push_count"] < C_MAX_PUSH):
                sc["push_count"] += 1
                sc["last_progress_price"] = price
                sc["last_progress_ts"] = time.time()
                cur_pct = (price - sc["base_price"]) / sc["base_price"] * 100
                daily_record_alert(symbol, price, "C_PROG")
                notify(f"🌊 主升浪推进({sc['push_count']})\n"
                       f"时间:{now_cn():%Y-%m-%d %H:%M:%S}\n"
                       f"币种:{symbol}\n当前价:{price:.6f}\n"
                       f"本次推进:+{prog_pct:.2f}%\n"
                       f"累计涨幅:+{cur_pct:.2f}%")

def on_ws_kline(symbol, interval, kd):
    """WS 收盘 K 线回调"""
    daily_update_price(symbol, kd["c"])
    if interval == "15m":
        c_kline_cache_15m[symbol].append(kd["c"])
        c_evaluate(symbol, kd["c"])
    elif interval == "3m":
        c_kline_cache_3m[symbol].append({"h":kd["h"],"l":kd["l"],"c":kd["c"],"v":kd["v"]})
        c_evaluate(symbol, kd["c"])

# ================= 区间补漏(REST) =================
def range_check(sym):
    try:
        # 拉一次 15m,够覆盖到 4H(16 根)
        k = get_klines(sym, "15m", 20)
        if len(k) < 17:
            return
        closes = [float(x[4]) for x in k]
        price = closes[-1]
        daily_update_price(sym, price)
        # 30M(过去 2 根 15m)、1H(过去 4 根)、4H(过去 16 根)
        for label, mins, threshold in RANGE_THRESHOLDS:
            n = mins // 15
            if len(closes) <= n:
                continue
            base = closes[-n-1]
            pct = (price - base) / base * 100
            if pct >= threshold:
                if label not in state_range_played[sym]:
                    state_range_played[sym].add(label)
                    daily_record_alert(sym, price, f"RANGE_{label}")
                    notify(f"📈 区间补漏 · {label} 累计 +{pct:.2f}%\n"
                           f"时间:{now_cn():%Y-%m-%d %H:%M:%S}\n"
                           f"币种:{sym}\n当前价:{price:.6f}\n"
                           f"说明:已在拉升中")
    except Exception:
        pass

# ================= 启动回扫 =================
def boot_rescan(symbols):
    """启动时扫描:已经在涨势中(过去4H≥8%)的币直接初始化进 C 推进"""
    print(f"[BOOT] 启动回扫 {len(symbols)} 个币...")
    cnt = 0
    for sym in symbols:
        try:
            k = get_klines(sym, "15m", 20)
            if len(k) < 17:
                continue
            closes = [float(x[4]) for x in k]
            base = closes[-17]
            price = closes[-1]
            pct = (price - base) / base * 100
            if pct >= BOOT_RISE_PCT:
                # 灌入 cache 让 C 系统能用
                for c in closes:
                    c_kline_cache_15m[sym].append(c)
                # 直接当作 C 已激活
                sc = state_c[sym]
                sc["active"] = True
                sc["base_price"] = base
                sc["last_high"] = price
                sc["push_count"] = 1
                sc["daily_round"] = 1
                sc["last_progress_price"] = price
                sc["last_progress_ts"] = time.time()
                sc["day"] = today_cn()
                sc["trigger_type"] = "启动回扫(已在涨势)"
                daily_record_alert(sym, price, "C_BOOT")
                cnt += 1
                notify(f"🌊 启动回扫 · 已在涨势\n"
                       f"币种:{sym}\n当前价:{price:.6f}\n"
                       f"过去4H累计:+{pct:.2f}%\n"
                       f"已纳入主升浪推进监控")
        except:
            pass
    print(f"[BOOT] 回扫完成,纳入 {cnt} 个币进 C 推进")

# ================= 前 200 名单管理 =================
top_symbols = set()
top_lock = threading.Lock()
all_symbols = []

def refresh_top(ws_client):
    """每小时重排前 200,并保留'还在监控中'的币继续 WS"""
    global top_symbols
    try:
        tickers = get_24h_tickers()
        usdt_set = set(all_symbols)
        rows = [t for t in tickers if t["symbol"] in usdt_set]
        rows.sort(key=lambda x: float(x.get("quoteVolume", 0) or 0), reverse=True)
        new_top = set(t["symbol"] for t in rows[:TOP_N])

        # 保留还在监控的币(B 或 C 激活)
        keep = set()
        for sym in usdt_set:
            if state_b[sym]["active"]:
                keep.add(sym)
            if state_c[sym]["active"]:
                keep.add(sym)
        final = new_top | keep
        with top_lock:
            top_symbols = final
        ws_client.set_top_symbols(final)
        print(f"[TOP] 刷新前 {TOP_N},合并监控中后 {len(final)} 个币订阅 WS")
    except Exception as e:
        print(f"[TOP REFRESH ERROR] {e}")

# ================= 日报 =================
def reset_daily_states():
    global state_a_played, state_range_played
    with state_lock:
        state_daily.clear()
    state_a_played = set()
    state_range_played = defaultdict(set)
    for sym in list(state_b.keys()):
        state_b[sym].update(new_b())
    for sym in list(state_c.keys()):
        state_c[sym].update(new_c())
    print("[DAILY] 当日状态已重置")

def generate_report():
    """生成日报 Excel 并邮件发送"""
    try:
        with state_lock:
            data = {sym: dict(d) for sym, d in state_daily.items() if d["first_price"] is not None}
        if not data:
            print("[REPORT] 今日无信号,跳过")
            return
        rows = []
        for sym, d in data.items():
            fp = d["first_price"]
            high = d["high"] or fp
            low = d["low"] if d["low"] < 1e17 else fp
            cur_high_pct = (high - fp) / fp * 100
            drawdown = (high - low) / high * 100 if high > 0 else 0

            def fmt(t):
                return t.strftime("%H:%M:%S") if t else ""

            rows.append({
                "币种": sym,
                "首次时间": fmt(d["first_time"]),
                "首次价": fp,
                "首次来源": d["first_src"],
                "二次时间": fmt(d["second_time"]),
                "二次价": d["second_price"] or "",
                "三次时间": fmt(d["third_time"]),
                "三次价": d["third_price"] or "",
                "当日最高": high,
                "当日最低": low,
                "首次→最高(%)": round(cur_high_pct, 2),
                "最大回撤(%)": round(drawdown, 2),
                "推进次数": d["progress_count"],
            })
        df = pd.DataFrame(rows)
        df.sort_values("首次→最高(%)", ascending=False, inplace=True)

        # 汇总
        summary = pd.DataFrame([{
            "今日触发币数": len(df),
            "平均首次→最高(%)": round(df["首次→最高(%)"].mean(), 2),
            "命中率(≥5%) (%)": round((df["首次→最高(%)"] >= 5).mean() * 100, 2),
            "最大单币涨幅(%)": round(df["首次→最高(%)"].max(), 2),
        }])
        top10 = df.head(10)
        bot10 = df.tail(10)

        fname = f"日报_{today_cn().strftime('%Y%m%d')}.xlsx"
        with pd.ExcelWriter(fname, engine="openpyxl") as w:
            df.to_excel(w, sheet_name="明细", index=False)
            summary.to_excel(w, sheet_name="汇总", index=False)
            top10.to_excel(w, sheet_name="Top10", index=False)
            bot10.to_excel(w, sheet_name="Bottom10", index=False)
        send_email_attachment(f"监控日报 {today_cn()}",
                              f"附件为 {today_cn()} 信号日报,共 {len(df)} 个币种触发。", fname)
        print(f"[REPORT] 日报已发送: {fname}")
    except Exception as e:
        print(f"[REPORT ERROR] {e}")

def daily_scheduler():
    """日报独立线程:每天 00:00:05 触发,触发后睡到下一天"""
    while True:
        n = now_cn()
        nxt = (n + timedelta(days=1)).replace(hour=0, minute=0, second=5, microsecond=0)
        sleep_sec = (nxt - n).total_seconds()
        time.sleep(max(1, sleep_sec))
        try:
            generate_report()
        except Exception as e:
            print(f"[DAILY GEN ERROR] {e}")
        try:
            reset_daily_states()
        except Exception as e:
            print(f"[DAILY RESET ERROR] {e}")

# ================= SQLite(简易持久化) =================
def db_init():
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS alerts(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT,
                    symbol TEXT,
                    src TEXT,
                    price REAL,
                    msg TEXT)""")
    conn.commit()
    conn.close()

def db_log(symbol, src, price, msg):
    try:
        conn = sqlite3.connect(DB_FILE)
        conn.execute("INSERT INTO alerts(ts,symbol,src,price,msg) VALUES(?,?,?,?,?)",
                     (now_cn().isoformat(), symbol, src, price, msg))
        conn.commit()
        conn.close()
    except:
        pass

# 把 db_log 接到 daily_record_alert 后面(简单包一层)
_orig_record = daily_record_alert
def daily_record_alert_logged(symbol, price, src):
    _orig_record(symbol, price, src)
    db_log(symbol, src, price, src)
daily_record_alert = daily_record_alert_logged

# ================= 主循环 =================
def main():
    global all_symbols
    db_init()
    all_symbols = get_symbols()
    print(f"[INIT] 共 {len(all_symbols)} 个 USDT 永续合约")
    notify(f"✅ 监控启动,共 {len(all_symbols)} 个 USDT 永续合约")

    # 启动 WS
    ws = WSClient(on_kline_closed=on_ws_kline)
    ws.start()
    time.sleep(2)
    refresh_top(ws)

    # 启动回扫
    with top_lock:
        boot_list = list(top_symbols)[:TOP_N]
    boot_rescan(boot_list)

    # 启动日报线程
    threading.Thread(target=daily_scheduler, daemon=True).start()

    last_top_refresh = time.time()
    last_day = today_cn()

    while True:
        try:
            # 跨日保险
            if today_cn() != last_day:
                last_day = today_cn()

            # 每小时刷新前 200
            if time.time() - last_top_refresh >= TOP_REFRESH_SEC:
                refresh_top(ws)
                last_top_refresh = time.time()

            # 全市场噪音判断
            try:
                tickers = get_24h_tickers()
                usdt = [t for t in tickers if t["symbol"] in set(all_symbols)]
                up = sum(1 for t in usdt if float(t.get("priceChangePercent", 0)) >= NOISE_PCT)
                if usdt and up / len(usdt) >= NOISE_RATIO:
                    print(f"[NOISE] 全市场普涨 {up}/{len(usdt)},跳过本轮")
                    time.sleep(SCAN_INTERVAL)
                    continue
            except:
                pass

            # REST 扫描:A、B、补漏
            for sym in all_symbols:
                system_a_check(sym)
                system_b_check(sym)
                range_check(sym)

            time.sleep(SCAN_INTERVAL)
        except KeyboardInterrupt:
            print("[EXIT] 手动退出")
            break
        except Exception as e:
            print(f"[MAIN ERROR] {e}")
            time.sleep(10)

if __name__ == "__main__":
    main()