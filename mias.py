# -*- coding: utf-8 -*-
# 系统 B（3M HH 拉盘策略）+ 多连接 WebSocket + 日报 + TG/邮件通知
# 适用于香港服务器（网络畅通）

import time
import requests
import smtplib
import pandas as pd
import json
import threading
import websocket
import math
from collections import defaultdict
from datetime import datetime, timezone, timedelta, date
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.header import Header
from email.utils import formataddr
import os

# ================= 基础配置 =================
BINANCE_FAPI = "https://fapi.binance.com"
BINANCE_WS = "wss://fstream.binance.com/stream"

SCAN_INTERVAL = 30
WS_MAX_STREAMS = 200          # 每个连接最多200个流
WS_HEARTBEAT_TIMEOUT = 45

# ================= 系统 B 参数 =================
HH_MIN_TOTAL_PCT = 2.0
DRAWDOWN_FAIL = 0.07
MAX_PUSH = 3
MAX_DAILY_ROUND = 2

# ================= 全市场噪音过滤 =================
MARKET_NOISE_PCT = 1.2
MARKET_NOISE_RATIO = 0.6

# ================= 通知配置 =================
BOT_TOKEN = "8557301222:AAHj1rSQ63zJGFXVxxuTniwRP2Y1tj3QsAs"
CHAT_ID = "5408890841"

EMAIL_USER = "1113496210@qq.com"
EMAIL_PASS = "hzshvazrbnyzfhdf"
EMAIL_TO = "1113496210@qq.com"

# ================= 全局变量 =================
symbols = []
symbol_groups = []
group_data_status = []
group_last_msg_time = []
group_ws_clients = []
group_reconnect_threads = []
current_prices = {}
use_ws_for_symbol = defaultdict(lambda: False)

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
})

# ================= 通知函数 =================
def send_tg(text):
    try:
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        requests.post(url, json={"chat_id": CHAT_ID, "text": text}, timeout=10)
    except:
        pass

def send_email_with_text(subject, content):
    try:
        msg = MIMEText(content, "plain", "utf-8")
        msg["From"] = formataddr(("盘面监控", EMAIL_USER))
        msg["To"] = EMAIL_TO
        msg["Subject"] = Header(subject, "utf-8")
        server = smtplib.SMTP_SSL("smtp.qq.com", 465)
        server.login(EMAIL_USER, EMAIL_PASS)
        server.sendmail(EMAIL_USER, [EMAIL_TO], msg.as_string())
        server.quit()
    except:
        pass

def send_email_with_attachment(subject, body, filepath):
    try:
        msg = MIMEMultipart()
        msg["From"] = formataddr(("盘面监控", EMAIL_USER))
        msg["To"] = EMAIL_TO
        msg["Subject"] = Header(subject, "utf-8")
        msg.attach(MIMEText(body, "plain", "utf-8"))
        with open(filepath, "rb") as f:
            part = MIMEApplication(f.read(), Name=os.path.basename(filepath))
        part['Content-Disposition'] = f'attachment; filename="{os.path.basename(filepath)}"'
        msg.attach(part)
        server = smtplib.SMTP_SSL("smtp.qq.com", 465)
        server.login(EMAIL_USER, EMAIL_PASS)
        server.sendmail(EMAIL_USER, [EMAIL_TO], msg.as_string())
        server.quit()
    except:
        pass

def notify_all(text):
    send_tg(text)
    send_email_with_text("实时监控提示", text)

# ================= REST API =================
def get_symbols():
    r = requests.get(f"{BINANCE_FAPI}/fapi/v1/exchangeInfo", timeout=10).json()
    return [
        s["symbol"] for s in r["symbols"]
        if s["contractType"] == "PERPETUAL"
        and s["quoteAsset"] == "USDT"
        and s["status"] == "TRADING"
    ]

def get_klines_rest(symbol, interval, limit):
    r = requests.get(
        f"{BINANCE_FAPI}/fapi/v1/klines",
        params={"symbol": symbol, "interval": interval, "limit": limit},
        timeout=10
    )
    r.raise_for_status()
    return r.json()

def get_all_tickers_24hr():
    try:
        r = requests.get(f"{BINANCE_FAPI}/fapi/v1/ticker/24hr", timeout=10)
        r.raise_for_status()
        data = r.json()
        return {item["symbol"]: float(item["priceChangePercent"]) for item in data}
    except:
        return {}

# ================= WebSocket 多分组管理 =================
def on_ws_message(ws, message, group_idx):
    global group_last_msg_time, current_prices
    group_last_msg_time[group_idx] = time.time()
    try:
        data = json.loads(message)
        if "data" not in data:
            return
        stream = data.get("stream", "")
        if not stream.endswith("@kline_3m"):
            return
        k = data["data"]["k"]
        symbol = data["data"]["s"]
        current_prices[symbol] = float(k["c"])
    except:
        pass

def on_ws_error(ws, error, group_idx):
    print(f"Group {group_idx} WebSocket error: {error}")

def on_ws_close(ws, close_status_code, close_msg, group_idx):
    print(f"Group {group_idx} WebSocket closed")
    group_data_status[group_idx] = False
    for sym in symbol_groups[group_idx]:
        use_ws_for_symbol[sym] = False
    # 启动重连
    if not any(t.is_alive() for t in group_reconnect_threads if hasattr(t, '_args') and t._args[0] == group_idx):
        t = threading.Thread(target=reconnect_websocket, args=(group_idx,), daemon=True)
        t.start()
        group_reconnect_threads.append(t)

def on_ws_open(ws, group_idx):
    streams = [f"{sym.lower()}@kline_3m" for sym in symbol_groups[group_idx]]
    subscribe_msg = {
        "method": "SUBSCRIBE",
        "params": streams,
        "id": 1
    }
    ws.send(json.dumps(subscribe_msg))
    group_data_status[group_idx] = True
    for sym in symbol_groups[group_idx]:
        use_ws_for_symbol[sym] = True
    print(f"Group {group_idx} subscribed to {len(streams)} streams")

def start_websocket_for_group(group_idx):
    ws = websocket.WebSocketApp(
        BINANCE_WS,
        on_open=lambda ws: on_ws_open(ws, group_idx),
        on_message=lambda ws, msg: on_ws_message(ws, msg, group_idx),
        on_error=lambda ws, err: on_ws_error(ws, err, group_idx),
        on_close=lambda ws, code, msg: on_ws_close(ws, code, msg, group_idx)
    )
    group_ws_clients.append(ws)
    ws.run_forever()

def reconnect_websocket(group_idx):
    delay = 5
    while not group_data_status[group_idx]:
        print(f"Reconnecting group {group_idx} in {delay}s...")
        time.sleep(delay)
        try:
            new_ws = websocket.WebSocketApp(
                BINANCE_WS,
                on_open=lambda ws: on_ws_open(ws, group_idx),
                on_message=lambda ws, msg: on_ws_message(ws, msg, group_idx),
                on_error=lambda ws, err: on_ws_error(ws, err, group_idx),
                on_close=lambda ws, code, msg: on_ws_close(ws, code, msg, group_idx)
            )
            t = threading.Thread(target=new_ws.run_forever, daemon=True)
            t.start()
            time.sleep(5)
            if group_data_status[group_idx]:
                print(f"Group {group_idx} reconnected")
                group_ws_clients[group_idx] = new_ws
                break
            else:
                delay = min(delay * 2, 120)
        except Exception as e:
            print(f"Reconnect error: {e}")
            delay = min(delay * 2, 120)

# ================= 市场数据获取 =================
def get_market_data(symbol):
    klines = get_klines_rest(symbol, "3m", 6)
    if use_ws_for_symbol.get(symbol, False) and symbol in current_prices:
        price = current_prices[symbol]
    else:
        price = float(klines[-1][4])
    return klines, price

# ================= 日报 =================
def generate_daily_report():
    rows = []
    for sym, st in state_b.items():
        if st.get("first_price") is None:
            continue
        high = st.get("daily_high", 0.0)
        low = st.get("daily_low", 0.0)
        first = st.get("first_price")
        push_times = st.get("push_times", 0)
        if high == 0.0:
            high = first
        if low == 0.0:
            low = first
        daily_pct = (high - first) / first * 100
        rows.append({
            "币种": sym,
            "首次提示价": round(first, 6),
            "当日涨幅(%)": round(daily_pct, 2),
            "当日最高": round(high, 6),
            "当日最低": round(low, 6),
            "推送次数": push_times
        })
    if rows:
        df = pd.DataFrame(rows)
        df.sort_values(by="当日涨幅(%)", ascending=False, inplace=True)
        filename = f"系统B日报_{date.today().strftime('%Y%m%d')}.xlsx"
        df.to_excel(filename, index=False)
        send_email_with_attachment(f"系统B日报 {date.today()}", "附件为系统B日报", filename)

def reset_daily_states():
    for st in state_b.values():
        st["daily_high"] = 0.0
        st["daily_low"] = 1e10
        st["push_times"] = 0
        st["first_price"] = None

# ================= 市场噪音 =================
def is_market_noisy(tickers_24h):
    noisy_count = sum(1 for pct in tickers_24h.values() if pct >= MARKET_NOISE_PCT)
    ratio = noisy_count / len(tickers_24h) if tickers_24h else 0
    return ratio >= MARKET_NOISE_RATIO

# ================= 系统 B 核心逻辑 =================
def process_symbol(sym, tickers_24h):
    try:
        sb = state_b[sym]
        today = date.today()
        if sb["day"] != today:
            sb["day"] = today
            sb["daily_round"] = 0
            sb["active"] = False
            sb["push_count"] = 0
        if sb["daily_round"] >= MAX_DAILY_ROUND:
            return

        klines, price_now = get_market_data(sym)
        highs = [float(x[2]) for x in klines]
        lows = [float(x[3]) for x in klines]
        change_24h = tickers_24h.get(sym, 0.0)

        if not sb["active"]:
            if len(highs) < 3:
                return
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
                sb["push_times"] = 1
                msg = f"🟢 3M 拉盘启动（1）\n时间：{datetime.now().strftime('%Y-%m-%d %H:%M')}\n币种：{sym}\n当前价格：{price_now:.6f}\n24h涨跌：{change_24h:+.2f}%\n启动涨幅：+{start_pct:.2f}%"
                notify_all(msg)
        else:
            drawdown = (sb["last_high"] - lows[-1]) / sb["last_high"]
            if drawdown >= DRAWDOWN_FAIL:
                sb["active"] = False
                return
            if highs[-1] > sb["last_high"] and sb["push_count"] < MAX_PUSH:
                sb["last_high"] = highs[-1]
                sb["push_count"] += 1
                current_pct = (sb["last_high"] - sb["base_low"]) / sb["base_low"] * 100
                sb["daily_high"] = max(sb["daily_high"], price_now)
                sb["daily_low"] = min(sb["daily_low"], price_now)
                sb["push_times"] += 1
                msg = f"🚀 3M 拉盘推进（{sb['push_count']}）\n时间：{datetime.now().strftime('%Y-%m-%d %H:%M')}\n币种：{sym}\n当前价格：{price_now:.6f}\n累计涨幅：+{current_pct:.2f}%"
                notify_all(msg)
            sb["daily_high"] = max(sb["daily_high"], price_now)
            sb["daily_low"] = min(sb["daily_low"], price_now)
    except Exception as e:
        pass

# ================= 主函数 =================
def main():
    global symbols, symbol_groups, group_data_status, group_last_msg_time

    print("正在获取币种列表...")
    symbols = get_symbols()
    print(f"获取到 {len(symbols)} 个 USDT 永续合约")
    notify_all(f"✅ 系统B（多连接WebSocket）启动 | 监控 {len(symbols)} 个币种")

    # 分组
    group_size = WS_MAX_STREAMS
    num_groups = math.ceil(len(symbols) / group_size)
    for i in range(num_groups):
        start = i * group_size
        end = start + group_size
        group = symbols[start:end]
        symbol_groups.append(group)
        group_data_status.append(False)
        group_last_msg_time.append(0.0)

    # 启动 WebSocket 线程
    for idx in range(num_groups):
        t = threading.Thread(target=start_websocket_for_group, args=(idx,), daemon=True)
        t.start()

    # 等待连接
    time.sleep(10)
    for idx, status in enumerate(group_data_status):
        print(f"Group {idx}: {'Connected' if status else 'Disconnected (REST backup)'}")

    last_daily_reset = date.today()
    last_ticker_fetch = 0
    tickers_cache = {}

    while True:
        try:
            now = datetime.now(timezone(timedelta(hours=8)))
            today = now.date()

            if now.hour == 0 and now.minute == 0 and today != last_daily_reset:
                generate_daily_report()
                reset_daily_states()
                last_daily_reset = today
                time.sleep(60)

            if time.time() - last_ticker_fetch > 300 or not tickers_cache:
                tickers_cache = get_all_tickers_24hr()
                last_ticker_fetch = time.time()
                if is_market_noisy(tickers_cache):
                    print(f"{now} 市场噪音过高，跳过扫描")
                    time.sleep(SCAN_INTERVAL)
                    continue

            for sym in symbols:
                process_symbol(sym, tickers_cache)

            ws_covered = sum(1 for sym in symbols if use_ws_for_symbol.get(sym, False))
            active_signals = sum(1 for s in state_b.values() if s['active'])
            print(f"{now} | WS覆盖: {ws_covered}/{len(symbols)} | 活跃: {active_signals}")

            time.sleep(SCAN_INTERVAL)

        except Exception as e:
            print("主循环异常:", e)
            time.sleep(5)

if __name__ == "__main__":
    main()