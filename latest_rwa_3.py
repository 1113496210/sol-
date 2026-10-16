# -*- coding: utf-8 -*-
# =====================================================
# Binance USDT 永续合约 · AB 系统 + 15M 山寨币观察板块整合 + 日报 + 邮件/TG即时通知
# =====================================================

import time
import requests
import smtplib
import pandas as pd
from collections import defaultdict
from datetime import datetime, timezone, timedelta, date
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.header import Header
from email.utils import formataddr
import os

# ================= 基础配置 =================
BINANCE_API = "https://fapi.binance.com"
SCAN_INTERVAL = 60

# ================= 系统 A 参数 =================
FIRST_TRIGGER = 1.8
SECOND_TRIGGER = 3.8
VOLUME_MULTIPLIER_1 = 1.5
VOLUME_MULTIPLIER_2 = 1.2
COOLDOWN = 180

# ================= 系统 B 参数 =================
HH_INIT_BARS = 3
HH_MIN_TOTAL_PCT = 2
DRAWDOWN_FAIL = 0.07
MAX_PUSH = 3
MAX_DAILY_ROUND = 2

# ================= 全市场噪音 =================
MARKET_NOISE_PCT = 1.2
MARKET_NOISE_RATIO = 0.6

# ================= 通知配置 =================
BOT_TOKEN = "7874342652:AAFQKWIrSVszpi1z60ixnr-VYpXf26rG8UY"
CHAT_ID = "5408890841"

EMAIL_USER = "1113496210@qq.com"
EMAIL_PASS = "gnlxwgxduzexgeag"
EMAIL_TO = "1113496210@qq.com"

# ================= 观察板块参数 =================
OBSERVE_MAX_PUSH = 3
OBSERVE_INTERVAL_MIN = 15
OBSERVE_WINDOW = 5  # 最近几根15M K线用于计算资金流
ALT_BLACKLIST = ["BTCUSDT","ETHUSDT"]  # 排除主流币

# ================= 通知模块 =================
def send_tg(text):
    try:
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        requests.post(url, json={"chat_id": CHAT_ID, "text": text}, timeout=10)
    except:
        pass

def send_email_with_text(subject, content):
    """发送即时文本邮件"""
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
    """即时提示：同时发送 TG + 邮箱"""
    send_tg(text)
    send_email_with_text("实时监控提示", text)

# ================= Binance 工具 =================
def get_symbols():
    r = requests.get(f"{BINANCE_API}/fapi/v1/exchangeInfo", timeout=10).json()
    return [
        s["symbol"] for s in r["symbols"]
        if s["contractType"] == "PERPETUAL"
        and s["quoteAsset"] == "USDT"
        and s["status"] == "TRADING"
    ]

def get_klines(symbol, interval, limit):
    r = requests.get(
        f"{BINANCE_API}/fapi/v1/klines",
        params={"symbol": symbol, "interval": interval, "limit": limit},
        timeout=10
    )
    r.raise_for_status()
    return r.json()

def get_trades(symbol, limit=100):
    try:
        r = requests.get(f"{BINANCE_API}/fapi/v1/trades", params={"symbol": symbol, "limit": limit}, timeout=5)
        r.raise_for_status()
        return r.json()
    except:
        return []

def get_position_risk(symbol):
    try:
        r = requests.get(f"{BINANCE_API}/fapi/v2/positionRisk", params={"symbol": symbol}, timeout=5)
        r.raise_for_status()
        return r.json()
    except:
        return []

def get_24h_change(symbol):
    try:
        r = requests.get(f"{BINANCE_API}/fapi/v1/ticker/24hr", params={"symbol": symbol}, timeout=5).json()
        return float(r["priceChangePercent"])
    except:
        return 0.0

# ================= 状态缓存 =================
state_a = defaultdict(lambda: {
    "last_alert": 0,
    "first_price": None,
    "second_done": False,
    "daily_high": 0.0,
    "daily_low": 1e10,
    "push_times": 0
})

state_b = defaultdict(lambda: {
    "active": False,
    "last_high": None,
    "base_low": None,
    "push_count": 0,
    "day": None,
    "daily_round": 0,
    "start_pct": 0.0,
    "daily_high": 0.0,
    "daily_low": 1e10,
    "push_times": 0,
})

state_observe = defaultdict(lambda: {
    "last_push_date": None,
    "first_time": None,
    "first_price": None,
    "progression_count": 0,
    "capital_flow": 0.0,
    "longshort_ratio": 0.0,
    "volatility_15m": 0.0,
    "state": "INIT",
    "daily_high": 0.0,
    "daily_low": 1e10,
    "push_times": 0
})

# ================= 启动 =================
symbols = get_symbols()
notify_all(f"✅ AB 系统 + 观察板块启动，共 {len(symbols)} 个 USDT 永续合约")

# ================= 日报函数 =================
def generate_daily_report(states, report_name):
    rows = []
    for sym, st in states.items():
        if st.get("first_price") is None:
            continue
        high = st.get("daily_high", 0.0)
        low = st.get("daily_low", 0.0)
        first_price = st.get("first_price")
        push_times = st.get("push_times",0)
        if high==0.0: high = first_price
        if low==0.0: low = first_price
        daily_pct = (high - first_price)/first_price*100
        rows.append({
            "币种": sym,
            "首次提示价": first_price,
            "当日涨幅(%)": round(daily_pct,2),
            "当日最高": high,
            "当日最低": low,
            "推送次数": push_times
        })
    if not rows:
        return
    df = pd.DataFrame(rows)
    df.sort_values(by="当日涨幅(%)", ascending=False, inplace=True)
    filename = f"{report_name}_{date.today().strftime('%Y%m%d')}.xlsx"
    df.to_excel(filename, index=False)
    # 邮件发送
    send_email_with_attachment(f"{report_name}日报 {date.today()}", f"附件为{report_name}日报", filename)

# ================= 主循环 =================
while True:
    try:
        now_cn = datetime.now(timezone(timedelta(hours=8)))
        today = date.today()

        # ===== 每日 0 点触发日报 =====
        if now_cn.hour==0 and now_cn.minute==0:
            generate_daily_report(state_a, "AB系统")
            generate_daily_report(state_b, "AB系统")
            generate_daily_report(state_observe, "观察板块")
            # 重置每日数据
            for st in list(state_a.values())+list(state_b.values())+list(state_observe.values()):
                st["daily_high"] = 0.0
                st["daily_low"] = 1e10
                st["push_times"] = 0
            time.sleep(60)

        # ===== 全市场噪音判断 =====
        up_count = 0
        for sym in symbols[:50]:
            try:
                k = get_klines(sym, "1m", 2)
                o = float(k[-1][1])
                c = float(k[-1][4])
                if (c - o)/o*100 >= MARKET_NOISE_PCT:
                    up_count += 1
            except:
                pass
        if up_count/50 >= MARKET_NOISE_RATIO:
            time.sleep(SCAN_INTERVAL)
            continue

        for sym in symbols:
            # ================= 系统 A =================
            try:
                k1 = get_klines(sym,"1m",3)
                o = float(k1[-1][1])
                c = float(k1[-1][4])
                pct = (c-o)/o*100
                vol_now = float(k1[-1][5])
                vol_avg = sum(float(x[5]) for x in k1[:-1])/2
                sa = state_a[sym]
                now_ts = time.time()
                if pct>=FIRST_TRIGGER and vol_now>=vol_avg*VOLUME_MULTIPLIER_1:
                    if now_ts - sa["last_alert"]>COOLDOWN and not sa["first_price"]:
                        sa["first_price"] = c
                        sa["last_alert"] = now_ts
                        sa["daily_high"] = c
                        sa["daily_low"] = c
                        sa["push_times"] = 1
                        msg = f"🟢 主力启动\n时间：{now_cn.strftime('%Y-%m-%d %H:%M')}\n币种：{sym}\n当前价格：{c:.6f}\n1M 涨幅：+{pct:.2f}%\n成交量：明显放大\n判定：主力介入 / 吸筹"
                        notify_all(msg)
                if sa["first_price"] and not sa["second_done"]:
                    total_pct = (c - sa["first_price"])/sa["first_price"]*100
                    if total_pct>=SECOND_TRIGGER and vol_now>=vol_avg*VOLUME_MULTIPLIER_2:
                        sa["second_done"] = True
                        sa["daily_high"] = max(sa["daily_high"], c)
                        sa["daily_low"] = min(sa["daily_low"], c)
                        sa["push_times"] +=1
                        msg = f"🔥 二次启动\n时间：{now_cn.strftime('%Y-%m-%d %H:%M')}\n币种：{sym}\n当前价格：{c:.6f}\n累计涨幅：+{total_pct:.2f}%"
                        notify_all(msg)
                sa["daily_high"] = max(sa["daily_high"], c)
                sa["daily_low"] = min(sa["daily_low"], c)
            except:
                pass

            # ================= 系统 B =================
            try:
                sb = state_b[sym]
                if sb["day"] != today:
                    sb["day"] = today
                    sb["daily_round"] = 0
                    sb["active"] = False
                    sb["push_count"] = 0
                if sb["daily_round"] >= MAX_DAILY_ROUND:
                    continue
                k3 = get_klines(sym,"3m",6)
                highs = [float(x[2]) for x in k3]
                lows = [float(x[3]) for x in k3]
                price_now = float(k3[-1][4])
                change_24h = get_24h_change(sym)
                if not sb["active"]:
                    hh = highs[-3]<highs[-2]<highs[-1]
                    start_pct = (highs[-1]-lows[-3])/lows[-3]*100
                    if hh and start_pct>=HH_MIN_TOTAL_PCT:
                        sb["active"]=True
                        sb["last_high"]=highs[-1]
                        sb["base_low"]=lows[-3]
                        sb["push_count"]=1
                        sb["daily_round"]+=1
                        sb["start_pct"]=start_pct
                        sb["daily_high"]=price_now
                        sb["daily_low"]=price_now
                        sb["push_times"]=1
                        msg = f"🟢 3M 拉盘启动（1）\n时间：{now_cn.strftime('%Y-%m-%d %H:%M')}\n币种：{sym}\n当前价格：{price_now:.6f}\n\n交易所 24h 涨跌幅：{change_24h:+.2f}%\n结构启动涨幅：+{start_pct:.2f}%\n当前结构涨幅：+{start_pct:.2f}%\n结构：3M HH 连续新高"
                        notify_all(msg)
                else:
                    drawdown = (sb["last_high"]-lows[-1])/sb["last_high"]
                    if drawdown>=DRAWDOWN_FAIL:
                        sb["active"]=False
                        continue
                    if highs[-1]>sb["last_high"] and sb["push_count"]<MAX_PUSH:
                        sb["last_high"]=highs[-1]
                        sb["push_count"]+=1
                        current_pct=(sb["last_high"]-sb["base_low"])/sb["base_low"]*100
                        sb["daily_high"] = max(sb["daily_high"], price_now)
                        sb["daily_low"] = min(sb["daily_low"], price_now)
                        sb["push_times"]+=1
                        msg = f"🚀 3M 拉盘推进（{sb['push_count']}）\n时间：{now_cn.strftime('%Y-%m-%d %H:%M')}\n币种：{sym}\n当前价格：{price_now:.6f}\n\n交易所 24h 涨跌幅：{change_24h:+.2f}%\n结构启动涨幅：+{sb['start_pct']:.2f}%\n当前结构涨幅：+{current_pct:.2f}%\n结构：3M HH 持续突破\n状态：第 {sb['push_count']} 次推进"
                        notify_all(msg)
            except:
                pass

            # ================= 观察板块 =================
            try:
                if sym in ALT_BLACKLIST:
                    continue
                so = state_observe[sym]
                if so["last_push_date"] != today:
                    so["progression_count"] = 0
                    so["first_time"] = None
                    so["first_price"] = None
                    so["state"] = "INIT"
                k15 = get_klines(sym,"15m",OBSERVE_WINDOW)
                highs15 = [float(x[2]) for x in k15]
                lows15 = [float(x[3]) for x in k15]
                closes15 = [float(x[4]) for x in k15]
                vols15 = [float(x[5]) for x in k15]
                volatility = (max(highs15)-min(lows15))/sum(closes15)/len(closes15)
                low_rise = all(lows15[i]>lows15[i-1] for i in range(1,len(lows15)))
                vol_avg = sum(vols15)/len(vols15)
                vol_current = vols15[-1]
                trades = get_trades(sym, limit=100)
                buy_amount = sum(float(t['qty'])*float(t['price']) for t in trades if not t['isBuyerMaker'])
                sell_amount = sum(float(t['qty'])*float(t['price']) for t in trades if t['isBuyerMaker'])
                capital_flow = (buy_amount-sell_amount)/(buy_amount+sell_amount+1e-9)
                pos = get_position_risk(sym)
                total_long = sum(float(p['positionAmt']) for p in pos if float(p['positionAmt'])>0)
                total_short = -sum(float(p['positionAmt']) for p in pos if float(p['positionAmt'])<0)
                longshort_ratio = (total_long/(total_short+1e-9)) if total_short>0 else total_long
                if low_rise and volatility<0.02 and vol_current>0.4*vol_avg and capital_flow>0 and longshort_ratio>1:
                    if so["last_push_date"] != today:
                        msg = f"观察币：{sym}\n时间：{now_cn.strftime('%Y-%m-%d %H:%M')}\n状态：主力建仓/缓慢拉升\n15M 高点：{highs15[-1]}  低点：{lows15[-1]}\n资金流入：{capital_flow*100:.2f}%\n多空对比：多头占优 {longshort_ratio:.2f}\n说明：结构健康，主力持续建仓"
                        notify_all(msg)
                        so["last_push_date"] = today
                        so["state"] = "OBSERVE_OK"
                        so["first_price"] = closes15[-1]
                        so["daily_high"] = closes15[-1]
                        so["daily_low"] = closes15[-1]
                        so["push_times"] = 1
                so["daily_high"] = max(so["daily_high"], closes15[-1])
                so["daily_low"] = min(so["daily_low"], closes15[-1])
            except:
                pass

        time.sleep(SCAN_INTERVAL)

    except Exception as e:
        time.sleep(5)

