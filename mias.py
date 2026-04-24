# -*- coding: utf-8 -*-
# =====================================================
# Binance USDT 永续合约 · B 系统 V2.2
# 三层过滤 + 资金行为验证 + 3M 实时预警
# =====================================================

import time
import json
import random
import requests
import smtplib
import pandas as pd
import os
from collections import defaultdict, deque
from datetime import datetime, timezone, timedelta, date
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.header import Header
from email.utils import formataddr

# ================= 基础配置 =================
BINANCE_API = "https://fapi.binance.com"
SCAN_INTERVAL = 30                 # 30 秒一轮
SYMBOL_REFRESH_HOURS = 72          # 3 天刷新一次币种
COLD_START_MINUTES = 5             # 冷启动保护 5 分钟

# ================= 通知配置 =================
BOT_TOKEN = "8557301222:AAHj1rSQ63zJGFXVxxuTniwRP2Y1tj3QsAs"
CHAT_ID = "5408890841"
EMAIL_USER = "1113496210@qq.com"
EMAIL_PASS = "hzshvazrbnyzfhdf"
EMAIL_TO = "1113496210@qq.com"

# ================= 主流币黑名单 =================
MAJOR_BLACKLIST = {
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT",
    "DOGEUSDT", "ADAUSDT", "AVAXUSDT", "TRXUSDT", "LINKUSDT",
    "DOTUSDT", "LTCUSDT", "BCHUSDT", "ATOMUSDT", "NEARUSDT", "APTUSDT"
}

# ================= Layer 1 参数（预警） =================
L1_PCT_THRESHOLD = 0.8             # 3M 实时涨幅 ≥ 0.8%
L1_VOL_RATIO = 2.5                 # 量能折算倍数 ≥ 2.5
L1_24H_MAX = 15.0                  # 24h 涨幅 < 15%
L1_TURNOVER_MIN = 30000            # 3M 最小成交额（USDT）
L1_PROGRESS_MIN = 0.20             # 3M 至少走到 20% 才判断

# ================= Layer 2 参数（资金验证） =================
L2_TAKER_BUY_RATIO = 0.60          # 主动买盘占比 ≥ 60%
L2_OI_CHANGE = 2.0                 # 5 分钟 OI 变化 ≥ 2%
L2_DENSITY_MIN = 2000              # 每笔成交额 ≥ 2000 U

# ================= Layer 3 参数（结构确认） =================
L3_PROGRESS_MIN = 0.66             # 第 2 根 3M 走到 2/3 才确认
L3_ACCUMULATE_PCT = 1.2            # 累计涨幅 ≥ 1.2%
L3_VOL_MULT = 1.3                  # 3M 量能 ≥ 1.3x

# ================= 推进/失败参数 =================
PUSH_NEW_HIGH_RATIO = 1.005        # 新高 ≥ last_high × 1.005
PUSH_PULLBACK_MAX = 0.02           # 推进时回撤 < 2%
FAIL_DRAWDOWN = 0.035              # 硬失败回撤 ≥ 3.5%
ACTIVE_TIMEOUT_MIN = 15            # 启动 15 分钟未推进则失效
FAIL_COOLDOWN_MIN = 30             # 失败冷却 30 分钟
MAX_PUSH = 3                       # 推进最多 3 次
MAX_DAILY_ROUND = 3                # 每币每日最多 3 轮
WARN_COOLDOWN_MIN = 10             # 预警冷却 10 分钟

# ================= 市商噪音过滤参数 =================
NOISE_TAKER_BUY_MIN = 0.50         # 量高但主动买 < 50% → 疑似对敲
NOISE_BODY_RATIO_MIN = 0.30        # 实体 / 振幅 < 30% → 插针
NOISE_PER_TRADE_MIN = 500          # 每笔 < 500 U → 机器人刷单（需结合量能）
PER_TRADE_MIN_TRADES = 20          # 最少成交笔数，否则 per_trade 不作数

# ================= 同步异动过滤参数 =================
MARKET_SAMPLE_SIZE = 80            # 样本数量
MARKET_NOISE_PCT = 1.2             # 样本 1M 涨幅阈值
MARKET_NOISE_RATIO = 0.60          # 样本占比阈值

# ================= 文件路径 =================
STATE_FILE = "state.json"
LAST_REPORT_FILE = "last_report.txt"
LOG_FILE = "logs.txt"

# ================= 全局状态 =================
SYMBOLS = []                       # 当前监控的币种
MARKET_SAMPLE = []                 # 同步异动样本
LAST_SYMBOL_REFRESH = 0
SYSTEM_START_TIME = None

# ================= 状态缓存 =================
_DT_FIELDS = ["start_time", "warn_time", "first_warn_time", "confirm_time"]

state_b = defaultdict(lambda: {
    "last_warn_ts": 0,
    "warn_level": None,
    "warn_price": None,
    "warn_time": None,
    "consecutive_count": 0,
    "last_candidate_ts": 0,

    "active": False,
    "last_high": None,
    "base_low": None,
    "start_price": None,
    "start_time": None,
    "push_count": 0,
    "last_push_time": 0,

    "fail_cooldown_until": 0,
    "day": None,
    "daily_round": 0,

    "daily_high": 0.0,
    "daily_low": 1e10,
    "push_times": 0,
    "first_warn_time": None,
    "first_warn_price": None,
    "confirm_time": None,
    "confirm_price": None,
    "final_status": None,
})

oi_history = defaultdict(lambda: deque(maxlen=5))
oi_last_sample_ts = defaultdict(float)

daily_signals = []
email_queue = []
last_email_flush = 0
batch_notify_queue = []

# ================= 日志 =================
def log(msg, level="INFO"):
    try:
        ts = datetime.now(timezone(timedelta(hours=8))).strftime("%Y-%m-%d %H:%M:%S")
        line = f"[{ts}] [{level}] {msg}\n"
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line)
        if level in ("ERROR", "WARN"):
            print(line.strip())
    except:
        pass

# ================= 通知基础函数 =================
def send_tg(text):
    try:
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        requests.post(url, json={"chat_id": CHAT_ID, "text": text}, timeout=10)
    except Exception as e:
        log(f"TG 发送失败：{e}", "WARN")

def send_email_raw(subject, content, attach_path=None):
    try:
        if attach_path:
            msg = MIMEMultipart()
            msg.attach(MIMEText(content, "plain", "utf-8"))
            with open(attach_path, "rb") as f:
                part = MIMEApplication(f.read(), Name=os.path.basename(attach_path))
                part['Content-Disposition'] = f'attachment; filename="{os.path.basename(attach_path)}"'
                msg.attach(part)
        else:
            msg = MIMEText(content, "plain", "utf-8")
        msg["From"] = formataddr(("B 系统监控", EMAIL_USER))
        msg["To"] = EMAIL_TO
        msg["Subject"] = Header(subject, "utf-8")

        server = smtplib.SMTP_SSL("smtp.qq.com", 465)
        server.login(EMAIL_USER, EMAIL_PASS)
        server.sendmail(EMAIL_USER, [EMAIL_TO], msg.as_string())
        server.quit()
    except Exception as e:
        log(f"邮件发送失败：{e}", "WARN")

def queue_email(subject, content):
    email_queue.append((subject, content))

def flush_email_queue():
    global email_queue, last_email_flush
    if not email_queue:
        return
    now = time.time()
    if now - last_email_flush < 60:
        return
    subjects = [e[0] for e in email_queue]
    contents = [e[1] for e in email_queue]
    combined_subject = f"B 系统 · {len(email_queue)} 条信号"
    if len(email_queue) == 1:
        combined_subject = subjects[0]
    combined_content = "\n\n" + ("─" * 40 + "\n\n").join(contents)
    send_email_raw(combined_subject, combined_content)
    email_queue = []
    last_email_flush = now

def notify_tg_only(text):
    send_tg(text)

def notify_tg_and_email(tg_text, email_subject, email_content):
    send_tg(tg_text)
    queue_email(email_subject, email_content)

# ================= Binance API =================
def api_get(path, params=None, timeout=10):
    try:
        r = requests.get(f"{BINANCE_API}{path}", params=params, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        log(f"API 失败 {path} {params}：{e}", "WARN")
        return None

def get_all_symbols():
    data = api_get("/fapi/v1/exchangeInfo")
    if not data:
        return []
    symbols = [
        s["symbol"] for s in data["symbols"]
        if s.get("contractType") == "PERPETUAL"
        and s.get("quoteAsset") == "USDT"
        and s.get("status") == "TRADING"
    ]
    return [s for s in symbols if s not in MAJOR_BLACKLIST]

def get_all_tickers():
    data = api_get("/fapi/v1/ticker/24hr")
    if not data:
        return {}
    return {t["symbol"]: t for t in data if isinstance(t, dict)}

def get_klines(symbol, interval, limit):
    data = api_get("/fapi/v1/klines", {"symbol": symbol, "interval": interval, "limit": limit})
    return data if data else []

def get_open_interest(symbol):
    data = api_get("/fapi/v1/openInterest", {"symbol": symbol})
    if data and "openInterest" in data:
        try:
            return float(data["openInterest"])
        except:
            return None
    return None

# ================= 币种刷新 =================
def refresh_symbols(force=False):
    global SYMBOLS, MARKET_SAMPLE, LAST_SYMBOL_REFRESH
    now = time.time()
    if not force and now - LAST_SYMBOL_REFRESH < SYMBOL_REFRESH_HOURS * 3600:
        return
    new_symbols = get_all_symbols()
    if not new_symbols:
        log("币种刷新失败，使用旧列表", "WARN")
        return
    old_set = set(SYMBOLS)
    new_set = set(new_symbols)
    added = new_set - old_set
    removed = old_set - new_set

    for sym in removed:
        state_b.pop(sym, None)
        oi_history.pop(sym, None)
        oi_last_sample_ts.pop(sym, None)

    SYMBOLS = new_symbols
    LAST_SYMBOL_REFRESH = now

    if len(SYMBOLS) >= MARKET_SAMPLE_SIZE:
        MARKET_SAMPLE = random.sample(SYMBOLS, MARKET_SAMPLE_SIZE)
    else:
        MARKET_SAMPLE = list(SYMBOLS)

    if not force:
        msg = (
            f"🔄 币种列表已更新\n"
            f"{datetime.now(timezone(timedelta(hours=8))).strftime('%Y-%m-%d %H:%M')}\n\n"
            f"新增：{len(added)} 个\n" +
            ("\n".join(f"• {s}" for s in list(added)[:10]) + "\n" if added else "") +
            f"\n下架：{len(removed)} 个\n" +
            ("\n".join(f"• {s}" for s in list(removed)[:10]) + "\n" if removed else "") +
            f"\n当前监控：{len(SYMBOLS)} 个"
        )
        notify_tg_only(msg)
        log(f"币种刷新：监控 {len(SYMBOLS)} 个，新增 {len(added)}，下架 {len(removed)}")

# ================= 状态持久化 =================
def save_state():
    try:
        serializable = {}
        for sym, st in state_b.items():
            d = {}
            for k, v in st.items():
                if isinstance(v, (set, deque)):
                    continue
                if isinstance(v, datetime):
                    d[k] = v.isoformat()
                elif isinstance(v, date):
                    d[k] = v.isoformat()
                else:
                    d[k] = v
            serializable[sym] = d
        data = {"state_b": serializable, "daily_signals": daily_signals}
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, default=str, ensure_ascii=False)
    except Exception as e:
        log(f"状态保存失败：{e}", "WARN")

def load_state():
    global daily_signals
    try:
        if not os.path.exists(STATE_FILE):
            return
        with open(STATE_FILE, "r") as f:
            data = json.load(f)
        for sym, st in data.get("state_b", {}).items():
            for k, v in st.items():
                state_b[sym][k] = v
            # 恢复 day 类型
            if isinstance(state_b[sym].get("day"), str):
                try:
                    state_b[sym]["day"] = date.fromisoformat(state_b[sym]["day"])
                except:
                    state_b[sym]["day"] = None
            # 恢复所有 datetime 字段
            for dt_field in _DT_FIELDS:
                val = state_b[sym].get(dt_field)
                if isinstance(val, str):
                    try:
                        state_b[sym][dt_field] = datetime.fromisoformat(val)
                    except:
                        state_b[sym][dt_field] = None
        daily_signals = data.get("daily_signals", [])
        log(f"状态已恢复：{len(state_b)} 个币")
    except Exception as e:
        log(f"状态加载失败：{e}", "WARN")

# ================= Layer 1 候选筛选 =================
def analyze_3m_realtime(sym):
    k3 = get_klines(sym, "3m", 5)
    if not k3 or len(k3) < 5:
        return None
    cur = k3[-1]
    open_time_ms = int(cur[0])
    now_ms = int(time.time() * 1000)
    progress = (now_ms - open_time_ms) / (3 * 60 * 1000)
    progress = max(0.0, min(1.0, progress))

    if progress < L1_PROGRESS_MIN:
        return None

    o = float(cur[1]); h = float(cur[2]); l = float(cur[3]); c = float(cur[4])
    v = float(cur[5])
    quote_vol = float(cur[7])
    trades = int(cur[8])
    taker_buy_vol = float(cur[9])
    taker_buy_quote = float(cur[10])

    real_time_pct = (c - o) / o * 100 if o > 0 else 0
    amplitude = (h - l) / o * 100 if o > 0 else 0
    body_ratio = abs(c - o) / (h - l) if (h - l) > 0 else 0

    prev_vols = [float(k[5]) for k in k3[:-1]]
    avg_vol = sum(prev_vols) / len(prev_vols) if prev_vols else 0
    expected_vol = avg_vol * progress
    vol_ratio = (v / expected_vol) if expected_vol > 0 else 0

    taker_buy_ratio = taker_buy_quote / quote_vol if quote_vol > 0 else 0

    per_trade = quote_vol / trades if trades >= PER_TRADE_MIN_TRADES else 0

    return {
        "open": o, "high": h, "low": l, "close": c,
        "volume": v, "quote_vol": quote_vol, "trades": trades,
        "taker_buy_ratio": taker_buy_ratio, "per_trade": per_trade,
        "real_time_pct": real_time_pct, "amplitude": amplitude,
        "body_ratio": body_ratio, "vol_ratio": vol_ratio,
        "progress": progress, "k3": k3,
    }

def pass_layer1(data, change_24h):
    if data is None:
        return False
    if data["real_time_pct"] < L1_PCT_THRESHOLD:
        return False
    if data["vol_ratio"] < L1_VOL_RATIO:
        return False
    if change_24h >= L1_24H_MAX:
        return False
    if data["quote_vol"] < L1_TURNOVER_MIN:
        return False
    return True

# ================= 市商噪音过滤 =================
def is_market_maker_noise(data, oi_change):
    reasons = []
    if data["vol_ratio"] >= L1_VOL_RATIO and data["taker_buy_ratio"] < NOISE_TAKER_BUY_MIN:
        reasons.append("主动买盘过低(对敲)")
    if data["amplitude"] >= 1.5 and data["body_ratio"] < NOISE_BODY_RATIO_MIN:
        reasons.append("长影线(插针)")
    if data["vol_ratio"] >= L1_VOL_RATIO and data["per_trade"] > 0 and data["per_trade"] < NOISE_PER_TRADE_MIN:
        reasons.append("单笔过小(机器人刷单)")
    if oi_change is not None and abs(oi_change) < 0.3 and data["real_time_pct"] >= 0.8:
        reasons.append("OI无变化(无真金入场)")
    if data["trades"] > 500 and data["per_trade"] > 0 and data["per_trade"] < 300:
        reasons.append("高密度微单")

    return (len(reasons) >= 2), reasons

# ================= Layer 2 资金验证 =================
def sample_oi(sym):
    now = time.time()
    last_ts = oi_last_sample_ts.get(sym, 0)
    if now - last_ts < 120:
        pass
    else:
        oi = get_open_interest(sym)
        if oi is not None:
            oi_history[sym].append((now, oi))
            oi_last_sample_ts[sym] = now

    history = list(oi_history[sym])
    if len(history) < 2:
        return None
    latest_ts, latest_oi = history[-1]
    past_oi = None
    for ts, oi in history:
        if latest_ts - ts >= 240:
            past_oi = oi
            break
    if past_oi is None or past_oi <= 0:
        return None
    change = (latest_oi - past_oi) / past_oi * 100
    return change

def layer2_check(data, oi_change):
    detail = {}
    passed = 0

    taker_ok = data["taker_buy_ratio"] >= L2_TAKER_BUY_RATIO
    detail["taker_buy_ratio"] = data["taker_buy_ratio"]
    detail["taker_buy_pass"] = taker_ok
    if taker_ok:
        passed += 1

    if oi_change is not None:
        oi_ok = oi_change >= L2_OI_CHANGE
        detail["oi_change"] = oi_change
        detail["oi_pass"] = oi_ok
        if oi_ok:
            passed += 1
    else:
        detail["oi_change"] = None
        detail["oi_pass"] = False

    density_ok = data["per_trade"] >= L2_DENSITY_MIN
    detail["per_trade"] = data["per_trade"]
    detail["density_pass"] = density_ok
    if density_ok:
        passed += 1

    return passed, detail

# ================= Layer 3 结构确认 =================
def layer3_check(data):
    k3 = data["k3"]
    if data["progress"] < L3_PROGRESS_MIN:
        return False, {}

    highs = [float(k[2]) for k in k3]
    lows = [float(k[3]) for k in k3]

    if not (highs[-3] < highs[-2]):
        return False, {}
    if not (highs[-1] > highs[-2]):
        return False, {}

    base_low = min(lows[-3:])
    accumulate_pct = (highs[-1] - base_low) / base_low * 100
    if accumulate_pct < L3_ACCUMULATE_PCT:
        return False, {}

    prev_vols = [float(k[5]) for k in k3[-5:-1]]
    avg_vol = sum(prev_vols) / len(prev_vols) if prev_vols else 0
    vol_ok = data["vol_ratio"] >= L3_VOL_MULT
    if not vol_ok:
        return False, {}

    return True, {
        "accumulate_pct": accumulate_pct,
        "base_low": base_low,
        "last_high": highs[-1],
    }

# ================= 同步异动过滤 =================
def market_synced_surge():
    if not MARKET_SAMPLE:
        return False
    sample_hit = 0
    surge_count = 0
    for sym in MARKET_SAMPLE[:40]:
        k = get_klines(sym, "1m", 2)
        if not k or len(k) < 2:
            continue
        sample_hit += 1
        o = float(k[-1][1])
        c = float(k[-1][4])
        pct_1m = (c - o) / o * 100 if o > 0 else 0
        if pct_1m >= MARKET_NOISE_PCT:
            surge_count += 1
    if sample_hit < 10:
        return False
    ratio = surge_count / sample_hit
    return ratio >= MARKET_NOISE_RATIO

# ================= 推送构造 =================
def now_cn_str():
    return datetime.now(timezone(timedelta(hours=8))).strftime("%Y-%m-%d %H:%M:%S")

def build_warn_message(level, sym, data, change_24h, l2_detail, l2_passed):
    emoji = "⚡" if level == "light" else "🟡"
    name = "异动预警" if level == "light" else "强预警（资金确认）"
    lines = [
        f"{emoji} {name}",
        now_cn_str(),
        "",
        f"币种：{sym}",
        f"当前价：{data['close']:.6f}",
        f"3M 实时涨幅：+{data['real_time_pct']:.2f}%",
        f"3M 量能：{data['vol_ratio']:.1f}x（实时折算）",
        f"24h 涨跌：{change_24h:+.2f}%",
        f"3M 成交额：{data['quote_vol']:.0f} U",
        "",
    ]
    lines.append(f"资金面（{l2_passed}/3 通过）：")
    lines.append(f"• 主动买盘：{l2_detail['taker_buy_ratio']*100:.1f}% {'✓' if l2_detail['taker_buy_pass'] else '✗'}")
    if l2_detail["oi_change"] is not None:
        lines.append(f"• OI 变化：{l2_detail['oi_change']:+.2f}% {'✓' if l2_detail['oi_pass'] else '✗'}")
    else:
        lines.append(f"• OI 变化：数据积累中")
    lines.append(f"• 成交密度：{l2_detail['per_trade']:.0f} U/笔 {'✓' if l2_detail['density_pass'] else '✗'}")
    lines.append("")
    if level == "light":
        lines.append("判定：量价异动，初步信号")
    else:
        lines.append("判定：资金加速，等待结构确认")
    return "\n".join(lines)

def build_confirm_message(sym, data, change_24h, l2_detail, l2_passed, l3_detail, st):
    lines = [
        "🟢 3M 拉盘确认启动（1）",
        now_cn_str(),
        "",
        f"币种：{sym}",
        f"当前价：{data['close']:.6f}",
        f"结构启动涨幅：+{l3_detail['accumulate_pct']:.2f}%",
    ]
    if st.get("first_warn_price"):
        cum = (data["close"] - st["first_warn_price"]) / st["first_warn_price"] * 100
        lines.append(f"累计涨幅（从预警）：+{cum:.2f}%")
    lines.append(f"24h 涨跌：{change_24h:+.2f}%")
    lines.append("")
    lines.append("3M 结构：")
    lines.append("• 前 2 根 HH 成立 ✓")
    lines.append("• 累计涨幅 ≥ 1.2% ✓")
    lines.append(f"• 3M 量能 {data['vol_ratio']:.1f}x ✓")
    lines.append("")
    lines.append(f"资金行为（{l2_passed}/3 通过）：")
    lines.append(f"• 主动买盘：{l2_detail['taker_buy_ratio']*100:.1f}% {'✓' if l2_detail['taker_buy_pass'] else '✗'}")
    if l2_detail["oi_change"] is not None:
        lines.append(f"• OI 变化：{l2_detail['oi_change']:+.2f}% {'✓' if l2_detail['oi_pass'] else '✗'}")
    else:
        lines.append(f"• OI 变化：数据积累中")
    lines.append(f"• 成交密度：{l2_detail['per_trade']:.0f} U/笔 {'✓' if l2_detail['density_pass'] else '✗'}")
    lines.append("")
    if st.get("first_warn_time"):
        diff_sec = (datetime.now(timezone(timedelta(hours=8))) - st["first_warn_time"]).total_seconds()
        lines.append(f"距预警：{int(diff_sec/60)} 分钟")
    lines.append("判定：结构 + 资金双重确认")
    lines.append(f"今日第 {st['daily_round']} 轮")
    return "\n".join(lines)

def build_push_message(sym, data, change_24h, st, push_num, cur_high):
    from_start = (data["close"] - st["start_price"]) / st["start_price"] * 100
    new_high_pct = (cur_high - st["last_high"]) / st["last_high"] * 100 if st["last_high"] else 0
    pullback = (cur_high - data["close"]) / cur_high * 100
    lines = [
        f"🚀 3M 拉盘推进（{push_num}）",
        now_cn_str(),
        "",
        f"币种：{sym}",
        f"当前价：{data['close']:.6f}",
        f"累计涨幅（从启动）：+{from_start:.2f}%",
        f"本次新高涨幅：+{new_high_pct:.2f}%",
        f"距本次高点回撤：-{pullback:.2f}%",
        f"24h 涨跌：{change_24h:+.2f}%",
        "",
        "资金状态：健康",
        f"• 主动买盘：{data['taker_buy_ratio']*100:.1f}%",
        "",
        f"第 {push_num} 次推进",
    ]
    if st.get("start_time"):
        diff = (datetime.now(timezone(timedelta(hours=8))) - st["start_time"]).total_seconds()
        lines.append(f"距启动 {int(diff/60)} 分钟")
    return "\n".join(lines)

def add_batch(level, sym, summary):
    batch_notify_queue.append((level, sym, summary))

def flush_batch():
    global batch_notify_queue
    if not batch_notify_queue:
        return
    if len(batch_notify_queue) < 3:
        batch_notify_queue = []
        return
    groups = defaultdict(list)
    for level, sym, summary in batch_notify_queue:
        groups[level].append(summary)
    lines = [
        f"⚡ 本轮异动批量提示（共 {len(batch_notify_queue)} 个）",
        now_cn_str()
    ]
    for level in ["confirm", "strong", "light", "push"]:
        if level not in groups:
            continue
        title = {
            "confirm": "🟢 确认启动",
            "strong": "🟡 强预警",
            "light": "⚡ 轻预警",
            "push": "🚀 推进"
        }[level]
        lines.append("")
        lines.append(f"{title}：")
        for s in groups[level]:
            lines.append(s)
    send_tg("\n".join(lines))
    batch_notify_queue = []

# ================= 主循环：单币处理 =================
def process_symbol(sym, ticker, cold_start):
    st = state_b[sym]
    today = date.today()
    now_cn = datetime.now(timezone(timedelta(hours=8)))
    now_ts = time.time()

    if st["day"] != today:
        st["day"] = today
        st["daily_round"] = 0
        st["active"] = False
        st["first_warn_time"] = None
        st["first_warn_price"] = None
        st["confirm_time"] = None
        st["confirm_price"] = None
        st["final_status"] = None
        st["daily_high"] = 0.0
        st["daily_low"] = 1e10
        st["push_times"] = 0

    if now_ts < st["fail_cooldown_until"]:
        return

    if st["daily_round"] >= MAX_DAILY_ROUND and not st["active"]:
        return

    try:
        change_24h = float(ticker.get("priceChangePercent", 0))
    except:
        change_24h = 0

    if st["active"]:
        data = analyze_3m_realtime(sym)
        if data is None:
            return

        cur_high = data["high"]
        cur_low = data["low"]
        price_now = data["close"]

        drawdown = (st["last_high"] - cur_low) / st["last_high"] if st["last_high"] else 0
        if drawdown >= FAIL_DRAWDOWN:
            st["active"] = False
            st["fail_cooldown_until"] = now_ts + FAIL_COOLDOWN_MIN * 60
            st["final_status"] = "失败(回撤)"
            log(f"{sym} 失败：回撤 {drawdown*100:.2f}%，进入 30 分钟冷却")
            return

        if st["start_time"]:
            elapsed_min = (now_cn - st["start_time"]).total_seconds() / 60
            if elapsed_min > ACTIVE_TIMEOUT_MIN and st["push_count"] <= 1:
                st["active"] = False
                st["final_status"] = "失效(超时)"
                log(f"{sym} 启动 {elapsed_min:.0f} 分钟未推进，失效")
                return

        if cur_high >= st["last_high"] * PUSH_NEW_HIGH_RATIO:
            pullback = (cur_high - price_now) / cur_high
            if pullback < PUSH_PULLBACK_MAX and st["push_count"] < MAX_PUSH:
                if now_ts - st["last_push_time"] >= 180:
                    st["push_count"] += 1
                    st["last_high"] = cur_high
                    st["last_push_time"] = now_ts
                    st["push_times"] += 1
                    st["final_status"] = f"推进({st['push_count']})"

                    push_msg = build_push_message(sym, data, change_24h, st, st["push_count"], cur_high)
                    if not cold_start:
                        send_tg(push_msg)
                        queue_email(f"🚀 {sym} 推进{st['push_count']}", push_msg)
                        add_batch("push", sym, f"{now_cn.strftime('%H:%M')} | {sym} | 累计+{(price_now-st['start_price'])/st['start_price']*100:.2f}% | 第{st['push_count']}次")

        st["daily_high"] = max(st["daily_high"], price_now)
        st["daily_low"] = min(st["daily_low"], price_now)
        return

    # --- 候选流程 ---
    data = analyze_3m_realtime(sym)
    if not pass_layer1(data, change_24h):
        return

    oi_change = sample_oi(sym)

    is_noise, noise_reasons = is_market_maker_noise(data, oi_change)
    if is_noise:
        log(f"{sym} 市商噪音过滤：{', '.join(noise_reasons)}")
        return

    l2_passed, l2_detail = layer2_check(data, oi_change)
    if l2_passed == 0:
        return
    warn_level = "light" if l2_passed == 1 else "strong"

    l3_ok = False
    l3_detail = {}
    if l2_passed >= 2:
        l3_ok, l3_detail = layer3_check(data)

    is_fresh_warn = (now_ts - st["last_warn_ts"]) >= WARN_COOLDOWN_MIN * 60
    is_upgrade = (st["warn_level"] == "light" and warn_level == "strong")

    if not is_fresh_warn and not is_upgrade and not l3_ok:
        return

    if (now_ts - st.get("last_candidate_ts", 0)) <= 120:
        st["consecutive_count"] += 1
    else:
        st["consecutive_count"] = 1
    st["last_candidate_ts"] = now_ts

    if st["consecutive_count"] >= 2 and warn_level == "light":
        warn_level = "strong"

    if l3_ok:
        st["active"] = True
        st["start_price"] = data["close"]
        st["start_time"] = now_cn
        st["last_high"] = l3_detail["last_high"]
        st["base_low"] = l3_detail["base_low"]
        st["push_count"] = 1
        st["last_push_time"] = now_ts
        st["daily_round"] += 1
        st["confirm_time"] = now_cn
        st["confirm_price"] = data["close"]
        st["push_times"] = 1
        st["daily_high"] = data["close"]
        st["daily_low"] = data["close"]
        st["final_status"] = "确认启动"

        if st["first_warn_time"] is None:
            st["first_warn_time"] = now_cn
            st["first_warn_price"] = data["close"]

        daily_signals.append({
            "symbol": sym,
            "warn_time": st["first_warn_time"].strftime("%H:%M:%S"),
            "warn_price": st["first_warn_price"],
            "confirm_time": now_cn.strftime("%H:%M:%S"),
            "confirm_price": data["close"],
            "type": "confirm",
        })

        msg = build_confirm_message(sym, data, change_24h, l2_detail, l2_passed, l3_detail, st)
        if not cold_start:
            send_tg(msg)
            queue_email(f"🟢 {sym} 确认启动", msg)
            add_batch("confirm", sym, f"{now_cn.strftime('%H:%M')} | {sym} | +{l3_detail['accumulate_pct']:.2f}% | 主买 {l2_detail['taker_buy_ratio']*100:.0f}% | 24h {change_24h:+.1f}%")
        return

    # 只触发预警
    if st["first_warn_time"] is None:
        st["first_warn_time"] = now_cn
        st["first_warn_price"] = data["close"]

    st["last_warn_ts"] = now_ts
    st["warn_level"] = warn_level
    st["warn_price"] = data["close"]
    st["warn_time"] = now_cn

    msg = build_warn_message(warn_level, sym, data, change_24h, l2_detail, l2_passed)
    if not cold_start:
        if warn_level == "light":
            send_tg(msg)
            add_batch("light", sym, f"{now_cn.strftime('%H:%M')} | {sym} | +{data['real_time_pct']:.2f}% | 主买 {l2_detail['taker_buy_ratio']*100:.0f}% | 24h {change_24h:+.1f}%")
        else:
            send_tg(msg)
            queue_email(f"🟡 {sym} 强预警", msg)
            add_batch("strong", sym, f"{now_cn.strftime('%H:%M')} | {sym} | +{data['real_time_pct']:.2f}% | 主买 {l2_detail['taker_buy_ratio']*100:.0f}% | 24h {change_24h:+.1f}%")

    daily_signals.append({
        "symbol": sym,
        "warn_time": st["first_warn_time"].strftime("%H:%M:%S"),
        "warn_price": st["first_warn_price"],
        "type": warn_level,
    })

# ================= 日报（修复：内容为昨天，跨天检测） =================
def read_last_report_date():
    try:
        if not os.path.exists(LAST_REPORT_FILE):
            return None
        with open(LAST_REPORT_FILE, "r") as f:
            return f.read().strip()
    except:
        return None

def write_last_report_date(d):
    try:
        with open(LAST_REPORT_FILE, "w") as f:
            f.write(d)
    except:
        pass

def generate_daily_report():
    # 日报反映"昨天"的统计
    yesterday = date.today() - timedelta(days=1)
    report_date_str = yesterday.isoformat()
    last = read_last_report_date()
    if last == report_date_str:
        return

    total = len(daily_signals)
    count_light = sum(1 for s in daily_signals if s.get("type") == "light")
    count_strong = sum(1 for s in daily_signals if s.get("type") == "strong")
    count_confirm = sum(1 for s in daily_signals if s.get("type") == "confirm")

    rows = []
    for sym, st in state_b.items():
        if st.get("first_warn_time") is None and st.get("confirm_time") is None:
            continue
        high = st.get("daily_high", 0) or 0
        start_p = st.get("start_price") or st.get("first_warn_price")
        max_pct = (high - start_p) / start_p * 100 if start_p and high > 0 else 0
        rows.append({
            "币种": sym,
            "预警时间": st["first_warn_time"].strftime("%H:%M:%S") if st.get("first_warn_time") else "",
            "预警价": st.get("first_warn_price") or "",
            "确认时间": st["confirm_time"].strftime("%H:%M:%S") if st.get("confirm_time") else "",
            "确认价": st.get("confirm_price") or "",
            "推进次数": st.get("push_count", 0),
            "最高价": high,
            "最大涨幅(%)": round(max_pct, 2),
            "最终状态": st.get("final_status") or "仅预警",
        })

    rows_sorted = sorted(rows, key=lambda x: x["最大涨幅(%)"], reverse=True)
    top5 = rows_sorted[:5]

    body = [
        f"【币安 B 系统 日报】{report_date_str}",
        "",
        "═══ 信号统计 ═══",
        f"总推送：{total} 条",
        f" ⚡ 轻预警：{count_light}",
        f" 🟡 强预警：{count_strong}",
        f" 🟢 确认启动：{count_confirm}",
        "",
        "═══ 最佳信号 TOP 5 ═══",
    ]
    for i, r in enumerate(top5, 1):
        body.append(f"{i}. {r['币种']} 启动 {r['预警价']} → 高 {r['最高价']:.6f} +{r['最大涨幅(%)']}%")

    filename = None
    if rows_sorted:
        filename = f"B系统日报_{report_date_str.replace('-','')}.xlsx"
        df = pd.DataFrame(rows_sorted)
        df.to_excel(filename, index=False)

    send_email_raw(f"B 系统日报 {report_date_str}", "\n".join(body), attach_path=filename)
    send_tg("\n".join(body[:15]))

    write_last_report_date(report_date_str)
    log(f"日报已发送：{report_date_str}")

    # 重置每日统计（为今天清空）
    daily_signals.clear()
    for sym, st in state_b.items():
        st["daily_high"] = 0.0
        st["daily_low"] = 1e10
        st["push_times"] = 0
        st["daily_round"] = 0
        st["first_warn_time"] = None
        st["first_warn_price"] = None
        st["confirm_time"] = None
        st["confirm_price"] = None
        st["final_status"] = None

# ================= 启动 =================
def main():
    global SYSTEM_START_TIME
    log("====== 系统启动 ======")

    load_state()
    refresh_symbols(force=True)

    SYSTEM_START_TIME = time.time()
    startup_msg = (
        f"✅ 币安 B 系统启动\n"
        f"{now_cn_str()}\n\n"
        f"监控币种：{len(SYMBOLS)} 个（已排除 {len(MAJOR_BLACKLIST)} 个主流币）\n"
        f"扫描频率：{SCAN_INTERVAL} 秒/轮\n"
        f"架构：三层过滤 + 资金验证\n\n"
        f"当前状态：冷启动保护（前 {COLD_START_MINUTES} 分钟只收集数据）"
    )
    send_tg(startup_msg)
    queue_email("B 系统启动", startup_msg)

    cold_start_end_notified = False
    last_state_save = 0
    last_market_check = 0
    market_paused = False
    last_report_check_date = None

    while True:
        try:
            now_ts = time.time()
            now_cn = datetime.now(timezone(timedelta(hours=8)))

            # 日报跨天检测（修复问题2）
            today_str = now_cn.date().isoformat()
            if last_report_check_date != today_str:
                if last_report_check_date is not None:
                    generate_daily_report()
                last_report_check_date = today_str

            refresh_symbols(force=False)

            cold_start = (now_ts - SYSTEM_START_TIME) < COLD_START_MINUTES * 60
            if not cold_start and not cold_start_end_notified:
                send_tg(f"🟢 冷启动保护解除\n{now_cn_str()}\n系统进入正式推送模式")
                cold_start_end_notified = True

            tickers = get_all_tickers()
            if not tickers:
                log("全市场 ticker 获取失败，本轮跳过", "WARN")
                time.sleep(SCAN_INTERVAL)
                continue

            # 冷启动期间提前采样 OI，为后续积累数据（修复问题1）
            if cold_start:
                sample_targets = random.sample(SYMBOLS, min(50, len(SYMBOLS)))
                for sym in sample_targets:
                    sample_oi(sym)

            if now_ts - last_market_check > 300:
                paused_now = market_synced_surge()
                market_paused = paused_now
                last_market_check = now_ts

            if market_paused and not cold_start:
                for sym in random.sample(SYMBOLS, min(20, len(SYMBOLS))):
                    sample_oi(sym)
                time.sleep(SCAN_INTERVAL)
                continue

            for sym in SYMBOLS:
                ticker = tickers.get(sym)
                if not ticker:
                    continue
                process_symbol(sym, ticker, cold_start)

            flush_batch()
            flush_email_queue()

            if now_ts - last_state_save > 300:
                save_state()
                last_state_save = now_ts

            time.sleep(SCAN_INTERVAL)

        except KeyboardInterrupt:
            log("用户手动停止")
            save_state()
            break
        except Exception as e:
            log(f"主循环异常：{e}", "ERROR")
            time.sleep(SCAN_INTERVAL)

if __name__ == "__main__":
    main()