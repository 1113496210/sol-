#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sqlite3
import requests

BINANCE_API = "https://fapi.binance.com"
session = requests.Session()
DB_PATH = "/www/signals.db"

# 需要检查的币种列表
symbols_to_check = ["PROVEUSDT", "USELESSUSDT", "1000CHEEMSUSDT"]

print("=" * 70)
print("🔍 币安合约资金费率策略诊断工具")
print("=" * 70)

# ---------- 1. 成交额初步检查 ----------
print("\n📊 1. 24h成交额检查（门槛 1300万 USDT）")
print("-" * 50)
for sym in symbols_to_check:
    try:
        r = session.get(f"{BINANCE_API}/fapi/v1/ticker/24hr", params={"symbol": sym}).json()
        if 'code' in r and r['code'] == -1121:
            print(f"{sym:20s} ❌ 合约不存在 (Invalid symbol)")
            continue
        vol = float(r.get('quoteVolume', 0))
        status = "✅ 通过" if vol > 13_000_000 else "❌ 未通过"
        print(f"{sym:20s} {vol/1e6:>8.1f}M USDT  {status}")
    except Exception as e:
        print(f"{sym:20s} 请求失败: {e}")

# ---------- 2. 模拟 high_volume_symbols ----------
print("\n📋 2. 当前全部高成交额币种（volume > 13M）")
print("-" * 50)
try:
    tickers = session.get(f"{BINANCE_API}/fapi/v1/ticker/24hr").json()
    high_volume_set = set()
    for t in tickers:
        try:
            if float(t.get('quoteVolume', 0)) > 13_000_000:
                high_volume_set.add(t['symbol'])
        except:
            pass
    print(f"高成交额币种总数: {len(high_volume_set)}")
    for sym in symbols_to_check:
        if sym in high_volume_set:
            print(f"{sym} ✅ 在 high_volume_set 中")
        else:
            print(f"{sym} ❌ 不在 high_volume_set 中")
except Exception as e:
    print(f"获取全市场24hr数据失败: {e}")

# ---------- 3. 详细诊断每个币（多因子） ----------
print("\n🔎 3. 详细多因子诊断")
for symbol in symbols_to_check:
    print(f"\n{'='*60}")
    print(f"诊断对象: {symbol}")
    print(f"{'='*60}")

    # ----- 成交额（再次确认）-----
    try:
        ticker = session.get(f"{BINANCE_API}/fapi/v1/ticker/24hr", params={"symbol": symbol}).json()
        if 'code' in ticker and ticker['code'] == -1121:
            print("❌ 合约不存在，跳过后续所有因子")
            continue
        vol = float(ticker.get('quoteVolume', 0))
        print(f"\n1️⃣ 24h成交额: {vol/1e6:.1f}M USDT  {'✅' if vol > 13_000_000 else '❌'}")
    except Exception as e:
        print(f"\n1️⃣ 获取成交额失败: {e}")
        continue

    # ----- 3M HH -----
    try:
        k3 = session.get(f"{BINANCE_API}/fapi/v1/klines",
                         params={"symbol": symbol, "interval": "3m", "limit": 6}).json()
        highs = [float(x[2]) for x in k3]
        lows = [float(x[3]) for x in k3]
        hh = highs[-3] < highs[-2] < highs[-1]
        start_pct = (highs[-1] - lows[-3]) / lows[-3] * 100
        print(f"\n2️⃣ 3M HH:")
        print(f"   高点序列: {highs[-3]:.8f} → {highs[-2]:.8f} → {highs[-1]:.8f}")
        print(f"   HH连续新高: {'✅' if hh else '❌'}")
        print(f"   启动涨幅: {start_pct:.2f}% (门槛1.2%) -> {'✅' if start_pct >= 1.2 else '❌'}")
        print(f"   速报门槛: {start_pct:.2f}% (门槛3%)   -> {'✅' if start_pct >= 3.0 else '❌'}")
    except Exception as e:
        print(f"\n2️⃣ 3M HH获取失败: {e}")

    # ----- 1H 趋势通道 -----
    try:
        k1h = session.get(f"{BINANCE_API}/fapi/v1/klines",
                          params={"symbol": symbol, "interval": "1h", "limit": 24}).json()
        closes = [float(k[4]) for k in k1h]
        opens = [float(k[1]) for k in k1h]
        highs_1h = [float(k[2]) for k in k1h]
        change_24h = (closes[-1] - closes[0]) / closes[0] * 100
        bull_count = sum(1 for i in range(len(closes)) if closes[i] > opens[i])
        high_24h = max(highs_1h)
        at_high_pct = closes[-1] / high_24h * 100
        print(f"\n3️⃣ 趋势通道:")
        print(f"   24h涨幅: {change_24h:+.2f}% (门槛>5%) -> {'✅' if change_24h > 5 else '❌'}")
        print(f"   24h阳线数: {bull_count}/24 (门槛≥10) -> {'✅' if bull_count >= 10 else '❌'}")
        print(f"   距24h最高: {at_high_pct:.1f}% (门槛>90%) -> {'✅' if at_high_pct > 90 else '❌'}")
    except Exception as e:
        print(f"\n3️⃣ 趋势通道获取失败: {e}")

    # ----- OI 变化 -----
    try:
        oi = session.get(f"{BINANCE_API}/futures/data/openInterestHist",
                         params={"symbol": symbol, "period": "1h", "limit": 2}).json()
        if isinstance(oi, list) and len(oi) >= 2:
            oi_prev = float(oi[0]['sumOpenInterest'])
            oi_now = float(oi[-1]['sumOpenInterest'])
            oi_change = (oi_now - oi_prev) / oi_prev * 100
            print(f"\n4️⃣ OI变化: {oi_change:+.2f}% (硬因子>1%) -> {'✅' if abs(oi_change) > 1 else '❌'}")
        else:
            print(f"\n4️⃣ OI数据不足: {oi}")
    except Exception as e:
        print(f"\n4️⃣ OI获取失败: {e}")

    # ----- 量能比 (15M) -----
    try:
        k15 = session.get(f"{BINANCE_API}/fapi/v1/klines",
                          params={"symbol": symbol, "interval": "15m", "limit": 20}).json()
        vols = [float(k[5]) for k in k15]
        if len(vols) >= 4:
            avg_vol = sum(vols[:-3]) / (len(vols)-3)
            recent_vol = sum(vols[-3:]) / 3
            vol_ratio = recent_vol / avg_vol if avg_vol > 0 else 0
            print(f"\n5️⃣ 量能比: {vol_ratio:.2f}x (硬因子>1.5) -> {'✅' if vol_ratio > 1.5 else '❌'}")
        else:
            print(f"\n5️⃣ 量能数据不足（只有{len(vols)}根K线）")
    except Exception as e:
        print(f"\n5️⃣ 量能获取失败: {e}")

# ---------- 4. 数据库推送记录查询 ----------
print("\n\n📀 4. 本地数据库推送记录查询")
print("=" * 70)

try:
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # 查询特定币种的推送记录
    print("\n▶ 各币种推送记录（最多5条）:")
    for sym in symbols_to_check:
        cursor.execute("""
            SELECT count, first_at, sent_at 
            FROM trend_log 
            WHERE symbol = ? 
            ORDER BY sent_at DESC 
            LIMIT 5
        """, (sym,))
        rows = cursor.fetchall()
        print(f"\n{sym}:")
        if rows:
            for r in rows:
                print(f"  次数={r[0]}, 首次推送={r[1]}, 最近推送={r[2]}")
        else:
            print("  无任何推送记录")

    # 最近10条全局推送
    print("\n▶ 最近10条推送记录（全币种）:")
    cursor.execute("""
        SELECT symbol, count, sent_at 
        FROM trend_log 
        ORDER BY sent_at DESC 
        LIMIT 10
    """)
    rows = cursor.fetchall()
    if rows:
        for r in rows:
            print(f"  {r[0]} (次数={r[1]}) 时间={r[2]}")
    else:
        print("  trend_log 表为空")

    # 各表记录总数
    print("\n▶ 数据表记录统计:")
    for table in ['trend_log', 'channel_log', 'sent_log']:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        print(f"  {table}: {count} 条")

    conn.close()
except sqlite3.Error as e:
    print(f"数据库连接或查询错误: {e}")
except FileNotFoundError:
    print(f"数据库文件不存在: {DB_PATH}")
except Exception as e:
    print(f"其他错误: {e}")

print("\n" + "=" * 70)
print("✅ 诊断完成")
