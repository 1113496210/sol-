import sqlite3

conn = sqlite3.connect('wwwsignals.db')

print('1. 趋势推送记录')
for sym in ['PROVEUSDT', 'USELESSUSDT', '1000CHEEMSUSDT']
    cur = conn.execute('SELECT count, first_at, sent_at FROM trend_log WHERE symbol= ORDER BY sent_at DESC LIMIT 5', (sym,))
    rows = cur.fetchall()
    print(f'{sym}')
    if rows
        for r in rows
            print(f'  次数={r[0]} 首次={r[1]} 最近={r[2]}')
    else
        print('  无记录')

print('n2. 最近10条趋势推送')
cur = conn.execute('SELECT symbol, count, sent_at FROM trend_log ORDER BY sent_at DESC LIMIT 10')
rows = cur.fetchall()
for r in rows
    print(f'  {r[0]} 次数={r[1]} 时间={r[2]}')
if not rows
    print('  trend_log 表是空的')

print('n3. 表记录数')
for table in ['trend_log', 'channel_log', 'sent_log']
    cur = conn.execute(f'SELECT COUNT() FROM {table}')
    print(f'  {table} {cur.fetchone()[0]} 条')

conn.close()