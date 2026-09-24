#!/usr/bin/env python3
"""Characterise the biocloud workload for queue-placement design.

Everything here is descriptive: no model is fit, nothing is written back.
"""
import sqlite3, sys, math, datetime as dt, collections, statistics as st

DB = sys.argv[1] if len(sys.argv) > 1 else 'biocloud.sqlite'
db = sqlite3.connect(DB); db.row_factory = sqlite3.Row
q = lambda s, *a: db.execute(s, a).fetchall()
day = lambda x: dt.datetime.utcfromtimestamp(int(x)).strftime('%Y-%m-%d') if x else '-'

MEM_PER_CPU = 0x8000000000000000

def tres(s):
    """'1=128,2=1024000,4=1' -> {1:128, 2:1024000, 4:1}"""
    out = {}
    if not s: return out
    for kv in s.split(','):
        if '=' in kv:
            k, v = kv.split('=', 1)
            try: out[int(k)] = int(v)
            except ValueError: pass
    return out

def pct(xs, p):
    xs = sorted(xs)
    if not xs: return float('nan')
    i = min(len(xs) - 1, max(0, int(round(p / 100 * (len(xs) - 1)))))
    return xs[i]

print('=== TRES ids ===')
for r in q("select id, type, name from tres_table order by id"):
    print(f"  {r['id']:>5}  {r['type']}{'/'+r['name'] if r['name'] else ''}")

print('\n=== coverage ===')
for r in q("""select count(*) n, min(time_submit) a, max(time_submit) b,
              sum(time_start>0) started from job_table"""):
    print(f"  jobs={r['n']:,}  {day(r['a'])} .. {day(r['b'])}  started={r['started']:,}")

print('\n=== monthly volume ===')
for r in q("""select strftime('%Y-%m', time_submit,'unixepoch') mo, count(*) n,
              round(sum(cpus_req*(time_end-time_start))/3600.0) cpuh,
              count(distinct id_user) users, count(distinct partition) parts
              from job_table where time_start>0 and time_end>time_start
              group by mo order by mo"""):
    print(f"  {r['mo']}  jobs={r['n']:>8,}  cpu_h={r['cpuh']:>10,}  users={r['users']:>3}")

print('\n=== partition usage ===')
for r in q("""select partition, count(*) n,
              round(sum(cpus_req*(time_end-time_start))/3600.0) cpuh
              from job_table where time_start>0 and time_end>time_start
              group by partition order by cpuh desc limit 15"""):
    print(f"  {r['partition']:<24} jobs={r['n']:>8,}  cpu_h={r['cpuh']:>10,}")
