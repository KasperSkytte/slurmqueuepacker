#!/usr/bin/env python3
"""First-pass characterisation of the Slurm accounting history."""
import sqlite3, sys, math, datetime as dt, collections

db = sqlite3.connect(sys.argv[1] if len(sys.argv) > 1 else 'biocloud.sqlite')
db.row_factory = sqlite3.Row
q = lambda s, *a: db.execute(s, a).fetchall()

def ts(x): return dt.datetime.utcfromtimestamp(int(x)).strftime('%Y-%m-%d') if x else '-'

print('=== 1. coverage ===')
for r in q("""select count(*) n, min(time_submit) a, max(time_submit) b,
              sum(time_start>0) started, sum(time_start=0) never_started
              from job_table"""):
    print(f"  jobs={r['n']:,}  {ts(r['a'])} .. {ts(r['b'])}  started={r['started']:,}  never_started={r['never_started']:,}")

print('\n=== 2. jobs & cpu-hours per month ===')
for r in q("""select strftime('%Y-%m', time_submit,'unixepoch') mo, count(*) n,
              round(sum(cpus_req*(time_end-time_start))/3600.0) cpuh,
              count(distinct id_user) users
              from job_table where time_start>0 and time_end>time_start
              group by mo order by mo"""):
    print(f"  {r['mo']}  jobs={r['n']:>8,}  cpu_h={r['cpuh']:>10,}  users={r['users']:>3}")

print('\n=== 3. partitions used (whole history) ===')
for r in q("""select partition, count(*) n,
              round(sum(cpus_req*(time_end-time_start))/3600.0) cpuh
              from job_table where time_start>0 group by partition
              order by cpuh desc limit 20"""):
    print(f"  {r['partition']:<20} jobs={r['n']:>8,}  cpu_h={r['cpuh']:>10,}")

print('\n=== 4. distinct job states ===')
for r in q("select state, count(*) n from job_table group by state order by n desc limit 15"):
    print(f"  state={r['state']:<4} n={r['n']:,}")
