#!/usr/bin/env python3
"""Build typed, analysis-ready `jobs` and `jobstats` tables from the raw import."""
import sqlite3, sys

MEM_PER_CPU = 0x8000000000000000
DB = sys.argv[1] if len(sys.argv) > 1 else 'biocloud.sqlite'
db = sqlite3.connect(DB)
db.execute('PRAGMA journal_mode=OFF'); db.execute('PRAGMA synchronous=OFF')

def tres(s):
    out = {}
    for kv in (s or '').split(','):
        if '=' in kv:
            k, v = kv.split('=', 1)
            try: out[int(k)] = int(v)
            except ValueError: pass
    return out

assoc = {int(a): (u or '', c or '')
         for a, u, c in db.execute("select id_assoc,user,acct from assoc_table")}
qos = {int(i): n for i, n in db.execute("select id,name from qos_table")}

db.execute('DROP TABLE IF EXISTS jobs')
db.execute("""CREATE TABLE jobs (
  dbx TEXT PRIMARY KEY, id_job INTEGER, array_job INTEGER, array_task INTEGER,
  user TEXT, acct TEXT, partition TEXT, qos TEXT, name TEXT, work_dir TEXT,
  cpus_req INTEGER, mem_mb INTEGER, mem_per_cpu INTEGER, gpus INTEGER,
  timelimit_min INTEGER, submit INTEGER, eligible INTEGER, start INTEGER,
  end INTEGER, elapsed INTEGER, wait INTEGER, state INTEGER,
  nodes_alloc INTEGER, nodelist TEXT, node_inx TEXT,
  alloc_cpus INTEGER, alloc_mem_mb INTEGER, script_hash TEXT, priority INTEGER)""")

rows = []; n = 0
sel = """select job_db_inx,id_job,id_array_job,id_array_task,id_assoc,id_qos,partition,
         job_name,work_dir,cpus_req,mem_req,timelimit,time_submit,time_eligible,
         time_start,time_end,state,nodes_alloc,nodelist,node_inx,tres_req,tres_alloc,
         script_hash_inx,priority from job_table"""
ins = 'INSERT INTO jobs VALUES (' + ','.join('?' * 29) + ')'
for r in db.execute(sel):
    (dbx, idj, aj, at, ia, iq, part, name, wd, creq, mreq, tl,
     sub, elig, start, end, state, na, nl, ninx, treq, talloc, sh, prio) = r
    i = lambda x: int(x) if x not in (None, '') else 0
    treq_d, tall_d = tres(treq), tres(talloc)
    mreq = i(mreq)
    per_cpu = 1 if mreq & MEM_PER_CPU else 0
    mem_mb = treq_d.get(2) or (mreq & 0x7FFFFFFFFFFFFFFF)
    start, end, elig = i(start), i(end), i(elig)
    u, a = assoc.get(i(ia), ('', ''))
    rows.append((str(dbx), i(idj), i(aj), i(at), u, a, part, qos.get(i(iq), ''),
                 name, wd, i(creq), mem_mb, per_cpu, treq_d.get(1001, 0),
                 i(tl), i(sub), elig, start, end,
                 (end - start) if start and end > start else 0,
                 (start - elig) if start and elig and start > elig else 0,
                 i(state), i(na), nl, ninx,
                 tall_d.get(1, 0), tall_d.get(2, 0), str(sh), i(prio)))
    if len(rows) >= 50000:
        db.executemany(ins, rows); n += len(rows); rows.clear()
db.executemany(ins, rows); n += len(rows)
print(f'jobs: {n:,}')

print('aggregating steps...')
db.execute('DROP TABLE IF EXISTS jobstats')
db.execute("""CREATE TABLE jobstats (
  dbx TEXT PRIMARY KEY, tot_cpu_sec INTEGER, max_rss_mb INTEGER, nsteps INTEGER)""")
agg = {}
for dbx, us, ss, mx in db.execute(
        "select job_db_inx,user_sec,sys_sec,tres_usage_in_max from step_table"):
    dbx = str(dbx)
    cpu = (int(us or 0) + int(ss or 0))
    rss = tres(mx).get(2, 0) // (1024 * 1024)
    a = agg.get(dbx)
    if a is None: agg[dbx] = [cpu, rss, 1]
    else:
        a[0] += cpu; a[1] = max(a[1], rss); a[2] += 1
db.executemany('INSERT INTO jobstats VALUES (?,?,?,?)',
               ((k, v[0], v[1], v[2]) for k, v in agg.items()))
print(f'jobstats: {len(agg):,}')

for s in ('CREATE INDEX ix_jobs_start ON jobs(start)',
          'CREATE INDEX ix_jobs_end ON jobs(end)',
          'CREATE INDEX ix_jobs_user ON jobs(user)',
          'CREATE INDEX ix_jobs_sh ON jobs(script_hash)',
          'CREATE INDEX ix_jobs_part ON jobs(partition)'):
    db.execute(s)
db.commit(); print('done')
