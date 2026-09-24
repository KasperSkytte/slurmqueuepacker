#!/usr/bin/env python3
"""Stream a mysqldump of the Slurm accounting DB into a SQLite file.

Reads only the tables/columns needed for scheduling analysis, so the result is
a fraction of the dump size and can be queried with plain SQL. Does not touch
any live database.

  usage: dump2sqlite.py <dump.sql> <out.sqlite> [--cluster biocloud]
"""
import re, sys, sqlite3, os

STR = re.compile(rb"'((?:[^'\\]|\\.)*)'")
UNESC = {b'0': b'\0', b'n': b'\n', b'r': b'\r', b'Z': b'\x1a',
         b'b': b'\b', b't': b'\t'}

# table suffix -> columns to keep (None = keep all)
WANT = {
    'job_table': """job_db_inx account admin_comment array_max_tasks constraints
        cpus_req exit_code job_name id_assoc id_array_job id_array_task id_job
        id_qos id_resv id_user id_group het_job_id mem_req nodelist nodes_alloc
        node_inx partition priority state timelimit time_submit time_eligible
        time_start time_end time_suspended gres_used tres_alloc tres_req flags
        restart_cnt script_hash_inx state_reason_prev work_dir submit_line""".split(),
    'step_table': """job_db_inx id_step step_name nodes_alloc nodelist task_cnt
        state exit_code time_start time_end time_suspended user_sec sys_sec
        tres_alloc tres_usage_in_ave tres_usage_in_max tres_usage_in_tot
        tres_usage_out_tot""".split(),
    'event_table': None,
    'resv_table': None,
    'assoc_table': None,
    'wckey_table': None,
}
GLOBAL = {'qos_table': None, 'tres_table': None, 'user_table': None,
          'acct_table': None, 'cluster_table': None, 'txn_table': None}


def unescape(b):
    if b'\\' not in b:
        return b.decode('utf-8', 'replace')
    out = bytearray(); i = 0; n = len(b)
    while i < n:
        c = b[i:i + 1]
        if c == b'\\' and i + 1 < n:
            nxt = b[i + 1:i + 2]
            out += UNESC.get(nxt, nxt); i += 2
        else:
            out += c; i += 1
    return out.decode('utf-8', 'replace')


def split_tuple(s):
    """Split the inside of a mysqldump VALUES tuple into python values."""
    vals = []; pos = 0; n = len(s)
    while True:
        if pos < n and s[pos:pos + 1] == b"'":
            m = STR.match(s, pos)
            vals.append(unescape(m.group(1))); pos = m.end()
        else:
            j = s.find(b',', pos)
            if j < 0:
                j = n
            raw = s[pos:j]
            vals.append(None if raw == b'NULL' else raw.decode())
            pos = j
        if pos >= n:
            break
        pos += 1  # skip comma
    return vals


def main(dump, out, cluster):
    if os.path.exists(out):
        os.remove(out)
    db = sqlite3.connect(out)
    db.execute('PRAGMA journal_mode=OFF'); db.execute('PRAGMA synchronous=OFF')

    targets = {}                      # dump table name -> local name
    for suf in WANT:
        targets[f'{cluster}_{suf}'] = suf
    for t in GLOBAL:
        targets[t] = t

    schema = {}                       # dump table -> [column names]
    cur_tbl = None; cols = []
    ins = {}                          # local name -> (sql, keep_idx, buffer)
    counts = {}
    active = None

    def flush(local):
        sql, idx, buf = ins[local]
        if buf:
            db.executemany(sql, buf)
            counts[local] = counts.get(local, 0) + len(buf)
            buf.clear()

    with open(dump, 'rb') as f:
        for line in f:
            if line.startswith(b'CREATE TABLE '):
                cur_tbl = line.split(b'`')[1].decode(); cols = []
                continue
            if cur_tbl is not None:
                if line.startswith(b') ENGINE'):
                    schema[cur_tbl] = cols; cur_tbl = None
                elif line.startswith(b'  `'):
                    name = line.split(b'`')[1].decode()
                    if not name.startswith(('PRIMARY', 'KEY', 'UNIQUE')):
                        cols.append(name)
                continue

            if line.startswith(b'INSERT INTO '):
                t = line.split(b'`')[1].decode()
                active = targets.get(t)
                if active and active not in ins:
                    all_cols = schema[t]
                    want = WANT[active] if active in WANT else GLOBAL.get(active)
                    keep = [(i, c) for i, c in enumerate(all_cols)
                            if want is None or c in want]
                    names = [c for _, c in keep]
                    db.execute('CREATE TABLE "%s" (%s)' % (
                        active, ','.join('"%s"' % c for c in names)))
                    ins[active] = ('INSERT INTO "%s" VALUES (%s)' % (
                        active, ','.join('?' * len(names))),
                        [i for i, _ in keep], [])
                continue

            if line.startswith(b'(') and active:
                s = line.rstrip()
                end = s.endswith(b');')
                vals = split_tuple(s[1:-2])
                sql, idx, buf = ins[active]
                try:
                    buf.append([vals[i] for i in idx])
                except IndexError:
                    continue
                if end:
                    flush(active); active = None
                elif len(buf) >= 20000:
                    flush(active)
                continue

    for local in ins:
        flush(local)
    db.commit()
    for k, v in sorted(counts.items()):
        print(f'  {k:20s} {v:>10,} rows')
    print('wrote', out, f'{os.path.getsize(out)/1e6:.0f} MB')


if __name__ == '__main__':
    cl = sys.argv[sys.argv.index('--cluster') + 1] if '--cluster' in sys.argv else 'biocloud'
    main(sys.argv[1], sys.argv[2], cl)
