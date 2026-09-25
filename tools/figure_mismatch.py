#!/usr/bin/env python3
"""The README figure: slim and fat nodes, side by side, at one real moment.

Reconstructs every node's allocation from the accounting history at time T,
finds the high-memory jobs that were waiting then, and draws each node's CPU
and memory allocation as paired bars, in a light and a dark SVG. Also prints
the history-wide numbers the README quotes.

  usage: figure_mismatch.py biocloud.sqlite "2026-07-20 13:12" docs/img/

Nodes out of service are not in the accounting data reliably (no events
were recorded for a 10-day outage of two nodes in July 2026). A node that ran
no job for more than OUT_OF_SERVICE seconds is treated as unavailable for that
whole gap. The history count is a lower bound: an out-of-service slim node is
never counted as room a waiting job could have had, while an idle fat node
always counts as room it did have -- even when it may in fact have been down,
or the waiting jobs held by a per-user cap instead.

Read-only on the database.
"""
import collections, re, sqlite3, sys, time

PART_NODES = {'zen3': [1, 2, 3, 4, 5, 6, 7], 'zen5': [12, 13, 16, 17],
              'zen3x': [8, 9], 'zen5x': [14, 15]}
NODE_PART = {f'bio-node{n:02d}': p for p, ns in PART_NODES.items() for n in ns}
SLIM, FAT = ('zen5', 'zen3'), ('zen5x', 'zen3x')          # each in PriorityTier order
FAT_RATIO = 6000          # MB per CPU: the site's static rule sends jobs above this to FAT
T0 = 1760000000           # zen* partition era begins
OUT_OF_SERVICE = 24 * 3600


def expand(nl):
    """'bio-node12' / 'bio-node[03-05,07]' / comma lists -> [names]"""
    out = []
    for tok in re.findall(r'[^,\[]+(?:\[[^\]]*\])?', nl or ''):
        tok = tok.strip()
        m = re.match(r'^(.*?)\[([0-9,\-]+)\]$', tok)
        if not tok:
            continue
        if not m:
            out.append(tok)
            continue
        pre, body = m.groups()
        for seg in body.split(','):
            if '-' in seg:
                a, b = seg.split('-')
                out += [f'{pre}{i:0{len(a)}d}' for i in range(int(a), int(b) + 1)]
            else:
                out.append(f'{pre}{seg}')
    return out


def capacities(db):
    cap = {}
    for name, tres in db.execute("select node_name, tres from event_table"):
        t = dict(kv.split('=') for kv in tres.split(',') if '=' in kv)
        if name in NODE_PART:
            cap[name] = (int(t.get('1', 0)), int(t.get('2', 0)))
    return cap


def outages(db, cap):
    """node -> [(start, end)] gaps of more than OUT_OF_SERVICE with no job running."""
    spans = collections.defaultdict(list)
    for nl, st, en in db.execute("""select nodelist, start, end from jobs
                                    where start >= ? and end > start and alloc_cpus > 0""", (T0,)):
        for n in expand(nl):
            if n in cap:
                spans[n].append((st, en))
    gaps = {}
    for n in cap:
        out, reach = [], T0
        for st, en in sorted(spans[n]):
            if st - reach > OUT_OF_SERVICE:
                out.append((reach, st))
            reach = max(reach, en)
        gaps[n] = out
    return gaps


def down(gaps, n, t):
    return any(a <= t < b for a, b in gaps.get(n, ()))


def snapshot(db, cap, t):
    """node -> (allocated cpus, allocated MB) at time t."""
    used = collections.defaultdict(lambda: [0.0, 0.0])
    for nl, c, m in db.execute("""select nodelist, alloc_cpus, alloc_mem_mb from jobs
                                  where start <= ? and end > ? and alloc_cpus > 0""", (t, t)):
        nodes = [n for n in expand(nl) if n in cap]
        for n in nodes:
            used[n][0] += c / len(nodes)
            used[n][1] += m / len(nodes)
    return used


def waiting_fat(db, t):
    """Single-node high-memory jobs eligible but not yet started at t."""
    return db.execute("""select id_job, cpus_req, mem_mb, partition, start - eligible from jobs
                         where eligible <= ? and start > ? and cpus_req > 0 and nodes_alloc = 1
                           and mem_mb * 1.0 / cpus_req > ?""", (t, t, FAT_RATIO)).fetchall()


def history(db, cap, gaps):
    """How often a high-memory job waited > 1 h while every fat node was too full
    for it and a slim node had room of its shape (checked 10 min after it
    became eligible). Event-driven, as in tools/stranding.py."""
    ev = collections.defaultdict(list)
    fat = []
    for nl, c, m, st, en, el, w, cr, mr in db.execute(
            """select nodelist, alloc_cpus, alloc_mem_mb, start, end, eligible, wait,
                      cpus_req, mem_mb from jobs
               where start >= ? and end > start and alloc_cpus > 0""", (T0,)):
        nodes = [n for n in expand(nl) if n in cap]
        if not nodes:
            continue
        for n in nodes:
            ev[st].append((n, c / len(nodes), m / len(nodes)))
            ev[en].append((n, -c / len(nodes), -m / len(nodes)))
        if w > 3600 and cr > 0 and mr / cr > FAT_RATIO and len(nodes) == 1:
            fat.append((el + 600, cr, mr, w))
    fat.sort()
    used = {n: [0.0, 0.0] for n in cap}
    fi = hits = 0
    wait_all = wait_hit = 0.0

    def room(parts, jc, jm, t, skip_down):
        return any(NODE_PART[n] in parts and not (skip_down and down(gaps, n, t))
                   and cap[n][0] - u[0] >= jc and cap[n][1] - u[1] >= jm
                   for n, u in used.items())
    for t in sorted(ev):
        while fi < len(fat) and fat[fi][0] <= t:
            _, jc, jm, w = fat[fi]
            fi += 1
            wait_all += w / 3600
            if room(SLIM, jc, jm, t, True) and not room(FAT, jc, jm, t, False):
                hits += 1
                wait_hit += w / 3600
        for n, dc, dm in ev[t]:
            used[n][0] += dc
            used[n][1] += dm
    return dict(jobs=len(fat), hits=hits, wait_all=wait_all, wait_hit=wait_hit)


# ---------------------------------------------------------------- drawing
THEMES = {
    'light': dict(surface='#fcfcfb', ink='#0b0b0b', ink2='#52514e', muted='#898781',
                  grid='#e1e0d9', axis='#c3c2b7', cpu='#2a78d6', mem='#eb6834'),
    'dark': dict(surface='#1a1a19', ink='#ffffff', ink2='#c3c2b7', muted='#898781',
                 grid='#2c2c2a', axis='#383835', cpu='#3987e5', mem='#d95926'),
}
FONT = "-apple-system, 'Segoe UI', Helvetica, Arial, sans-serif"


def bar(x, y, w, h, color, r=4):
    """Horizontal bar: square at the baseline, 4px rounded data end."""
    if w <= 0:
        return ''
    r = min(r, w, h / 2)
    return (f'<path d="M{x:.1f},{y:.1f} h{w - r:.1f} a{r},{r} 0 0 1 {r},{r} '
            f'v{h - 2 * r:.1f} a{r},{r} 0 0 1 -{r},{r} h-{w - r:.1f} z" fill="{color}"/>')


def text(x, y, s, color, size=12, weight=400, anchor='start'):
    s = s.replace('&', '&amp;').replace('<', '&lt;')
    return (f'<text x="{x:.1f}" y="{y:.1f}" fill="{color}" font-size="{size}" '
            f'font-weight="{weight}" text-anchor="{anchor}">{s}</text>')


def draw(theme, cap, used, waiting, when, off):
    """waiting: (jobid, cpus, mem_mb, partitions, total wait s)."""
    c = THEMES[theme]
    W, PAD, ROW, BW, BH, HEAD = 940, 24, 30, 150, 9, 20
    fits_any = {n for n in cap for (_, jc, jm, _, _) in waiting if n not in off
                and cap[n][0] - used[n][0] >= jc and cap[n][1] - used[n][1] >= jm}
    out = []

    def panel(x0, y0, title, note, groups):
        out.append(text(x0, y0, title, c['ink'], 13, 600))
        out.append(text(x0, y0 + 17, note, c['ink2'], 12))
        bx, y = x0 + 84, y0 + 32
        top = y
        for part, nodes in groups:
            out.append(text(x0, y + 12, part, c['muted'], 11, 600))
            y += HEAD
            for n in nodes:
                cc, cm = cap[n]
                uc, um = used[n]
                out.append(text(x0, y + BH + 5, n, c['ink2'], 12))
                if n in off:
                    out.append(text(bx, y + BH + 5, 'out of service', c['muted'], 11))
                else:
                    out.append(bar(bx, y, BW * min(1, uc / cc), BH, c['cpu']))
                    out.append(bar(bx, y + BH + 2, BW * min(1, um / cm), BH, c['mem']))
                    fits = n in fits_any
                    out.append(text(bx + BW + 10, y + BH + 5,
                                    f'{cc - uc:.0f} CPUs, {(cm - um) / 1024:,.0f} GB free',
                                    c['ink'] if fits else c['muted'], 11, 600 if fits else 400))
                y += ROW
        for k in (0, 0.5, 1):                                  # recessive grid, behind
            gx = bx + k * BW
            out.insert(0, f'<line x1="{gx:.1f}" y1="{top}" x2="{gx:.1f}" y2="{y - 6}" '
                          f'stroke="{c["grid"] if k else c["axis"]}" stroke-width="1"/>')
            out.append(text(gx, y + 8, f'{k:.0%}', c['muted'], 11, anchor='middle'))
        return y + 8

    def groups(parts):
        return [(p, sorted(n for n in cap if NODE_PART[n] == p)) for p in parts]

    total = [w for *_, w in waiting]
    # How many of them the slim nodes' free space could have taken right then,
    # first-fit, largest first: each one placed uses up what it takes.
    room = {n: [cap[n][0] - used[n][0], cap[n][1] - used[n][1]]
            for n in cap if NODE_PART[n] in SLIM and n not in off}
    placed = 0
    for _, jc, jm, _, _ in sorted(waiting, key=lambda j: -j[2]):
        for r in room.values():
            if r[0] >= jc and r[1] >= jm:
                r[0] -= jc
                r[1] -= jm
                placed += 1
                break
    big = collections.Counter((jc, round(jm / 1024)) for _, jc, jm, _, _ in waiting)
    y = PAD + 16
    out.append(text(PAD, y, f'Biocloud, {when}: memory ran out on the fat nodes, CPUs on the slim ones',
                    c['ink'], 16, 600))
    out.append(text(PAD, y + 22, f'{len(waiting)} high-memory jobs were waiting for a fat node; some went on '
                    f'to wait {max(total) / 3600:.0f} hours. The slim nodes had room for {placed} of them.',
                    c['ink2'], 13))
    ly = y + 46                                                # legend
    out.append(f'<rect x="{PAD}" y="{ly - 9}" width="12" height="9" rx="2" fill="{c["cpu"]}"/>')
    out.append(text(PAD + 18, ly, 'CPUs allocated', c['ink2'], 12))
    out.append(f'<rect x="{PAD + 128}" y="{ly - 9}" width="12" height="9" rx="2" fill="{c["mem"]}"/>')
    out.append(text(PAD + 146, ly, 'memory allocated', c['ink2'], 12))
    out.append(text(PAD + 272, ly, 'bold: free space a waiting job fits in', c['ink'], 12, 600))
    py = ly + 32
    b1 = panel(PAD, py, 'Slim nodes', 'CPUs fill first; memory is left over', groups(SLIM))
    x2 = W / 2 + 12
    b2 = panel(x2, py, 'Fat nodes', 'memory fills first; CPUs are left over', groups(FAT))
    wy = b2 + 34                                               # what was waiting
    out.append(text(x2, wy, 'Waiting for a fat node at that moment', c['ink'], 13, 600))
    for i, ((jc, gb), k) in enumerate(big.most_common(4)):
        out.append(text(x2, wy + 20 + 17 * i, f'{k} \u00d7  {jc} CPUs, {gb:,} GB', c['ink2'], 12))
    rest = len(waiting) - sum(k for _, k in big.most_common(4))
    if rest > 0:
        out.append(text(x2, wy + 20 + 17 * 4, f'{rest} more of other shapes', c['muted'], 12))
    H = max(b1, wy + 20 + 17 * 5) + PAD
    body = "\n".join(out)
    desc = (f"Paired bars per node showing the share of CPUs and memory allocated on {when}. "
            f"Slim nodes had most memory free; fat nodes had memory nearly full with CPUs idle. "
            f"{len(waiting)} high-memory jobs waited for fat nodes.")
    draw.placed = placed
    return (f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H:.0f}" '
            f'viewBox="0 0 {W} {H:.0f}" font-family="{FONT}" role="img">\n'
            f'<title>Node allocation on {when}</title><desc>{desc}</desc>\n'
            f'<rect width="100%" height="100%" rx="8" fill="{c["surface"]}"/>\n{body}\n</svg>\n')


def main():
    db_path, when, out_dir = sys.argv[1], sys.argv[2], sys.argv[3]
    db = sqlite3.connect(f'file:{db_path}?mode=ro', uri=True)
    cap = capacities(db)
    gaps = outages(db, cap)
    t = int(time.mktime(time.strptime(when, '%Y-%m-%d %H:%M')))
    used = snapshot(db, cap, t)
    off = {n for n in cap if down(gaps, n, t)}
    waiting = waiting_fat(db, t)
    label = time.strftime('%-d %B %Y, %H:%M', time.localtime(t))
    for theme in THEMES:
        path = f'{out_dir.rstrip("/")}/mismatch-{theme}.svg'
        open(path, 'w').write(draw(theme, cap, used, waiting, label, off))
        print('wrote', path)
    print('\nnode        CPUs used/total   memory used/total (GB)')
    for n in sorted(cap):
        print(f'{n}  {used[n][0]:>4.0f} / {cap[n][0]:<4}      {used[n][1] / 1024:>5.0f} / {cap[n][1] / 1024:.0f}'
              + ('   out of service' if n in off else ''))
    print(f'\nwaiting high-memory jobs at {label}: {len(waiting)}; '
          f'the slim nodes had room for {draw.placed} of them at once')
    h = history(db, cap, gaps)
    down_h = sum(b - a for g in gaps.values() for a, b in g) / 3600
    print(f'\nnode-hours treated as out of service (> {OUT_OF_SERVICE // 3600} h without a job): {down_h:,.0f}')
    print(f'\nsince the zen* partitions: {h["jobs"]:,} high-memory single-node jobs waited > 1 h;'
          f'\n  at least {h["hits"]:,} ({100 * h["hits"] / h["jobs"]:.1f}%) did so while every fat node was too full'
          f' and a slim node had room of their shape,'
          f'\n  accounting for {h["wait_hit"]:,.0f} of {h["wait_all"]:,.0f} wait-hours '
          f'({100 * h["wait_hit"] / h["wait_all"]:.0f}%)')


if __name__ == '__main__':
    main()
