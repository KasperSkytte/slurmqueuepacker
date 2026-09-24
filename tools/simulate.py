#!/usr/bin/env python3
"""Trace-driven replay of the biocloud queue.

Objective: minimise queue time, maximise CPU and memory *allocation* hours.
Allocation is what denies hardware to other people, so allocation is the unit.
Job CPU efficiency is deliberately not modelled and not scored: a job cannot be
100% efficient, and penalising that would punish users for their workload.

The one efficiency that does matter is *time*: users overshoot their time limits
by ~7x, which makes Slurm's backfill reservations far too pessimistic. Replaying
with reservation horizons taken from cohort history instead of declared limits
measures how much room that leaves to pack better.

  usage: simulate.py --start 2026-03-09 --days 14 [--jobs 8]
"""
import sqlite3, argparse, heapq, collections, os, sys, datetime as dt
import multiprocessing as mp

# ---------------------------------------------------------------- topology
PART_NODES = {'zen3': [1, 2, 3, 4, 5, 6, 7], 'zen3x': [8, 9],
              'zen5': [12, 13, 16, 17], 'zen5x': [14, 15]}
NODE_PART = {f'bio-node{n:02d}': p for p, ns in PART_NODES.items() for n in ns}
TIER = {'zen5': 10, 'zen3': 9, 'zen5x': 8, 'zen3x': 7}
SPEED = {'zen5': 1.00, 'zen5x': 1.00, 'zen3': 0.80, 'zen3x': 0.80}
SLIM, FAT = ('zen5', 'zen3'), ('zen5x', 'zen3x')

# demand ratio quantiles (MB/CPU), allocation-hour weighted, measured zen* era
DEMAND = [(800, .10), (1280, .15), (4267, .25), (7680, .25),
          (14178, .15), (30720, .10)]
Q50 = 4267          # demand median, for the reported stranding metric
TOL = 0.25          # partition-set tolerance, in CPUs of option value per job CPU

# QOS 'normal' on biocloud: MaxTRESPU=cpu=864, MaxTRESPA=cpu=1760.
# These, not resource exhaustion, are what actually holds most jobs in the queue.
MAX_CPU_PER_USER = 864
MAX_CPU_PER_ACCT = 1760

# Elastic relief. The caps exist to stop one user swallowing the cluster in a
# minute, not to ration it. When capacity is genuinely idle, individual PENDING
# jobs are moved to a 'flex' QOS that lifts the cap -- one at a time, and only
# ever a job that can start immediately, so the promotion rate is bounded by the
# rate at which resources actually free up. FLEX_CEILING is the hard stop: even
# with relief, no user exceeds this multiple of the base cap.
FLEX_CEILING = 2.0
FLEX_RESERVE = 0.05        # fraction of cluster placeable capacity kept in hand

# Global elastic limits: raise the cap for EVERYONE, uniformly, when the cluster
# is persistently idle; snap straight back the moment it is not. Fair by
# construction -- every user gets the same number -- and fair-share keeps
# deciding who fills the room. Raising is slow and stepwise; lowering is instant.
GL_RAISE_ABOVE = 0.25      # idle placeable fraction above which to step up
GL_LOWER_BELOW = 0.10      # below which to snap back to the base cap
GL_HYSTERESIS  = 5         # consecutive control intervals before stepping up
GL_STEP        = 1.25      # multiplicative step per raise
# Relief must not spend the option value the packer accumulated: a promoted job
# may not destroy more placeable capacity than the cores it actually consumes.
FLEX_PHI_TOL = 1.0


def load_caps(db):
    cap = {}
    for name, tres in db.execute("select node_name,tres from event_table"):
        if name in NODE_PART:
            t = dict(kv.split('=') for kv in tres.split(',') if '=' in kv)
            cap[name] = (int(t['1']), int(t['2']))
    return cap


def phi_node(fc, fm):
    """Placeable capacity: free CPUs usable under the measured demand mix.

    Marginal use only -- as an absolute quantity it charges ~25% of an idle
    zen3 node as unusable, because the top demand deciles fit on no node.
    """
    return sum(w * min(fc, fm / q) for q, w in DEMAND)


def stranded(fc, fm):
    """Idle CPUs a median job could not use, for want of memory on their node."""
    return max(0.0, fc - fm / Q50)


# ---------------------------------------------------------------- policies
def pol_actual(job, st):
    return [job.actual_part]


def pol_static(job, st):
    """Reproduces the current job_submit.lua."""
    return list(SLIM) if job.mem / job.cpus < 6000 else list(FAT)


def pol_feasible(job, st):
    """Any partition holding a node physically big enough for the job."""
    out = [p for p in TIER
           if any(st.cap[n][0] >= job.cpus and st.cap[n][1] >= job.mem
                  for n in st.nodes_of[p])]
    return out or list(FAT)


def pol_packer(job, st):
    """Gate the feasible set by option-value cost.

        cost(p) = phi destroyed  -  speed(p) x cpus

    Idle cluster: phi is large, so preserving it dominates and placement comes
    out ratio-matched. Busy cluster: phi is already near zero, the work term
    dominates, and anything that fits is admitted. No mode switch.
    """
    feas = pol_feasible(job, st)
    scored = []
    for p in feas:
        best = None
        for n in st.nodes_of[p]:
            fc, fm = st.free(n)
            if fc < job.cpus or fm < job.mem:
                continue
            loss = phi_node(fc, fm) - phi_node(fc - job.cpus, fm - job.mem)
            c = loss - SPEED[p] * job.cpus
            if best is None or c < best:
                best = c
        if best is not None:
            scored.append((best, p))
    if not scored:
        return feas
    scored.sort()
    if st.starving(job):                       # starvation guard: widen
        return [p for _, p in scored]
    # absolute tolerance, so the band does not scale with the score magnitude
    lo = scored[0][0]
    return [p for c, p in scored if c <= lo + TOL * job.cpus]


POLICIES = {'actual': pol_actual, 'static': pol_static,
            'feasible': pol_feasible, 'packer': pol_packer}


# ---------------------------------------------------------------- model
class Job:
    __slots__ = ('id', 'user', 'acct', 'elig', 'cpus', 'mem', 'dur', 'tl',
                 'prio', 'actual_part', 'actual_wait', 'pred', 'start', 'node')
    def __init__(self, *a):
        (self.id, self.user, self.acct, self.elig, self.cpus, self.mem,
         self.dur, self.tl, self.prio, self.actual_part, self.actual_wait,
         self.pred) = a
        self.start = None; self.node = None


class State:
    def __init__(self, cap, budget):
        self.cap = cap
        self.used = {n: [0, 0] for n in cap}
        self.nodes_of = collections.defaultdict(list)
        for n in cap: self.nodes_of[NODE_PART[n]].append(n)
        self.now = 0
        self.budget = budget
        self.user_cap = MAX_CPU_PER_USER      # moved by the global controller
        self.acct_cap = MAX_CPU_PER_ACCT
        self.user_cpu = collections.Counter()
        self.acct_cpu = collections.Counter()
        self.blocked = (0, 0)      # (by QOS cap, by resources) at the last pass

    def limited(self, job, ceiling=1.0):
        return (self.user_cpu[job.user] + job.cpus > self.user_cap * ceiling or
                self.acct_cpu[job.acct] + job.cpus > self.acct_cap * ceiling)

    def placeable(self):
        return sum(phi_node(*self.free(n)) for n in self.cap)

    def free(self, n):
        c, m = self.cap[n]; u = self.used[n]
        return c - u[0], m - u[1]

    def starving(self, job):
        cls = 'fat' if job.mem / job.cpus > 6000 else 'slim'
        return (self.now - job.elig) > self.budget[cls]


def run(jobs, cap, policy, budget, win, resv_mode='timelimit',
        reservations=4, scan=3000, tick=300, flex='off'):
    """resv_mode: 'timelimit' = what Slurm does (declared limits).
                  'predicted' = cohort-history horizon, capped by the limit."""
    st = State(cap, budget)
    jobs = sorted(jobs, key=lambda j: j.elig)
    ji = 0; seq = 0
    pending = []; running = []; done = []
    cpu_ch = 0.0; mem_gbh = 0.0; strand_ch = 0.0; cap_ch = 0.0
    blk_lim = 0.0; blk_res = 0.0        # job-hours spent blocked, by cause
    n_flex = [0, 0]                     # jobs promoted to flex, and their CPUs
    gl_streak = 0                       # consecutive idle intervals seen
    gl_peak = 1.0                       # highest cap multiple reached
    gl_raised_h = 0.0                   # hours spent above the base cap
    w0, w1 = win
    prev = jobs[0].elig
    makespan_end = 0
    total_cpus = sum(c for c, _ in cap.values())

    def accrue(t):
        nonlocal cpu_ch, mem_gbh, strand_ch, cap_ch, blk_lim, blk_res, prev, gl_raised_h
        a, b = max(prev, w0), min(t, w1); prev = t
        dt_h = (b - a) / 3600.0
        if dt_h <= 0:
            return
        for n in cap:
            fc, fm = st.free(n)
            cpu_ch += (cap[n][0] - fc) * dt_h
            mem_gbh += (cap[n][1] - fm) / 1024.0 * dt_h
            strand_ch += stranded(fc, fm) * dt_h
        cap_ch += total_cpus * dt_h
        blk_lim += st.blocked[0] * dt_h; blk_res += st.blocked[1] * dt_h
        if st.user_cap > MAX_CPU_PER_USER:
            gl_raised_h += dt_h

    while ji < len(jobs) or pending or running:
        cands = []
        if ji < len(jobs): cands.append(jobs[ji].elig)
        if running: cands.append(running[0][0])
        if not cands: break
        t = min(cands)
        if pending and t > st.now:
            t = min(t, st.now + tick)
        accrue(t); st.now = t

        while running and running[0][0] <= t:
            e, _, n, c, m, _, ju, ja = heapq.heappop(running)
            st.used[n][0] -= c; st.used[n][1] -= m
            st.user_cpu[ju] -= c; st.acct_cpu[ja] -= c
            makespan_end = max(makespan_end, e)
        while ji < len(jobs) and jobs[ji].elig <= t:
            pending.append(jobs[ji]); ji += 1

        def start(job, node):
            nonlocal seq
            st.used[node][0] += job.cpus; st.used[node][1] += job.mem
            st.user_cpu[job.user] += job.cpus; st.acct_cpu[job.acct] += job.cpus
            job.start = t; job.node = node
            dur = job.dur * SPEED[job.actual_part] / SPEED[NODE_PART[node]]
            horizon = job.tl if resv_mode == 'timelimit' else min(job.pred, job.tl)
            seq += 1
            heapq.heappush(running, (t + dur, seq, node, job.cpus, job.mem,
                                     t + horizon, job.user, job.acct))
            done.append(job)

        # ---- global elastic limit controller
        if flex == 'global':
            idle_frac = st.placeable() / total_cpus
            if idle_frac >= GL_RAISE_ABOVE:
                gl_streak += 1
                if gl_streak >= GL_HYSTERESIS:
                    st.user_cap = min(st.user_cap * GL_STEP,
                                      MAX_CPU_PER_USER * FLEX_CEILING)
                    st.acct_cap = min(st.acct_cap * GL_STEP,
                                      MAX_CPU_PER_ACCT * FLEX_CEILING)
                    gl_streak = 0
            elif idle_frac < GL_LOWER_BELOW:
                st.user_cap = MAX_CPU_PER_USER      # instant snap-back
                st.acct_cap = MAX_CPU_PER_ACCT
                gl_streak = 0
            else:
                gl_streak = 0
            gl_peak = max(gl_peak, st.user_cap / MAX_CPU_PER_USER)

        pending.sort(key=lambda j: (-j.prio, j.elig))
        resv = {}; still = []; capped = []
        n_lim = n_res = 0
        for job in pending[:scan]:
            if st.limited(job):                 # QOS cap: cannot start now
                still.append(job); capped.append(job); n_lim += 1; continue
            parts = policy(job, st)
            node = pick(st, job, parts, resv, t)
            if node is not None:
                start(job, node)
            else:
                if len(resv) < reservations:
                    add_resv(st, job, parts, resv, running, t)
                still.append(job); n_res += 1

        # ---- elastic relief. The guard and the relief work together, not against
        # each other: a job that is BOTH overdue and capped is the first thing to
        # promote, since the cap is precisely why it is overdue. Discretionary
        # relief pauses only while some overdue job is blocked by *resources*,
        # which is the capacity a promotion would consume.
        if flex == 'perjob' and capped:
            cap_ids = {id(j) for j in capped}
            res_starving = any(st.starving(j) and id(j) not in cap_ids
                               for j in still)
            order = sorted(capped,
                           key=lambda j: (not st.starving(j), -j.prio, j.elig))
            floor = FLEX_RESERVE * total_cpus
            for job in order:
                if res_starving and not st.starving(job):
                    continue
                if st.placeable() <= floor:
                    break
                if st.limited(job, FLEX_CEILING):        # hard per-user stop
                    continue
                node = pick(st, job, policy(job, st), resv, t)
                if node is None:                          # must start right now
                    continue
                fc, fm = st.free(node)
                loss = phi_node(fc, fm) - phi_node(fc - job.cpus, fm - job.mem)
                if loss > job.cpus * FLEX_PHI_TOL:        # would strand capacity
                    continue
                start(job, node); still.remove(job); n_lim -= 1
                n_flex[0] += 1; n_flex[1] += job.cpus

        still.extend(pending[scan:])
        pending = still
        st.blocked = (n_lim, n_res)
    accrue(st.now)
    return done, dict(cpu_ch=cpu_ch, mem_gbh=mem_gbh, strand_ch=strand_ch,
                      cap_ch=cap_ch, makespan=makespan_end - w0,
                      blk_lim=blk_lim, blk_res=blk_res,
                      flex_jobs=n_flex[0], flex_cpus=n_flex[1],
                      gl_peak=gl_peak, gl_raised_h=gl_raised_h)


def pick(st, job, parts, resv, now):
    best = None; bestkey = None
    for p in sorted(parts, key=lambda p: -TIER.get(p, 0)):
        for n in st.nodes_of.get(p, ()):
            fc, fm = st.free(n)
            if fc < job.cpus or fm < job.mem:
                continue
            r = resv.get(n)
            if r:
                rt, rc, rm = r
                if now + job.tl > rt and (fc - job.cpus < rc or fm - job.mem < rm):
                    continue
            busy = 0 if st.used[n][0] > 0 else 1        # bf_busy_nodes
            key = (-TIER.get(p, 0), busy, fc - job.cpus, fm - job.mem)
            if bestkey is None or key < bestkey:
                bestkey = key; best = n
        if best is not None:
            return best
    return best


def add_resv(st, job, parts, resv, running, now):
    """Earliest start, from what the scheduler BELIEVES about running jobs --
    their declared deadlines, not their real end times."""
    for p in sorted(parts, key=lambda p: -TIER.get(p, 0)):
        for n in st.nodes_of.get(p, ()):
            c, m = st.cap[n]
            if c < job.cpus or m < job.mem:
                continue
            ends = sorted((dl, cc, mm)
                          for _, _, nn, cc, mm, dl, _, _ in running if nn == n)
            fc, fm = st.free(n)
            if fc >= job.cpus and fm >= job.mem:
                resv[n] = (now, job.cpus, job.mem); return
            for dl, cc, mm in ends:
                fc += cc; fm += mm
                if fc >= job.cpus and fm >= job.mem:
                    resv[n] = (dl, job.cpus, job.mem); return


# ---------------------------------------------------------------- data
def build_runtime_pred(db, before, q=0.95, minn=20):
    """Reservation horizon from cohort history strictly before the window.

    Cohort-p95 over (user, name, cpus, mem). Measured on the zen* era: shrinks
    the horizon to 21.7% of declared limits, 6.3% of jobs outlive it, median
    overrun 2 minutes.
    """
    acc = collections.defaultdict(list)
    for u, nm, c, m, el in db.execute(
            """select user,name,cpus_req,mem_mb,elapsed from jobs
               where end<? and elapsed>0""", (before,)):
        acc[(u, nm, c, m)].append(el)
        acc[(u, nm)].append(el)
    out = {}
    for k, v in acc.items():
        if len(v) >= minn:
            v.sort(); out[k] = v[min(len(v) - 1, int(q * (len(v) - 1)))]
    return out


def load(db, t_from, t_to, pred):
    rows = db.execute("""select id_job,user,acct,eligible,cpus_req,mem_mb,elapsed,
                         timelimit_min,priority,partition,wait,name from jobs
                         where eligible>=? and eligible<? and start>0 and end>start
                           and cpus_req>0 and mem_mb>0""",
                      (t_from, t_to)).fetchall()
    out = []
    for (idj, user, acct, elig, cpus, mem, el, tlm, prio, part, wait, name) in rows:
        if part not in TIER:
            continue
        cpus, mem, el = int(cpus), int(mem), max(int(el), 1)
        tl = min(max(int(tlm), 1), 20160) * 60
        p = pred.get((user, name, cpus, mem)) or pred.get((user, name)) or tl
        out.append(Job(idj, user, acct, int(elig), cpus, mem, el, tl,
                       int(prio), part, int(wait), max(int(p * 1.25), 300)))
    return sorted(out, key=lambda j: j.elig)


def pctl(xs, p):
    if not xs: return 0.0
    xs = sorted(xs); return xs[min(len(xs) - 1, int(p / 100 * (len(xs) - 1)))]


# ---------------------------------------------------------------- driver
_G = {}

def _task(spec):
    pol, resv_mode, flex = spec
    jobs = [Job(j.id, j.user, j.acct, j.elig, j.cpus, j.mem, j.dur, j.tl,
                j.prio, j.actual_part, j.actual_wait, j.pred) for j in _G['jobs']]
    done, m = run(jobs, _G['cap'], POLICIES[pol], _G['budget'], _G['win'],
                  resv_mode=resv_mode, reservations=_G['nresv'], flex=flex)
    w0 = _G['win'][0]
    sc = [j for j in done if j.start is not None and j.elig >= w0]
    waits = [j.start - j.elig for j in sc]
    fat = [j.start - j.elig for j in sc if j.mem / j.cpus > 6000]
    return (pol, flex, m, len(sc), sum(waits) / 3600,
            sum(fat) / 3600, pctl(waits, 99) / 3600, pctl(fat, 99) / 3600)


def main():
    global FLEX_CEILING, FLEX_RESERVE, FLEX_PHI_TOL
    global GL_RAISE_ABOVE, GL_LOWER_BELOW, GL_HYSTERESIS, GL_STEP
    ap = argparse.ArgumentParser()
    ap.add_argument('--db', default='biocloud.sqlite')
    ap.add_argument('--start', required=True)
    ap.add_argument('--days', type=int, default=14)
    ap.add_argument('--warmup', type=int, default=4)
    ap.add_argument('--jobs', type=int, default=min(8, os.cpu_count() or 1))
    ap.add_argument('--flex-ceiling', type=float, default=FLEX_CEILING,
                    help='hard stop: max multiple of the base cap, both modes')
    ap.add_argument('--flex-reserve', type=float, default=FLEX_RESERVE,
                    help='per-job mode: fraction of placeable capacity kept in hand')
    ap.add_argument('--flex-phi-tol', type=float, default=FLEX_PHI_TOL,
                    help='per-job mode: max phi destroyed per CPU consumed')
    ap.add_argument('--gl-raise-above', type=float, default=GL_RAISE_ABOVE)
    ap.add_argument('--gl-lower-below', type=float, default=GL_LOWER_BELOW)
    ap.add_argument('--gl-hysteresis', type=int, default=GL_HYSTERESIS)
    ap.add_argument('--gl-step', type=float, default=GL_STEP)
    ap.add_argument('--reservations', type=int, default=4,
                    help='outstanding backfill reservations; 4 ~ EASY, 50+ ~ conservative')
    ap.add_argument('--budget-fat', type=float, default=1.0)
    ap.add_argument('--budget-slim', type=float, default=4.0)
    a = ap.parse_args()

    FLEX_CEILING, FLEX_RESERVE, FLEX_PHI_TOL = a.flex_ceiling, a.flex_reserve, a.flex_phi_tol
    GL_RAISE_ABOVE, GL_LOWER_BELOW = a.gl_raise_above, a.gl_lower_below
    GL_HYSTERESIS, GL_STEP = a.gl_hysteresis, a.gl_step

    w0 = int(dt.datetime.strptime(a.start, '%Y-%m-%d').replace(tzinfo=dt.UTC).timestamp())
    w1 = w0 + a.days * 86400
    db = sqlite3.connect(a.db)
    pred = build_runtime_pred(db, w0 - a.warmup * 86400)
    jobs = load(db, w0 - a.warmup * 86400, w1, pred)
    _G.update(cap=load_caps(db), jobs=jobs, win=(w0, w1), nresv=a.reservations,
              budget={'fat': a.budget_fat * 3600, 'slim': a.budget_slim * 3600})
    n_meas = sum(1 for j in jobs if j.elig >= w0)
    print(f"reservations={a.reservations}")
    print(f"window {a.start} +{a.days}d   {len(jobs):,} jobs "
          f"({len(jobs)-n_meas:,} warm-up, {n_meas:,} measured)   "
          f"{len(pred):,} runtime cohorts")

    rec = [j.actual_wait for j in jobs if j.elig >= w0]
    recf = [j.actual_wait for j in jobs if j.elig >= w0 and j.mem / j.cpus > 6000]
    print(f"recorded history: total wait {sum(rec)/3600:,.0f} job-h "
          f"(p99 {pctl(rec,99)/3600:.2f}h), fat {sum(recf)/3600:,.0f} job-h\n")

    specs = [(p, 'timelimit', f) for p in ('static', 'packer')
             for f in ('off', 'perjob', 'global')]
    # 'fork' so workers inherit the loaded trace; 3.14 no longer defaults to it.
    # Safe here: nothing in this process has started a thread.
    with mp.get_context('fork').Pool(a.jobs) as pool:
        res = pool.map(_task, specs)

    print(f"  {'policy':<9} {'mode':<6} {'cpu alloc-h':>12} {'%cap':>6} "
          f"{'stranded':>10} {'wait job-h':>11} {'p99 h':>7} {'fat job-h':>10} "
          f"{'blk cap':>9} {'blk res':>9} {'promo':>7} {'capx':>5} {'raised':>7}")
    print('  ' + '-' * 106)
    base = {}
    for pol, rm, m, n, wt, ft, p99, fp99 in res:
        if pol == 'static' and rm == 'off':
            base = dict(cpu=m['cpu_ch'], strand=m['strand_ch'], wait=wt, fat=ft)
        print(f"  {pol:<9} {rm:<6} {m['cpu_ch']:>12,.0f} "
              f"{m['cpu_ch']/m['cap_ch']*100:>5.1f}% "
              f"{m['strand_ch']:>10,.0f} {wt:>11,.0f} {p99:>7.2f} {ft:>10,.0f} "
              f"{m['blk_lim']:>9,.0f} {m['blk_res']:>9,.0f} {m['flex_jobs']:>7,} "
              f"{m['gl_peak']:>5.2f} {m['gl_raised_h']:>6.0f}h")
    print(f"\n  vs the current static rule with today's fixed QOS caps:")
    print(f"  {'policy':<9} {'mode':<6} {'cpu alloc':>10} {'stranded':>10} "
          f"{'wait':>9} {'fat wait':>10}")
    print('  ' + '-' * 58)
    for pol, rm, m, n, wt, ft, p99, fp99 in res:
        if pol == 'static' and rm == 'off': continue
        print(f"  {pol:<9} {rm:<6} {(m['cpu_ch']/base['cpu']-1)*100:>+9.1f}% "
              f"{(m['strand_ch']/base['strand']-1)*100:>+9.1f}% "
              f"{(wt/base['wait']-1)*100:>+8.1f}% "
              f"{(ft/max(base['fat'],1e-9)-1)*100:>+9.1f}%")


if __name__ == '__main__':
    main()
