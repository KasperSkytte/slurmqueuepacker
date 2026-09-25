"""The placement decision. Authoritative implementation.

tools/simulate.py imports from here, so the policy that is evaluated offline and
the policy that runs in production cannot drift apart.
"""
from __future__ import annotations
import math


def phi_node(fc: float, fm: float, demand) -> float:
    """Placeable capacity of one free shape: free CPUs usable under the demand mix.

    Marginal use only. As an absolute quantity it charges a fully idle node with
    "unusable" CPUs, because the top demand deciles fit on no node at all.
    """
    return sum(w * min(fc, fm / q) for q, w in demand)


def stranded(fc: float, fm: float, q50: float) -> float:
    """Idle CPUs a median job could not use, for want of memory on their node."""
    return max(0.0, fc - fm / q50)


def phi_cluster(free, demand) -> float:
    return sum(phi_node(fc, fm, demand) for fc, fm in free)


def feasible(cpus: int, mem_mb: int, nodes_by_part) -> list[str]:
    """Partitions holding at least one node physically big enough for the job.

    Feasibility is absolute (does it fit in a node?). Fit quality is a ratio.
    Conflating the two is the bug this project exists to fix.
    """
    return [p for p, nodes in nodes_by_part.items()
            if any(c >= cpus and m >= mem_mb for c, m in nodes)]


def score_partitions(cpus, mem_mb, nodes_by_part, free_by_part, speed, demand):
    """cost(p) = phi destroyed - speed(p) x cpus, best (lowest) first.

    Idle cluster: phi is large, so preserving it dominates and placement comes out
    ratio-matched. Busy cluster: phi is near zero, the work term dominates, and
    anything that fits is admitted. No mode switch, no pressure knob.
    """
    out = []
    for p in feasible(cpus, mem_mb, nodes_by_part):
        best = None
        for fc, fm in free_by_part.get(p, ()):
            if fc < cpus or fm < mem_mb:
                continue
            loss = phi_node(fc, fm, demand) - phi_node(fc - cpus, fm - mem_mb, demand)
            c = loss - speed.get(p, 1.0) * cpus
            if best is None or c < best:
                best = c
        if best is not None:
            out.append((best, p))
    out.sort()
    return out


def largest_partitions(nodes_by_part) -> list[str]:
    """Partitions holding the biggest node, by memory. Used only as a last
    resort for a job no node can hold: emitting an empty partition list would
    set partition="" on the job, whereas this lets Slurm raise its own
    'requested node configuration is not available' against a real partition."""
    if not nodes_by_part:
        return []
    best = max(max(m for _, m in v) for v in nodes_by_part.values() if v)
    return [p for p, v in nodes_by_part.items() if v and max(m for _, m in v) == best]


def choose(cpus, mem_mb, nodes_by_part, free_by_part, speed, demand,
           tolerance=0.25, starving=False):
    """The feasible *set* to hand Slurm. Slurm's PriorityTier still orders it.

    Never returns an empty list: 3 of biocloud's 64 shape buckets describe jobs
    no node can hold (e.g. 192 CPUs at 36 GB/CPU = 6.75 TB), and those must still
    resolve to a real partition so the user gets Slurm's normal error.
    """
    scored = score_partitions(cpus, mem_mb, nodes_by_part, free_by_part, speed, demand)
    if not scored:
        return sorted(feasible(cpus, mem_mb, nodes_by_part)
                      or largest_partitions(nodes_by_part))
    # Sorted by name, not by score. Slurm evaluates a multi-partition job in
    # PriorityTier order regardless of list order, so the order here is free --
    # and making it deterministic keeps the rendered table stable, so an
    # unchanged decision cannot masquerade as a change and trigger a rewrite.
    if starving:
        return sorted(p for _, p in scored)
    lo = scored[0][0]
    return sorted(p for c, p in scored if c <= lo + tolerance * cpus)


def pick_node(cpus, mem_mb, allowed, node_free, tiers, demand, min_gain=1.0,
              min_ratio_gain=0.1):
    """Which node a job that can start now should start on. Authoritative;
    job_submit.lua's pick_node mirrors it.

    Slurm tries a job's partitions in PriorityTier order and starts it in the
    first one with room, so the node is chosen inside the highest-ranked allowed
    partition that has room -- never against the tiers. Within it:

    1. the nodes that destroy the least placeable capacity (phi), within
       min_gain of the best, are the contenders;
    2. of those, the one whose free memory per CPU is closest to the job's
       (|log ratio|) wins, then the lower phi loss, then the name.

    Phi already favours matching shapes, but it is blind beyond the demand mix
    (every ratio above its top quantile scores alike) and near-indifferent between
    leftovers of similar shape; the ratio decides those. No pin when Slurm has no
    choice (one candidate), or when the winner is neither min_gain better by phi
    nor min_ratio_gain better by ratio than the worst candidate.

    node_free: name -> (free_cpus, free_mem_mb, [partitions]).
    Returns (node or None, partitions for the pinned job, info dict with "why").
    """
    ranks = sorted({tiers.get(p, 1) for p in allowed}, reverse=True)
    for r in ranks:
        group = {p for p in allowed if tiers.get(p, 1) == r}
        cands = sorted((phi_node(fc, fm, demand) - phi_node(fc - cpus, fm - mem_mb, demand), n)
                       for n, (fc, fm, parts) in node_free.items()
                       if fc >= cpus and fm >= mem_mb and group & set(parts))
        if cands:
            break
    else:
        return None, [], dict(why="no node has room for it now", candidates=0)
    tier = ",".join(sorted(group))
    want = mem_mb / cpus

    def mismatch(n):
        fc, fm, _ = node_free[n]
        return abs(math.log((fm / fc) / want))
    near = [(mismatch(n), loss, n) for loss, n in cands if loss - cands[0][0] < min_gain]
    mis, loss, best = min(near)
    phi_gain = cands[-1][0] - loss
    ratio_gain = max(mismatch(n) for _, n in cands) - mis
    info = dict(tier=tier, candidates=len(cands), phi_gain=round(phi_gain, 2),
                ratio_gain=round(ratio_gain, 3))
    have = node_free[best][1] / node_free[best][0] / 1024
    if len(cands) == 1:
        info["why"] = f"only {best} in {tier} has room, so Slurm will use it anyway"
        return None, [], info
    if phi_gain >= min_gain:
        info["why"] = (f"{len(cands)} {tier} nodes could start it now; {best} leaves the most "
                       f"room for other jobs ({phi_gain:.1f} CPUs' worth more than the worst "
                       f"choice), with {have:.1f} GB per CPU free for a job asking "
                       f"{want / 1024:.1f}")
    elif ratio_gain >= min_ratio_gain:
        info["why"] = (f"{len(cands)} {tier} nodes could start it now and would leave about "
                       f"as much room; {best}'s free memory per CPU ({have:.1f} GB) is closest "
                       f"to the job's ({want / 1024:.1f} GB)")
    else:
        info["why"] = f"the {len(cands)} {tier} nodes with room are about equally good"
        return None, [], info
    parts = sorted(p for p in node_free[best][2] if p in allowed)
    return best, parts, info


# Pending reasons under which a job cannot start at once whatever the node.
# A job waiting on a dependency is treated like any other: pinned if it fits now,
# released after [pin] release_after if it has not started by then.
NOT_STARTABLE = ("DependencyNeverSatisfied", "BeginTime", "JobHeldUser",
                 "JobHeldAdmin", "PartitionDown", "PartitionInactive")


def pin_eligible(job) -> str | None:
    """None if a queued job is the kind the plugin would pin; else why not.
    Mirrors the plugin's pin_eligible as far as squeue can tell."""
    if "_" in job["jobid"]:
        return "array job"
    if job.get("nnodes", 1) > 1:
        return "multi-node job"
    # The plugin also pins multi-task jobs limited to one node (-N 1), but
    # squeue cannot show that limit for a pending job, so a dry run skips them.
    if job.get("ntasks", 1) > 1:
        return "several tasks, which Slurm may split across nodes"
    if job.get("req_nodes"):
        return "the user chose nodes"
    if job.get("reason") in NOT_STARTABLE:
        return f"cannot start yet ({job['reason']})"
    return None


def static_fallback(cpus, mem_mb, slim, fat, threshold=6000):
    """What the plugin does when the table is missing, stale or unparsable."""
    return list(slim) if mem_mb / max(cpus, 1) < threshold else list(fat)


# ---------------------------------------------------------------- bucket table
def bucket_index(value, edges) -> int:
    i = 0
    while i < len(edges) and value >= edges[i]:
        i += 1
    return i


def build_table(cfg, nodes_by_part, free_by_part, speed):
    """Precompute the whole decision surface as (mpc, cpu, walltime) -> partitions.

    ~300 rows. Computed here, out of band; the plugin only indexes it.
    """
    b = cfg["policy"]["buckets"]
    demand = [tuple(x) for x in cfg["policy"]["demand"]]
    tol = cfg["policy"]["tolerance"]
    mpc_e, cpu_e, wt_e = b["mem_per_cpu"], b["cpus"], b["walltime_h"]
    table = {}
    for i in range(len(mpc_e) + 1):
        for j in range(len(cpu_e) + 1):
            cpus, mem = bucket_shape(cfg, i, j)
            parts = choose(cpus, mem, nodes_by_part, free_by_part, speed, demand, tol)
            for k in range(len(wt_e) + 1):
                # walltime does not change feasibility, only the phi horizon,
                # which is folded into free_by_part before this is called
                table[(i, j, k)] = parts
    return table


def bucket_shape(cfg, i, j) -> tuple[int, int]:
    """(cpus, mem_mb) of the representative job the (i, j) shape bucket is scored for."""
    b = cfg["policy"]["buckets"]
    mpc = _reps(b["mem_per_cpu"])[i]
    cpus = _reps(b["cpus"])[j]
    return int(cpus), int(mpc * cpus)


def caps(nodes_by_part) -> dict:
    """Each partition's largest node, (cpus, mem_mb): what render_lua emits as `cap`."""
    return {p: (max(c for c, _ in v), max(m for _, m in v))
            for p, v in nodes_by_part.items() if v}


def plugin_lookup(table, cap, cfg, cpus, mem_mb, minutes):
    """Python twin of job_submit.lua's packed_choice + keep_feasible.

    Used by dry runs to say what the plugin would have assigned a real job.
    Returns (partitions, (i, j, k), refit). Keep in step with the Lua.
    """
    b = cfg["policy"]["buckets"]
    cpus = max(1, cpus)
    key = (bucket_index(mem_mb / cpus, b["mem_per_cpu"]),
           bucket_index(cpus, b["cpus"]),
           bucket_index(minutes / 60, b["walltime_h"]))
    parts = list(table.get(key) or [])
    kept = [p for p in parts
            if p not in cap or (cpus <= cap[p][0] and mem_mb <= cap[p][1])]
    if not kept:
        kept = sorted(p for p, c in cap.items() if cpus <= c[0] and mem_mb <= c[1])
    if not kept and cap:
        kept = [max(cap, key=lambda p: cap[p][1])]
    return kept, key, kept != parts


def _reps(edges):
    """A representative value for each bucket, including the open-ended top."""
    out = [edges[0] / 2 if edges[0] > 1 else 1]
    for a, b in zip(edges, edges[1:]):
        out.append((a + b) / 2)
    out.append(edges[-1] * 1.5)
    return out


def render_lua(table, cfg, generated_at, version, nodes_by_part=None, pin=None) -> str:
    """Emit the table as Lua source, so the plugin parses it with the interpreter
    it already has: no JSON library, no dependency, no parser to get wrong.

    Also emits each partition's largest node, because the bucket table alone
    cannot answer feasibility. The top shape buckets are open-ended: a job at
    93,000 MB/CPU and one at 30,000 MB/CPU share a bucket, so a set chosen for
    the smaller can be handed to the larger, which no node in it can hold. The
    plugin filters the looked-up set by the job's real size against these caps.
    """
    b = cfg["policy"]["buckets"]
    lines = ["-- generated by sqpd; do not edit", "return {",
             f"  version = {version},", f"  generated_at = {generated_at:.0f},",
             f"  max_age = {cfg['cadence']['policy_max_age']:.0f},",
             "  mpc_edges = {%s}," % ", ".join(str(x) for x in b["mem_per_cpu"]),
             "  cpu_edges = {%s}," % ", ".join(str(x) for x in b["cpus"]),
             "  wt_edges  = {%s}," % ", ".join(str(x) for x in b["walltime_h"]),
             "  cap = {"]
    for p, (c, m) in sorted(caps(nodes_by_part or {}).items()):
        lines.append('    ["%s"] = {%d, %d},' % (p, c, m))
    lines += ["  },", "  t = {"]
    for (i, j, k), parts in sorted(table.items()):
        lines.append('    ["%d,%d,%d"] = "%s",' % (i, j, k, ",".join(parts)))
    lines.append("  },")
    if pin:
        lines += render_pin(cfg, pin)
    lines += ["}", ""]
    return "\n".join(lines)


def render_pin(cfg, pin) -> list[str]:
    """What the plugin needs to choose a node: free space per node, partition
    ranks, the demand mix for phi, and each user's room under the CPU cap."""
    c = cfg["pin"]
    out = ["  pin = {", f"    max_age = {c['max_age']:.0f},",
           f"    min_gain = {c['min_gain']},",
           f"    min_ratio_gain = {c['min_ratio_gain']},",
           "    demand = {%s}," % ", ".join("{%s, %s}" % (q, w)
                                            for q, w in cfg["policy"]["demand"]),
           "    tier = {"]
    for p, t in sorted(pin["tiers"].items()):
        out.append('      ["%s"] = %d,' % (p, t))
    out += ["    },", "    nodes = {"]
    for n, (fc, fm, parts) in sorted(pin["nodes"].items()):
        out.append('      ["%s"] = {%d, %d, "%s"},' % (n, fc, fm, ",".join(parts)))
    out += ["    },"]
    if pin.get("room") is not None:
        out += [f'    cap_qos = "{cfg["limits"]["qos_name"]}",', "    room = {"]
        for uid, left in sorted(pin["room"].items()):
            out.append("      [%d] = %d," % (uid, left))
        out += ["    },"]
    out.append("  },")
    return out
