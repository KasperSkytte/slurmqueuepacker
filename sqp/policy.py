"""The placement decision. Authoritative implementation.

tools/simulate.py imports from here, so the policy that is evaluated offline and
the policy that runs in production cannot drift apart.
"""
from __future__ import annotations


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


def render_lua(table, cfg, generated_at, version, nodes_by_part=None) -> str:
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
    lines += ["  },", "}", ""]
    return "\n".join(lines)
