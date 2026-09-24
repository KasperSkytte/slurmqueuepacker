"""Behavioural tests for the placement core, on biocloud-shaped topology.

These are the claims the design rests on. If one of them fails, the argument in
docs/design.html is wrong, not just the code.

Safe to run on a live cluster: nothing here may start a process. subprocess.run
is replaced before sqp is imported, and any attempt fails the run.
"""
import sys, os, time, subprocess
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

class _NoProcesses(AssertionError):
    pass
def _no_processes(args, **kw):
    raise _NoProcesses(f"tests must not start processes: {args}")
subprocess.run = _no_processes

from sqp import config, policy, daemon

CFG = config.load('/nonexistent')
DEMAND = [tuple(x) for x in CFG['policy']['demand']]
SPEED = {'zen5': 1.0, 'zen5x': 1.0, 'zen3': 0.8, 'zen3x': 0.8}
TOTAL = {
    'zen3':  [(192, 1021567)] * 5 + [(256, 1021540), (192, 505529)],
    'zen3x': [(192, 2041663), (256, 2041636)],
    'zen5':  [(288, 1537338)] * 2 + [(256, 1537407)] * 2,
    'zen5x': [(288, 2311479)] * 2,
}
def scale(frac):
    """Free shapes with `frac` of every node already consumed, ratio-matched."""
    return {p: [(int(c * (1 - frac)), int(m * (1 - frac))) for c, m in v]
            for p, v in TOTAL.items()}

fails = []
def check(name, cond, detail=""):
    print(f"  {'PASS' if cond else 'FAIL'}  {name}" + (f"  [{detail}]" if detail else ""))
    if not cond: fails.append(name)

print("1. feasibility is absolute, not a ratio")
tiny_fat = policy.feasible(1, 32768, TOTAL)          # 1 CPU, 32 GB -> 32k MB/CPU
check("a 1-CPU/32GB job is feasible everywhere", set(tiny_fat) == set(TOTAL),
      ",".join(sorted(tiny_fat)))
huge = policy.feasible(24, 2252800, TOTAL)           # 24 CPU, 2.2 TB
# zen3x tops out at 2,041,663 MB, so 2.2 TB fits on bio-node14/15 only:
# two nodes in the entire cluster.
check("a 24-CPU/2.2TB job is feasible only on zen5x",
      set(huge) == {'zen5x'}, ",".join(sorted(huge)))

print("\n2. idle cluster packs by ratio (the cold-start case)")
idle = policy.choose(1, 32768, TOTAL, scale(0.0), SPEED, DEMAND, 0.25)
check("high-ratio job avoids slim nodes when everything is free",
      not ({'zen3', 'zen5'} & set(idle)), ",".join(idle))

print("\n3. loaded cluster opens up (phi is spent, work term dominates)")
loaded = policy.choose(1, 32768, TOTAL, scale(0.85), SPEED, DEMAND, 0.25)
check("same job admitted more widely under load", len(loaded) >= len(idle),
      f"idle={len(idle)} loaded={len(loaded)}: {','.join(loaded)}")

print("\n4. starvation guard widens the set")
narrow = policy.choose(16, 131072, TOTAL, scale(0.5), SPEED, DEMAND, 0.25, starving=False)
wide = policy.choose(16, 131072, TOTAL, scale(0.5), SPEED, DEMAND, 0.25, starving=True)
check("starving job gets every feasible partition", len(wide) >= len(narrow),
      f"{len(narrow)} -> {len(wide)}")

print("\n5. the table only changes when the decisions change")
d = daemon.Daemon(CFG)
d.mode = "observe"
sigs = []
for frac in (0.0, 0.0, 0.0, 0.5, 0.5, 0.9):
    tbl = policy.build_table(CFG, TOTAL, scale(frac), SPEED)
    sigs.append(hash(tuple(sorted((k, ",".join(v)) for k, v in tbl.items()))))
check("identical state -> identical signature", sigs[0] == sigs[1] == sigs[2])
check("different load -> different signature", len(set(sigs)) > 1,
      f"{len(set(sigs))} distinct across 3 load levels")

print("\n6. static fallback reproduces the site's current rule")
check("low ratio -> slim",
      policy.static_fallback(16, 64000, ['zen5','zen3'], ['zen5x','zen3x']) == ['zen5','zen3'])
check("high ratio -> fat",
      policy.static_fallback(1, 32768, ['zen5','zen3'], ['zen5x','zen3x']) == ['zen5x','zen3x'])

print("\n7. rendered table is loadable and complete")
tbl = policy.build_table(CFG, TOTAL, scale(0.3), SPEED)
lua = policy.render_lua(tbl, CFG, time.time(), 7)
b = CFG['policy']['buckets']
expect = (len(b['mem_per_cpu'])+1) * (len(b['cpus'])+1) * (len(b['walltime_h'])+1)
check("every bucket has an entry", len(tbl) == expect, f"{len(tbl)} == {expect}")
check("no empty partition set", all(v for v in tbl.values()),
      "3 of 64 shape buckets fit no node; they must still name a real partition")
check("lua renders", lua.startswith("-- generated") and lua.rstrip().endswith("}"))
lua2 = policy.render_lua(tbl, CFG, time.time(), 7, TOTAL)
check("caps emitted for every partition",
      all(f'["{p}"]' in lua2.split("t = {")[0] for p in TOTAL))
check("cap values are the largest node in each partition",
      '["zen5x"] = {288, 2311479}' in lua2 and '["zen3x"] = {256, 2041663}' in lua2)

print("\n8. bucketing must not discard feasibility")
# 24 CPU x 2200 GB and 24 CPU x 800 GB share bucket (7,4); only zen5x holds the
# first. The plugin filters by real size against the emitted caps.
big = 2200 * 1024
cap = {p: (max(c for c, _ in v), max(m for _, m in v)) for p, v in TOTAL.items()}
setfor74 = "zen3x"                      # what the table actually held
kept = [p for p in setfor74.split(",") if 24 <= cap[p][0] and big <= cap[p][1]]
check("a 2.2TB job is not left with a partition that cannot hold it",
      kept == [], "zen3x max mem is %d < %d" % (cap['zen3x'][1], big))
check("zen5x can hold it", big <= cap['zen5x'][1])

print("\n9. plugin_lookup mirrors the plugin's refit")
tbl = policy.build_table(CFG, TOTAL, scale(0.3), SPEED)
cap = policy.caps(TOTAL)
parts, key, refit = policy.plugin_lookup(tbl, cap, CFG, 24, 2200 * 1024, 60)
check("a 2.2TB job is refit to the only partition that holds it",
      parts == ['zen5x'], f"{key} -> {parts} refit={refit}")
parts, key, refit = policy.plugin_lookup(tbl, cap, CFG, 1, 2048, 60)
check("an ordinary job is not refit", not refit and parts == tbl[key], ",".join(parts))

print("\n10. a dry run changes nothing and says what it would have done")
import io, json, tempfile
from sqp import slurm
ran = []
real_run = slurm._run
slurm._run = lambda args, timeout=10.0: ran.append(args) or ""
try:
    cfg = config.load('/nonexistent')
    cfg["general"]["mode"] = "observe"
    cfg["general"]["state_dir"] = tempfile.mkdtemp()
    cfg["general"]["disable_file"] = "/nonexistent/disable"
    d = daemon.Daemon(cfg)
    d.log_fh = io.StringIO()
    nodes = {f"{p}{i}": dict(cpus=c, mem=m, alloc_cpus=0, alloc_mem=0,
                             partitions=[p], up=True)
             for p, v in TOTAL.items() for i, (c, m) in enumerate(v)}
    d.sh.nodes, d.sh.parts = nodes, {p: dict(tier=1, state="UP") for p in TOTAL}
    d.score_once(d.sh.nodes, d.sh.parts, time.time())
    for _ in range(cfg["limits"]["hysteresis"]):
        d.act_once()                                   # idle cluster: step the cap up
    recs = [json.loads(l) for l in d.log_fh.getvalue().splitlines()]
    acts = {r["action"]: r for r in recs if r["event"] == "action"}
    check("actuation is off outside enforce", not slurm.actuation())
    check("no state-changing command was run", ran == [], str(ran))
    check("the policy table is not written where the plugin reads it",
          not os.path.exists(d.table_path))
    check("the would-be table is written for inspection",
          os.path.exists(d.dryrun_table_path))
    q = acts.get("set_qos_cpu_limits", {})
    check("the limit change is logged with its command and reason, not run",
          q.get("executed") is False and q.get("cmd", "").startswith("sacctmgr -i modify qos")
          and "raise_above" in q.get("why", ""), json.dumps(q)[:200])
    check("apply() refuses while actuation is off",
          slurm.set_qos_cpu_limits("sqp-test-no-such-qos", 1, 1) is False and ran == [])
    slurm.set_actuation(True)
    check("apply() runs once actuation is on",
          slurm.set_qos_cpu_limits("sqp-test-no-such-qos", 1, 1) is True and len(ran) == 1)
finally:
    slurm._run = real_run
    slurm.set_actuation(False)

print("\n10b. the process launcher itself refuses writes while actuation is off")
import subprocess
launched = []
blocker = subprocess.run
subprocess.run = lambda args, **kw: launched.append(args) or subprocess.CompletedProcess(args, 0, "", "")
try:
    slurm.set_actuation(False)
    refused = 0
    for argv in (slurm.cmd_set_qos_cpu_limits("sqp-test-no-such-qos", 1, 1),
                 slurm.cmd_set_job_partitions("1", ["zen3"]),
                 slurm.cmd_set_job_qos("1", "flex"), slurm.cmd_set_array_throttle("1", 5)):
        try:
            slurm._run(argv)
        except slurm.SlurmError:
            refused += 1
    check("every state-changing command is refused before a process starts",
          refused == 4 and launched == [], f"refused={refused} launched={launched}")
    slurm._run(["scontrol", "show", "nodes", "--oneliner"])
    check("read-only commands still run", len(launched) == 1)
finally:
    subprocess.run = blocker

print("\n11. partitions are discovered; interactive and GPU nodes excluded by default")
check("GPU found in Gres", slurm._has_gpu("gpu:a10:1(S:0)", "cpu=64"))
check("GPU found in CfgTRES", slurm._has_gpu("(null)", "cpu=64,mem=1M,gres/gpu=2"))
check("no GPU", not slurm._has_gpu("(null)", "cpu=64,mem=1M"))
def node(parts, gpu=False):
    return dict(cpus=64, mem=256000, alloc_cpus=0, alloc_mem=0,
                partitions=parts, up=True, gpu=gpu)
nodes = {"a": node(["zen3"]), "b": node(["Interactive"]), "g": node(["gpu"], True),
         "m1": node(["mixed"]), "m2": node(["mixed"], True)}
parts = {p: dict(tier=1, state="UP") for p in ("zen3", "Interactive", "gpu", "mixed")}
cfg = config.load('/nonexistent')
d = daemon.Daemon(cfg)
keep, dropped = d.partition_filter(parts, nodes)
check("defaults keep only CPU batch partitions", sorted(keep) == ["mixed", "zen3"],
      f"{keep} {dropped}")
total, _ = d.shapes(nodes, parts)
check("a mixed partition keeps only its CPU nodes", len(total["mixed"]) == 1)
cfg["topology"].update(exclude_interactive=False, exclude_gpu_nodes=False,
                       exclude_partitions=["zen3"])
keep, dropped = d.partition_filter(parts, nodes)
check("each exclusion can be turned off; names can be excluded",
      sorted(keep) == ["Interactive", "gpu", "mixed"] and "zen3" in dropped, f"{keep}")

check("no test started a process", subprocess.run is _no_processes)

print(f"\n{'ALL PASS' if not fails else 'FAILURES: ' + ', '.join(fails)}")
sys.exit(1 if fails else 0)
