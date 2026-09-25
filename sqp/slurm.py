"""Talking to Slurm.

Deliberately parses `scontrol ... --oneliner` key=value output rather than
`--json`. The JSON schemas move between releases (this site runs 24.11 in
production and 26.05 on its test box); the key=value form has been stable for
many years and costs nothing to parse. That is the difference between a tool
another site can install and one that only works here.
"""
from __future__ import annotations
import subprocess, shutil, shlex, time, re


class SlurmError(RuntimeError):
    pass


# The only commands that may run while actuation is off. Every process sqp
# starts goes through _run, so this is the last line of defence for a dry run
# under an account that Slurm would otherwise let change anything.
_READ_ONLY = {("scontrol", "show"), ("squeue",), ("sacctmgr", "-nP", "show")}


def _read_only(args) -> bool:
    return any(tuple(args[:len(p)]) == p for p in _READ_ONLY)


def _run(args, timeout=10.0) -> str:
    if not _actuate and not _read_only(args):
        raise SlurmError(f"refused, actuation is off: {shlex.join(args)}")
    try:
        r = subprocess.run(args, capture_output=True, text=True, timeout=timeout)
    except (subprocess.TimeoutExpired, OSError) as e:
        raise SlurmError(f"{args[0]}: {e}") from e
    if r.returncode != 0:
        raise SlurmError(f"{' '.join(args)}: rc={r.returncode} {r.stderr.strip()[:200]}")
    return r.stdout


def _kv(line: str) -> dict:
    """'NodeName=a CPUTot=8 Reason=foo bar' -> dict. Last key may contain spaces."""
    out = {}
    for m in re.finditer(r"(\w+)=([^ ]*(?: (?![\w]+=)[^ ]*)*)", line):
        out[m.group(1)] = m.group(2).strip()
    return out


def available() -> bool:
    return shutil.which("scontrol") is not None


def nodes() -> dict:
    """name -> dict(cpus, mem, alloc_cpus, alloc_mem, partitions, up)."""
    out = {}
    for line in _run(["scontrol", "show", "nodes", "--oneliner"]).splitlines():
        d = _kv(line)
        name = d.get("NodeName")
        if not name:
            continue
        state = d.get("State", "")
        try:
            cpus = int(d.get("CPUTot", 0))
            mem = int(d.get("RealMemory", 0)) - int(d.get("MemSpecLimit", 0) or 0)
            out[name] = dict(
                cpus=cpus, mem=mem,
                alloc_cpus=int(d.get("CPUAlloc", 0)),
                alloc_mem=int(d.get("AllocMem", 0)),
                partitions=[p for p in d.get("Partitions", "").split(",") if p],
                gpu=_has_gpu(d.get("Gres", ""), d.get("CfgTRES", "")),
                up=_up(state),
            )
        except ValueError:
            continue
    if not out:
        raise SlurmError("scontrol show nodes returned nothing parsable")
    return out


# Node states that offer nothing. Matched as whole flags: a substring test would
# count POWERED_DOWN (idle under power saving, resumed on demand) as DOWN.
_UNUSABLE = {"DOWN", "DRAIN", "DRAINED", "DRAINING", "FAIL", "FAILING", "INVAL"}


def _up(state: str) -> bool:
    """'IDLE+CLOUD+POWERED_DOWN' -> True, 'MIXED+DRAIN' -> False."""
    return not (set(state.upper().rstrip("*~#!%$@^-").split("+")) & _UNUSABLE)


def _has_gpu(gres: str, cfg_tres: str) -> bool:
    """'gpu:a10:1(S:0)' in Gres, or 'gres/gpu=1' in CfgTRES."""
    return (any(g.split(":")[0].strip().lower() == "gpu" for g in gres.split(","))
            or "gres/gpu" in cfg_tres)


def partitions() -> dict:
    """name -> dict(tier, nodes, state)."""
    out = {}
    for line in _run(["scontrol", "show", "partitions", "--oneliner"]).splitlines():
        d = _kv(line)
        name = d.get("PartitionName")
        if not name:
            continue
        out[name] = dict(tier=int(d.get("PriorityTier", 1) or 1),
                         state=d.get("State", "UP"))
    return out


PENDING_FMT = ("JobID:|,UserName:|,Account:|,NumCPUs:|,MinMemory:|,"
               "TimeLimit:|,Priority:|,Reason:|,QOS:|,Partition:|,Name:|")
# The rest is appended so the indices above stay put. tres-alloc is the
# *requested* TRES for a job that has not started; its mem is the job's total,
# which MinMemory is not when the job used --mem-per-cpu.
QUEUE_FMT = PENDING_FMT + (",StateCompact:|,tres-alloc:|,ReqNodes:|,NodeList:|,"
                           "SubmitTime:|,NumTasks:|")


def queue(states: str = "PD,R,CF") -> list[dict]:
    """Jobs in the given states, with the Reason that is nowhere in the accounting database."""
    txt = _run(["squeue", "-h", "-t", states, "-O", QUEUE_FMT], timeout=20.0)
    out = []
    for line in txt.splitlines():
        f = [x.strip() for x in line.split("|")]
        if len(f) < 17:
            continue
        try:
            tres = dict(kv.split("=", 1) for kv in f[12].split(",") if "=" in kv)
            nnodes = max(1, int(tres.get("node", 1) or 1))
            out.append(dict(jobid=f[0], user=f[1], account=f[2],
                            cpus=int(f[3] or 1), mem=_mem_mb(f[4]),
                            req_mem=_mem_mb(tres.get("mem", "")) // nnodes,
                            timelimit=_mins(f[5]), priority=float(f[6] or 0),
                            reason=f[7], qos=f[8], partition=f[9], name=f[10],
                            state=f[11], nnodes=nnodes,
                            gpu=any(k.startswith("gres/gpu") for k in tres),
                            req_nodes=f[13], nodelist=f[14], submit=_epoch(f[15]),
                            ntasks=int(f[16] or 1)))
        except ValueError:
            continue
    if txt.strip() and not out:
        raise SlurmError(f"squeue returned {len(txt.splitlines())} lines, none parsable")
    return out


def pending() -> list[dict]:
    return queue("PD")


def _epoch(s: str) -> float | None:
    """Slurm's local '2026-09-24T12:23:58' -> epoch seconds."""
    try:
        return time.mktime(time.strptime(s, "%Y-%m-%dT%H:%M:%S"))
    except ValueError:
        return None


def admin_comment(jobid: str) -> str:
    """A job's AdminComment, which squeue cannot print."""
    txt = _run(["scontrol", "show", "job", jobid, "--oneliner"])
    return _kv(txt).get("AdminComment", "") if txt.strip() else ""


# Pending reasons, exactly as squeue prints them (checked against the 24.11 and
# 26.05 sources). CAP_REASONS are the holds the per-user/per-account CPU caps
# cause -- MaxTRESPU and MaxTRESPA -- which are the ones a limit pulse releases.
CAP_REASONS = {"QOSMaxCpuPerUserLimit", "MaxCpuPerAccount"}
LIMIT_REASONS = CAP_REASONS | {"AssocGrpCpuLimit", "QOSGrpCpuLimit",
                               "AssocMaxJobsLimit", "QOSMaxJobsPerUserLimit"}


def _mem_mb(s: str) -> int:
    if not s:
        return 0
    mult = {"K": 1 / 1024, "M": 1, "G": 1024, "T": 1024 * 1024}
    if s[-1].upper() in mult:
        return int(float(s[:-1]) * mult[s[-1].upper()])
    return int(float(s))


def _mins(s: str) -> int:
    if not s or s in ("UNLIMITED", "INVALID"):
        return 14 * 24 * 60
    days, _, rest = s.partition("-")
    if rest:
        h, m, *sec = rest.split(":")
        return int(days) * 1440 + int(h) * 60 + int(m)
    p = s.split(":")
    if len(p) == 3:
        return int(p[0]) * 60 + int(p[1])
    if len(p) == 2:
        return int(p[0])
    return int(float(s))


# ---------------------------------------------------------------- read-only
def qos_cpu_limits(qos: str) -> tuple[int | None, int | None]:
    """Current (MaxTRESPU cpu, MaxTRESPA cpu) on a QOS; None where unset."""
    txt = _run(["sacctmgr", "-nP", "show", "qos", f"name={qos}",
                "format=MaxTRESPU,MaxTRESPA"], timeout=30.0)
    line = (txt.strip().splitlines() or [""])[0]
    u, _, a = line.partition("|")

    def cpu(s):
        m = re.search(r"(?:^|,)cpu=(\d+)", s)
        return int(m.group(1)) if m else None
    return cpu(u), cpu(a)


# ---------------------------------------------------------------- actions
# Every command that changes cluster state is built by a cmd_* function and run
# only through apply(). Actuation is OFF until the daemon turns it on in enforce
# mode, so a dry run cannot touch the cluster even if a caller forgets to check
# the mode: apply() then does nothing and reports that it did nothing.
_actuate = False


def set_actuation(on: bool) -> None:
    global _actuate
    _actuate = bool(on)


def actuation() -> bool:
    return _actuate


def apply(argv: list[str], timeout: float = 10.0) -> bool:
    """Run a state-changing command if actuation is on. True if it ran."""
    if not _actuate:
        return False
    _run(argv, timeout)
    return True


def cmdline(argv: list[str]) -> str:
    return shlex.join(argv)


def cmd_set_job_partitions(jobid: str, parts: list[str]) -> list[str]:
    return ["scontrol", "update", f"jobid={jobid}", f"partition={','.join(parts)}"]


def cmd_set_job_qos(jobid: str, qos: str) -> list[str]:
    return ["scontrol", "update", f"jobid={jobid}", f"qos={qos}"]


def cmd_set_array_throttle(jobid: str, n: int) -> list[str]:
    return ["scontrol", "update", f"jobid={jobid}", f"arraytaskthrottle={n}"]


def cmd_release_pin(jobid: str, parts: str, note: str) -> list[list[str]]:
    """Undo a pin: drop the node requirement, then restore the partitions.

    Two commands, because Slurm checks new partitions against the node
    requirement still in place and refuses ones the pinned node is not in."""
    return [["scontrol", "update", f"jobid={jobid}", "reqnodelist="],
            ["scontrol", "update", f"jobid={jobid}", f"partition={parts}",
             f"admincomment={note}"]]


def cmd_set_qos_cpu_limits(qos: str, per_user: int, per_account: int) -> list[str]:
    """The global elastic lever. One number, the same for everyone."""
    return ["sacctmgr", "-i", "modify", "qos", qos, "set",
            f"MaxTRESPU=cpu={int(per_user)}", f"MaxTRESPA=cpu={int(per_account)}"]


def set_job_partitions(jobid: str, parts: list[str]) -> bool:
    return apply(cmd_set_job_partitions(jobid, parts))


def set_job_qos(jobid: str, qos: str) -> bool:
    return apply(cmd_set_job_qos(jobid, qos))


def set_array_throttle(jobid: str, n: int) -> bool:
    return apply(cmd_set_array_throttle(jobid, n))


def set_qos_cpu_limits(qos: str, per_user: int, per_account: int) -> bool:
    return apply(cmd_set_qos_cpu_limits(qos, per_user, per_account), timeout=30.0)
