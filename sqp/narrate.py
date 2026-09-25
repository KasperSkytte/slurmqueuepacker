"""Decision records, in words.

sqpd writes every record twice: as JSON to the decision log, for tools, and
through render() to the text log, for people. sqp.report uses the same render(),
so reading the log later shows exactly what the live text log showed.
"""
from __future__ import annotations
import time


def _clock(ts) -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts))


def _mem(mb) -> str:
    gb = mb / 1024
    return f"{gb:.0f} GB" if gb >= 10 else f"{gb:.1f} GB"


def _walltime(minutes) -> str:
    d, rest = divmod(int(minutes), 1440)
    h, m = divmod(rest, 60)
    parts = [f"{d} d" if d else "", f"{h} h" if h else "", f"{m} min" if m else ""]
    return " ".join(p for p in parts if p) or "0 min"


def _n(k, word) -> str:
    return f"{k} {word}{'s' * (k != 1)}"


def _did(r, done: str, would: str, tried: str) -> str:
    if r.get("executed"):
        return done
    return tried if r.get("error") else would


STATE = {"PD": "pending", "R": "running", "CF": "starting"}


def job(r) -> str:
    head = (f"{_clock(r['ts'])}  job {r['jobid']} by {r['user']} (\"{r['name']}\"): "
            f"{_n(r['cpus'], 'CPU')}, {_mem(r['mem_mb'])}, {_walltime(r['minutes'])}")
    if r["state"] == "PD":
        state = "queued" if r.get("reason") in ("", "None", None) else \
            f"pending ({r['reason']})"
    else:
        state = f"{STATE.get(r['state'], r['state'])} on {r['node']}"
    sub = f"submitted {time.strftime('%H:%M:%S', time.localtime(r['submit']))}, " \
        if r.get("submit") else ""
    lines = [f"{head} -- {sub}{state}"]

    if r.get("acted"):
        # advise/enforce: the plugin has already placed it; say what sqp did
        if r.get("sqp_pin"):
            lines.append(f"    partitions   {r['sqp_from']} (sqp's choice at submission)")
            check = "" if r.get("pin") == r["sqp_pin"] else \
                f" [recomputed now: {r.get('pin') or 'no pin'}, {r['pin_why']}]"
            lines.append(f"    node         sqp pinned it to {r['sqp_pin']} in {r['actual']}"
                         + (f": {r['pin_why']}" if not check else check))
        else:
            lines.append(f"    partitions   {r['actual']} (set at submission)")
            lines.append("    node         left to Slurm" + (
                f": {r['pin_why']}" if not r.get("pin") else
                f" [recomputed now: would pin {r['pin']}, {r['pin_why']}]"))
        return "\n".join(lines)

    if r["state"] == "PD":
        same = set(r["actual"].split(",")) == set(r["would"].split(","))
        diff = "same" if same else "; ".join(r.get("differences") or ["different"])
        lines.append(f"    partitions   Slurm: {r['actual']:<22} sqp: {r['would']}   ({diff})")
    else:
        diff = "; ".join(r.get("differences") or []) or "sqp would allow it there too"
        lines.append(f"    partitions   Slurm ran it in {r['actual']}; sqp would allow "
                     f"{r['would']}   ({diff})")
    if r.get("pin"):
        lines.append(f"    node         sqp would pin it to {r['pin']} "
                     f"(partition {r['pin_parts']}): {r['pin_why']}")
    else:
        lines.append(f"    node         sqp would leave it to Slurm: {r['pin_why']}")
    return "\n".join(lines)


def action(r) -> str | None:
    t = _clock(r["ts"])
    a = r["action"]
    if a == "set_qos_cpu_limits":
        verb = _did(r, "changed", "would change", "tried to change")
        return (f"{t}  QOS {r.get('qos', '?')}: {verb} the CPU cap per user "
                f"{r.get('before_user')} -> {r.get('per_user')} and per account "
                f"{r.get('before_account')} -> {r.get('per_account')}: {r['why']}\n"
                f"    command: {r['cmd']}" + _blocked(r))
    if a == "release_pin":
        verb = _did(r, "released", "would release", "tried to release")
        return (f"{t}  job {r['jobid']}: {verb} its pin to {r['node']}, putting it back "
                f"in partitions {r['parts']}: {r['why']}\n    command: {r['cmd']}" + _blocked(r))
    if a == "write_policy_table" and r.get("changes"):
        verb = _did(r, "updated", "would update", "tried to update")
        n = len(r["changes"])
        return (f"{t}  placement table {verb}: {_n(n, 'job shape')} "
                f"now {'goes' if n == 1 else 'go'} to different partitions")
    return None


def _blocked(r) -> str:
    if r.get("error"):
        return f"\n    FAILED: {r['error']}"
    if not r.get("executed") and r.get("blocked"):
        return f"\n    not run: {r['blocked']}"
    return ""


def preflight(r) -> str:
    t = _clock(r["ts"])
    can = []
    if r.get("writes_table"):
        can.append("write the placement table")
    if r.get("actuation"):
        can.append("pin nodes, release pins and change QOS limits")
    lines = [f"{t}  sqp {r.get('mode')}: "
             + ("will " + " and ".join(can) if can else "changes nothing, only logs")]
    if r.get("batch_partitions") is not None:
        lines.append(f"    partitions   {', '.join(r['batch_partitions']) or '(none)'}")
    for p, why in (r.get("excluded_partitions") or {}).items():
        lines.append(f"    excluded     {p}: {why}")
    if r.get("excluded_gpu_nodes"):
        lines.append(f"    GPU nodes    ignored: {', '.join(r['excluded_gpu_nodes'])}")
    for w in r.get("warnings") or []:
        lines.append(f"    WARNING      {w}")
    return "\n".join(lines)


def render(r) -> str | None:
    """One record as text, or None for records that are only for tools."""
    ev = r.get("event")
    if ev == "placement":
        return job(r)
    if ev == "action":
        return action(r)
    if ev == "preflight":
        return preflight(r)
    if ev == "error":
        return f"{_clock(r['ts'])}  ERROR in {r.get('where')}: {r.get('detail')}"
    if ev == "start":
        return f"{_clock(r['ts'])}  sqp {r.get('version')} started in {r.get('mode')} mode"
    if ev == "stop":
        return f"{_clock(r['ts'])}  sqp stopped"
    return None
