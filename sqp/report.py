"""sqp-report - read a decision log as a person, not a parser.

    python3 -m sqp.report /var/log/sqp/decisions.jsonl            # timeline + summary
    python3 -m sqp.report decisions.jsonl --summary               # summary only
    python3 -m sqp.report decisions.jsonl --only different,excluded

The timeline has one entry per thing sqpd did or would have done: each change
with its command, whether it ran, and why; and each job it saw submitted, with
where Slurm put it, where the packer would have, and why.
"""
from __future__ import annotations
import argparse, collections, json, sys


def entries(path):
    with open(path) as f:
        for n, line in enumerate(f, 1):
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                print(f"{path}:{n}: unparsable line skipped", file=sys.stderr)


def show(r) -> str | None:
    ev, t = r.get("event"), r.get("time", r.get("ts"))
    if ev == "action":
        state = "RAN" if r.get("executed") else f"WOULD ({r.get('blocked', 'failed')})"
        out = [f"{t}  {state}  {r['action']}", f"    cmd: {r.get('cmd')}",
               f"    why: {r.get('why')}"]
        if r.get("error"):
            out.append(f"    error: {r['error']}")
        for c in r.get("changes") or []:
            out.append(f"      {c['shape']:>22}  {c['before'] or '-'} -> {c['after']}")
        return "\n".join(out)
    if ev == "placement":
        return (f"{t}  PLACE  job {r['jobid']} ({r['user']}, {r['name']}, {r['state']}) "
                f"[{r['verdict']}]\n    slurm: {r['actual']}   sqp: {r['would']}\n"
                f"    why: {r['why']}\n    cost: {r.get('cost')}")
    if ev in ("preflight", "error", "start", "stop"):
        extra = {k: v for k, v in r.items()
                 if k not in ("ts", "time", "event", "cadence")}
        return f"{t}  {ev.upper()}  {json.dumps(extra)}"
    return None


def main(argv=None):
    ap = argparse.ArgumentParser(prog="sqp-report")
    ap.add_argument("log")
    ap.add_argument("--summary", action="store_true", help="summary only")
    ap.add_argument("--only", help="placement verdicts to list, e.g. different,excluded")
    a = ap.parse_args(argv)
    only = set(a.only.split(",")) if a.only else None

    actions = collections.Counter()
    verdicts = collections.Counter()
    moves = collections.Counter()
    first = last = None
    for r in entries(a.log):
        first = first or r.get("time")
        last = r.get("time", last)
        if r.get("event") == "action":
            actions[(r["action"], "ran" if r.get("executed") else "would")] += 1
        elif r.get("event") == "placement":
            verdicts[r["verdict"]] += 1
            if r["verdict"] in ("different", "excluded"):
                moves[f"{r['actual']} -> {r['would']}"] += 1
            if only is not None and r["verdict"] not in only:
                continue
        elif only is not None:
            continue
        if not a.summary and (txt := show(r)):
            print(txt)

    print(f"\n== summary {first} .. {last}")
    for (act, how), n in sorted(actions.items()):
        print(f"  {act:<22} {how:<6} {n}")
    total = sum(verdicts.values())
    if total:
        print(f"  jobs seen: {total}")
        for v, n in verdicts.most_common():
            print(f"    {v:<10} {n:>7}  {100 * n / total:5.1f}%")
        if moves:
            print("  most common disagreements (slurm -> sqp):")
            for m, n in moves.most_common(10):
                print(f"    {n:>7}  {m}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
