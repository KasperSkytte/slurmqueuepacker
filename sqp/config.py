"""Configuration: every tunable in one TOML file, with documented defaults.

Nothing in the daemon reads a magic number directly; it all comes through here,
so a site can retune without touching code and a paper can report the settings.
"""
from __future__ import annotations
import copy, os, tomllib

DEFAULTS: dict = {
    "general": {
        # observe  - compute and log decisions, change nothing
        # advise   - also write the policy table (the plugin may still ignore it)
        # enforce  - also act on pending jobs and limits
        "mode": "observe",
        "state_dir": "/run/sqp",
        "log_file": "/var/log/sqp/decisions.jsonl",
        "disable_file": "/etc/sqp/disable",   # touch to fall back instantly
    },
    "cadence": {
        "score_interval": 1.0,        # seconds; the policy table refresh
        "node_poll_interval": 1.0,    # scontrol show nodes - small, cheap
        "queue_poll_interval": 8.0,   # squeue - takes a job-table lock, keep slow
        "act_interval": 15.0,         # how often the actor may issue updates
        "max_actions_per_interval": 5,
        "policy_max_age": 300.0,      # plugin ignores a table older than this
    },
    "topology": {
        # Empty = discover from `scontrol show partitions`. Listing some restricts
        # the packer to those. The exclusions below apply either way.
        "batch_partitions": [],
        "exclude_partitions": [],       # never assign these, by name
        "exclude_interactive": True,    # drop any partition named "interactive"
        # Ignore nodes with a GPU (Gres or CfgTRES gres/gpu); a partition left
        # with no nodes is dropped. GPU jobs are routed by the plugin, not here.
        "exclude_gpu_nodes": True,
        # Relative throughput per partition, for the work term. 1.0 = fastest.
        "speed": {},
        "default_speed": 1.0,
    },
    "policy": {
        # Demand mix: (MB per CPU, weight). Fitted from sacct history by
        # `sqp-fit`; these are biocloud's, allocation-hour weighted.
        "demand": [[800, 0.10], [1280, 0.15], [4267, 0.25],
                   [7680, 0.25], [14178, 0.15], [30720, 0.10]],
        "demand_median": 4267,        # for the stranding metric only
        "tolerance": 0.25,            # extra partitions admitted, CPUs of phi per job CPU
        "ratio_threshold": 6000,      # fallback static rule, MB/CPU
        "buckets": {
            "mem_per_cpu": [1024, 2048, 4096, 6144, 8192, 12288, 24576],
            "cpus": [1, 4, 8, 16, 32, 64, 128],
            "walltime_h": [1, 6, 24, 72],
        },
    },
    "starvation": {
        # Wait budget per class, hours. Beyond this a job's feasible set is
        # widened and competing cohorts are narrowed until it starts.
        "budget_hours": {"fat": 1.0, "slim": 4.0},
        "fat_ratio_threshold": 6000,  # MB/CPU above which a job counts as "fat"
    },
    "limits": {
        # off | global | perjob
        #   global - raise MaxTRESPU for EVERYONE, uniformly, while the cluster
        #            is persistently idle; snap back instantly when it is not.
        #            Fair by construction; fair-share still orders who fills it.
        #   perjob - move individual pending jobs to a flex QOS. Surgical, but
        #            biased toward jobs that are easy to place. NOT YET
        #            IMPLEMENTED: currently changes nothing.
        "mode": "global",
        "base_cpu_per_user": 864,
        "base_cpu_per_account": 1760,
        "ceiling": 2.0,               # hard stop, multiple of base
        "raise_above": 0.25,          # idle placeable fraction to step up
        "lower_below": 0.10,          # below this, snap straight back
        "hysteresis": 5,              # consecutive intervals before a step up
        "step": 1.25,
        "qos_name": "normal",         # global mode edits this QOS
        "flex_qos_name": "flex",      # perjob mode promotes into this QOS
        "flex_reserve": 0.05,
        "flex_phi_tolerance": 1.0,
    },
    "cohorts": {
        "key": ["user", "name", "cpus", "mem"],
        "min_members": 50,
        "max_queue_share": 0.5,       # one cohort may hold at most this much queue
    },
    "runtime_model": {
        "quantile": 0.95,
        "min_members": 20,
        "refresh_hours": 6,
        "margin": 1.25,
    },
}


def _merge(base: dict, over: dict) -> dict:
    out = copy.deepcopy(base)
    for k, v in over.items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _merge(out[k], v)
        else:
            out[k] = v
    return out


def load(path: str | None = None) -> dict:
    path = path or os.environ.get("SQP_CONFIG", "/etc/sqp/sqp.toml")
    if not os.path.exists(path):
        return copy.deepcopy(DEFAULTS)
    with open(path, "rb") as f:
        return _merge(DEFAULTS, tomllib.load(f))


def dump_defaults() -> str:
    """Emit the default config as TOML, for `sqp-config --defaults`."""
    def fmt(v):
        if isinstance(v, bool):  return "true" if v else "false"
        if isinstance(v, str):   return f'"{v}"'
        if isinstance(v, list):  return "[" + ", ".join(fmt(x) for x in v) + "]"
        return str(v)
    out = []
    for sect, body in DEFAULTS.items():
        out.append(f"[{sect}]")
        for k, v in body.items():
            if isinstance(v, dict):
                out.append(f"\n[{sect}.{k}]")
                for k2, v2 in v.items():
                    out.append(f"{k2} = {fmt(v2)}")
            else:
                out.append(f"{k} = {fmt(v)}")
        out.append("")
    return "\n".join(out)
