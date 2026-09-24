"""sqpd - the control loop.

Two planes. This process decides slowly and out of band; job_submit.lua applies
the decision instantly and in band, with one table lookup. If this process dies,
the table goes stale and the plugin falls straight through to the site's static
rule -- degrading to exactly today's behaviour, not to an outage.

Threads exist for I/O overlap, not parallel compute. Scoring the whole decision
surface is ~300 buckets and takes single-digit milliseconds; what must never
block is the tick, and a slow `scontrol` call otherwise would.
"""
from __future__ import annotations
import argparse, json, os, signal, sys, threading, time, traceback

from . import config, limits, policy, slurm


class Shared:
    """Latest snapshots, guarded by one lock. Readers never block on Slurm."""
    def __init__(self):
        self.lock = threading.Lock()
        self.nodes: dict = {}
        self.nodes_at: float = 0.0
        self.parts: dict = {}
        self.pending: list = []
        self.pending_at: float = 0.0
        self.stop = threading.Event()
        self.errors: dict = {}

    def note_error(self, where, exc) -> bool:
        """Record an error; True if it differs from the last one seen here."""
        with self.lock:
            new = self.errors.get(where, " ").split(" ", 1)[1] != str(exc)
            self.errors[where] = f"{time.time():.0f} {exc}"
            return new


class Daemon:
    def __init__(self, cfg):
        self.cfg = cfg
        self.sh = Shared()
        self.mode = cfg["general"]["mode"]
        self.state_dir = cfg["general"]["state_dir"]
        self.table_path = os.path.join(self.state_dir, "policy.lua")
        # Where a dry run puts the table it would have written: inspectable, but
        # not the path the plugin reads.
        self.dryrun_table_path = os.path.join(self.state_dir, "policy.dryrun.lua")
        self.status_path = os.path.join(self.state_dir, "status.json")
        self.limiter = limits.GlobalLimitController(cfg)
        self.promoter = limits.PerJobPromoter(cfg)
        self.version = 0
        self.last_rendered = None
        self.last_sig = None      # content signature, excluding the timestamp
        self.last_write = 0.0
        self.demand = [tuple(x) for x in cfg["policy"]["demand"]]
        self.log_fh = None
        self.last_shape = None    # (i, j) -> partitions, for logging what changed
        self.snap = None          # (table, cap, total, free, speed, version) for shadowing
        self.seen = None          # job ids already shadowed; None until the first poll
        # Belt and braces: the mode checks below decide what is *attempted*, and
        # slurm.apply() refuses to run anything unless this is on.
        slurm.set_actuation(self.mode == "enforce")

    # ---------------------------------------------------------------- helpers
    def disabled(self) -> bool:
        return os.path.exists(self.cfg["general"]["disable_file"])

    def log(self, event: str, **kw):
        now = time.time()
        rec = dict(ts=round(now, 3),
                   time=time.strftime("%Y-%m-%dT%H:%M:%S%z", time.localtime(now)),
                   event=event, mode=self.mode, **kw)
        line = json.dumps(rec, separators=(",", ":"))
        if self.log_fh:
            try:
                self.log_fh.write(line + "\n"); self.log_fh.flush()
            except OSError:
                pass
        else:
            print(line, flush=True)

    def blocked(self, modes) -> str | None:
        """Why an action may not be carried out now, or None if it may."""
        if self.mode not in modes:
            return f"mode={self.mode}"
        if self.disabled():
            return "disable_file present"
        return None

    def intend(self, action, cmd, why, blocked, run, **kw):
        """Log an intended change, and carry it out only if nothing blocks it.

        Every change sqpd would make goes through here, so the decision log is
        also a complete list of what a dry run would have done.
        """
        executed, extra = False, {}
        if blocked is None:
            try:
                executed = bool(run())
                if not executed:
                    blocked = "actuation off"
            except Exception as e:
                extra["error"] = repr(e)
        if blocked:
            extra["blocked"] = blocked
        self.log("action", action=action, executed=executed, **extra,
                 why=why, cmd=cmd, **kw)

    def batch_partitions(self, parts: dict, nodes: dict | None = None) -> list[str]:
        return self.partition_filter(parts, nodes)[0]

    def usable_node(self, d) -> bool:
        return not (self.cfg["topology"]["exclude_gpu_nodes"] and d.get("gpu"))

    def partition_filter(self, parts: dict, nodes: dict | None = None):
        """(partitions the packer may assign, {excluded partition: reason})."""
        t = self.cfg["topology"]
        skip = set(t["exclude_partitions"])
        keep, dropped = [], {}
        for p in (t["batch_partitions"] or parts):
            members = [d for d in (nodes or {}).values() if p in d["partitions"]]
            if p not in parts:
                dropped[p] = "listed in batch_partitions but not on this cluster"
            elif p in skip:
                dropped[p] = "exclude_partitions"
            elif t["exclude_interactive"] and p.lower() == "interactive":
                dropped[p] = "named interactive (exclude_interactive)"
            elif members and not any(self.usable_node(d) for d in members):
                dropped[p] = "every node has a GPU (exclude_gpu_nodes)"
            else:
                keep.append(p)
        return keep, dropped

    def shapes(self, nodes, parts):
        """(total shape per partition, free shape per partition)."""
        by_part_total, by_part_free = {}, {}
        for p in self.batch_partitions(parts, nodes):
            tot, free = [], []
            for n, d in nodes.items():
                if p not in d["partitions"] or not d["up"] or not self.usable_node(d):
                    continue
                tot.append((d["cpus"], d["mem"]))
                free.append((max(0, d["cpus"] - d["alloc_cpus"]),
                             max(0, d["mem"] - d["alloc_mem"])))
            if tot:
                by_part_total[p] = tot
                by_part_free[p] = free
        return by_part_total, by_part_free

    def speeds(self, parts) -> dict:
        cfgs = self.cfg["topology"]["speed"]
        dflt = self.cfg["topology"]["default_speed"]
        return {p: cfgs.get(p, dflt) for p in parts}

    # ---------------------------------------------------------------- threads
    def poll_nodes(self):
        iv = self.cfg["cadence"]["node_poll_interval"]
        while not self.sh.stop.wait(0):
            t0 = time.time()
            try:
                n = slurm.nodes()
                p = slurm.partitions()
                with self.sh.lock:
                    self.sh.nodes, self.sh.parts, self.sh.nodes_at = n, p, time.time()
            except Exception as e:                      # never let a poll kill the loop
                if self.sh.note_error("nodes", e):   # log each distinct failure once
                    self.log("error", where="nodes", detail=repr(e))
            if self.sh.stop.wait(max(0.0, iv - (time.time() - t0))):
                return

    def poll_queue(self):
        iv = self.cfg["cadence"]["queue_poll_interval"]
        while not self.sh.stop.wait(0):
            t0 = time.time()
            try:
                q = slurm.queue()
                with self.sh.lock:
                    self.sh.pending = [j for j in q if j["state"] == "PD"]
                    self.sh.pending_at = time.time()
                self.shadow(q)
            except Exception as e:
                if self.sh.note_error("queue", e):   # log each distinct failure once
                    self.log("error", where="queue", detail=repr(e))
            if self.sh.stop.wait(max(0.0, iv - (time.time() - t0))):
                return

    def score(self):
        """Rebuild the decision surface and write it only if it changed."""
        iv = self.cfg["cadence"]["score_interval"]
        while not self.sh.stop.wait(0):
            t0 = time.time()
            try:
                with self.sh.lock:
                    nodes, parts, age = dict(self.sh.nodes), dict(self.sh.parts), \
                        time.time() - self.sh.nodes_at
                if nodes and age < self.cfg["cadence"]["policy_max_age"]:
                    self.score_once(nodes, parts, t0)
            except Exception as e:
                self.sh.note_error("score", e)
                self.log("error", where="score", detail=repr(e))
            if self.sh.stop.wait(max(0.0, iv - (time.time() - t0))):
                return

    def score_once(self, nodes, parts, now):
        total, free = self.shapes(nodes, parts)
        if not total:
            return
        speed = self.speeds(total)
        table = policy.build_table(self.cfg, total, free, speed)
        # Compare the DECISIONS, not the rendered text: the text embeds a
        # timestamp, so comparing it would rewrite the file every tick and force
        # the plugin to reparse on every submission -- the exact churn the
        # write-on-change rule exists to prevent.
        sig = hash(tuple(sorted((k, v_) for k, vs in table.items()
                                for v_ in (",".join(vs),))))
        self.snap = (table, policy.caps(total), total, free, speed, self.version)
        refresh = (now - self.last_write) > self.cfg["cadence"]["policy_max_age"] / 3
        if sig == self.last_sig and not refresh:
            return
        changed = sig != self.last_sig
        self.last_sig = sig
        self.last_write = now
        self.version += 1
        rendered = policy.render_lua(table, self.cfg, now, self.version, total)
        self.last_rendered = rendered
        free_cpu = sum(fc for v in free.values() for fc, _ in v)
        phi = sum(policy.phi_node(fc, fm, self.demand)
                  for v in free.values() for fc, fm in v)
        strand = sum(policy.stranded(fc, fm, self.cfg["policy"]["demand_median"])
                     for v in free.values() for fc, fm in v)
        self.log("policy", version=self.version, free_cpu=round(free_cpu),
                 phi=round(phi, 1), stranded=round(strand, 1),
                 distinct=len({tuple(v) for v in table.values()}),
                 reason="changed" if changed else "refresh")
        why = self.table_why(table, total, free, speed, changed)
        self.intend("write_policy_table", f"write {self.table_path} (v{self.version})",
                    why, self.blocked(("advise", "enforce")),
                    lambda: self._write_atomic(self.table_path, rendered),
                    version=self.version, changes=self.table_changes)
        if self.mode == "observe":
            self._write_atomic(self.dryrun_table_path, rendered)

    def table_why(self, table, total, free, speed, changed) -> str:
        """Explain a table write; leaves the per-bucket diff in self.table_changes."""
        shape = {(i, j): parts for (i, j, k), parts in table.items() if k == 0}
        prev, self.last_shape = self.last_shape, shape
        self.table_changes = []
        if prev is None:
            return f"first table since start ({len(shape)} shape buckets)"
        if not changed:
            return ("decisions unchanged; rewrite so the plugin does not treat the "
                    f"table as stale (max_age {self.cfg['cadence']['policy_max_age']:.0f}s)")
        for key, parts in sorted(shape.items()):
            if prev.get(key) == parts:
                continue
            cpus, mem = policy.bucket_shape(self.cfg, *key)
            cost = policy.score_partitions(cpus, mem, total, free, speed, self.demand)
            self.table_changes.append(dict(
                bucket=f"{key[0]},{key[1]},*", shape=f"{cpus}c x {mem // cpus} MB/CPU",
                before=",".join(prev.get(key, [])), after=",".join(parts),
                cost={p: round(c, 1) for c, p in cost}))
        return (f"{len(self.table_changes)} of {len(shape)} shape buckets changed as "
                "free capacity moved (cost = placeable capacity destroyed - speed x cpus; "
                "partitions within tolerance of the cheapest are admitted)")

    def shadow(self, jobs):
        """Log where the plugin would have put each newly seen job, next to where
        Slurm actually put it. The plugin acts at submission, which a dry run
        cannot intercept, so this is how its effect is made visible."""
        ids = {j["jobid"] for j in jobs}
        if self.seen is None:            # placed before we were watching
            self.seen = ids
            return
        if self.snap is None:            # no table yet; try these again next poll
            return
        table, cap, total, free, speed, version = self.snap
        for j in jobs:
            if j["jobid"] in self.seen:
                continue
            actual = [p for p in j["partition"].split(",") if p]
            # GPU, interactive and other partitions are routed by the plugin
            # before the table is consulted; sqp has no opinion on them.
            if j.get("gpu") or not set(actual) & set(cap):
                continue
            cpus = max(1, j["cpus"])
            mem = max(j.get("req_mem") or j["mem"], 512)
            would, key, refit = policy.plugin_lookup(table, cap, self.cfg, cpus, mem,
                                                     j["timelimit"])
            if j["state"] == "PD":
                verdict = "same" if set(actual) == set(would) else "different"
            else:
                verdict = "allowed" if actual[0] in would else "excluded"
            cost = policy.score_partitions(cpus, mem, total, free, speed, self.demand)
            why = (f"{cpus}c x {mem // cpus} MB/CPU, {j['timelimit']} min -> bucket "
                   f"{key[0]},{key[1]},{key[2]} of table v{version}: "
                   f"{','.join(table.get(key, []))}")
            if refit:
                why += f"; refit to the job's real size -> {','.join(would)}"
            self.log("placement", jobid=j["jobid"], user=j["user"], name=j["name"],
                     state=j["state"], cpus=cpus, mem_mb=mem, minutes=j["timelimit"],
                     actual=",".join(actual), would=",".join(would), verdict=verdict,
                     why=why, cost={p: round(c, 1) for c, p in cost})
        self.seen = ids

    def preflight(self):
        """Record what this run is able to change, and where the live cluster
        differs from what the config assumes."""
        info = dict(actuation=slurm.actuation(),
                    writes_table=self.blocked(("advise", "enforce")) is None,
                    table_path=self.table_path, limits_mode=self.cfg["limits"]["mode"],
                    disable_file_present=self.disabled(), warnings=[])
        try:
            nodes = slurm.nodes()
            keep, dropped = self.partition_filter(slurm.partitions(), nodes)
            info["batch_partitions"] = keep
            info["excluded_partitions"] = dropped
            info["excluded_gpu_nodes"] = sorted(n for n, d in nodes.items()
                                                if not self.usable_node(d))
            if not keep:
                info["warnings"].append("no partitions left to assign")
        except slurm.SlurmError as e:
            info["warnings"].append(f"partitions: {e}")
        if self.cfg["limits"]["mode"] == "global":
            lc, qos = self.cfg["limits"], self.cfg["limits"]["qos_name"]
            try:
                u, a = slurm.qos_cpu_limits(qos)
                info["qos_live"] = dict(qos=qos, per_user=u, per_account=a)
                if (u, a) != (lc["base_cpu_per_user"], lc["base_cpu_per_account"]):
                    info["warnings"].append(
                        f"QOS {qos} is MaxTRESPU cpu={u} MaxTRESPA cpu={a}, config base "
                        f"is {lc['base_cpu_per_user']}/{lc['base_cpu_per_account']}; "
                        "in enforce mode the first limit change would replace the live values")
            except slurm.SlurmError as e:
                info["warnings"].append(f"qos: {e}")
        self.log("preflight", **info)

    def act(self):
        """Elastic limits, and (in perjob mode) individual promotions."""
        iv = self.cfg["cadence"]["act_interval"]
        while not self.sh.stop.wait(0):
            t0 = time.time()
            try:
                self.act_once()
            except Exception as e:
                self.sh.note_error("act", e)
                self.log("error", where="act", detail=repr(e))
            if self.sh.stop.wait(max(0.0, iv - (time.time() - t0))):
                return

    def act_once(self):
        with self.sh.lock:
            nodes, parts = dict(self.sh.nodes), dict(self.sh.parts)
            pend = list(self.sh.pending)
        if not nodes:
            return
        total, free = self.shapes(nodes, parts)
        if not total:
            return
        total_cpu = sum(c for v in total.values() for c, _ in v) or 1
        phi = sum(policy.phi_node(fc, fm, self.demand)
                  for v in free.values() for fc, fm in v)
        idle_frac = phi / total_cpu
        capped = [j for j in pend if j["reason"] in slurm.LIMIT_REASONS]

        if self.cfg["limits"]["mode"] == "global":
            change = self.limiter.observe(idle_frac)
            if change:
                per_user, per_acct = change
                self.log("limits", idle_fraction=round(idle_frac, 4),
                         per_user=per_user, per_account=per_acct,
                         multiple=round(self.limiter.multiple, 3),
                         capped_jobs=len(capped))
                argv = slurm.cmd_set_qos_cpu_limits(self.cfg["limits"]["qos_name"],
                                                     per_user, per_acct)
                self.intend("set_qos_cpu_limits", slurm.cmdline(argv), self.limiter.why,
                            self.blocked(("enforce",)),
                            lambda: slurm.apply(argv, timeout=30.0),
                            idle_fraction=round(idle_frac, 4), capped_jobs=len(capped))
        self._write_atomic(self.status_path, json.dumps(dict(
            ts=time.time(), mode=self.mode, version=self.version,
            idle_fraction=round(idle_frac, 4), total_cpu=total_cpu,
            capped_jobs=len(capped), pending=len(pend),
            limits=self.limiter.state(), errors=self.sh.errors), indent=1))

    @staticmethod
    def _write_atomic(path, text):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        tmp = f"{path}.tmp.{os.getpid()}"
        with open(tmp, "w") as f:
            f.write(text)
            f.flush(); os.fsync(f.fileno())
        os.replace(tmp, path)               # atomic; a reader never sees a half file
        return True

    # ---------------------------------------------------------------- run
    def run(self):
        lf = self.cfg["general"]["log_file"]
        if lf:
            try:
                os.makedirs(os.path.dirname(lf), exist_ok=True)
                self.log_fh = open(lf, "a")
            except OSError as e:
                print(f"sqpd: cannot open {lf}: {e}; logging to stdout", file=sys.stderr)
        self.log("start", version=__import__("sqp").__version__,
                 limits_mode=self.cfg["limits"]["mode"],
                 cadence=self.cfg["cadence"])
        self.preflight()
        threads = [threading.Thread(target=t, name=n, daemon=True)
                   for t, n in ((self.poll_nodes, "nodes"), (self.poll_queue, "queue"),
                                (self.score, "score"), (self.act, "act"))]
        # systemd and kill send SIGTERM; stop the same way as on ^C
        signal.signal(signal.SIGTERM, lambda *_: self.sh.stop.set())
        for t in threads:
            t.start()
        try:
            while all(t.is_alive() for t in threads):
                time.sleep(0.5)
        except KeyboardInterrupt:
            pass
        finally:
            self.sh.stop.set()
            for t in threads:
                t.join(timeout=3.0)
            self.log("stop")


def main(argv=None):
    ap = argparse.ArgumentParser(prog="sqpd")
    ap.add_argument("-c", "--config")
    ap.add_argument("--mode", choices=("observe", "advise", "enforce"))
    ap.add_argument("--dry-run", action="store_true",
                    help="force mode=observe: log every action it would take, "
                         "with the command and the reason, and change nothing")
    ap.add_argument("--log-file", help="override [general] log_file")
    ap.add_argument("--state-dir", help="override [general] state_dir")
    ap.add_argument("--once", action="store_true",
                    help="one scoring pass to stdout, then exit (for testing)")
    ap.add_argument("--print-config", action="store_true")
    a = ap.parse_args(argv)

    cfg = config.load(a.config)
    if a.dry_run and a.mode not in (None, "observe"):
        ap.error("--dry-run means --mode observe")
    if a.mode or a.dry_run:
        cfg["general"]["mode"] = a.mode or "observe"
    if a.log_file:
        cfg["general"]["log_file"] = a.log_file
    if a.state_dir:
        cfg["general"]["state_dir"] = a.state_dir
    if a.print_config:
        print(config.dump_defaults()); return 0

    d = Daemon(cfg)
    if a.once:
        # honour the configured log file here too, so --once and a real run
        # produce the same records rather than differing by invocation
        lf = cfg["general"]["log_file"]
        if lf:
            try:
                os.makedirs(os.path.dirname(lf), exist_ok=True)
                d.log_fh = open(lf, "a")
            except OSError:
                pass
        d.preflight()
        d.sh.nodes, d.sh.parts = slurm.nodes(), slurm.partitions()
        d.sh.nodes_at = time.time()
        d.score_once(d.sh.nodes, d.sh.parts, time.time())
        d.act_once()
        try:                    # a single pass has no "new" jobs: shadow them all
            d.seen = set()
            d.shadow(slurm.queue())
        except slurm.SlurmError as e:
            d.log("error", where="queue", detail=repr(e))
        print(d.last_rendered or "(no table produced)")
        return 0
    d.run()
    return 0


if __name__ == "__main__":
    sys.exit(main())
