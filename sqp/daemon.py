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
import argparse, collections, json, os, pwd, signal, sys, threading, time, traceback

from . import config, limits, narrate, policy, slurm


class Shared:
    """Latest snapshots, guarded by one lock. Readers never block on Slurm."""
    def __init__(self):
        self.lock = threading.Lock()
        self.nodes: dict = {}
        self.nodes_at: float = 0.0
        self.parts: dict = {}
        self.pending: list = []
        self.pending_at: float = 0.0
        self.jobs: list = []          # pending and running, from the last queue poll
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
        self.limiter = limits.LimitPulse(cfg)
        self.promoter = limits.PerJobPromoter(cfg)
        self.version = 0
        self.last_rendered = None
        self.last_sig = None      # content signature, excluding the timestamp
        self.last_write = 0.0
        self.demand = [tuple(x) for x in cfg["policy"]["demand"]]
        self.log_fh = None
        self.text_fh = None
        self.last_shape = None    # (i, j) -> partitions, for logging what changed
        self.last_pin_sig = None
        self.snap = None          # latest decision surface, for shadowing (a dict)
        self.released: set = set()   # pinned jobs already released or not ours
        self.uids: dict = {}
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
        if self.text_fh and (text := narrate.render(rec)):
            try:
                self.text_fh.write(text + "\n"); self.text_fh.flush()
            except OSError:
                pass

    def open_logs(self):
        for key, attr in (("log_file", "log_fh"), ("text_log", "text_fh")):
            path = self.cfg["general"].get(key)
            if not path:
                continue
            try:
                os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
                setattr(self, attr, open(path, "a"))
            except OSError as e:
                print(f"sqpd: cannot open {path}: {e}", file=sys.stderr)

    def blocked(self, modes) -> str | None:
        """Why an action may not be carried out now, or None if it may."""
        if self.mode not in modes:
            return f"mode={self.mode}"
        if self.disabled():
            return "disable_file present"
        return None

    def intend(self, action, cmd, why, blocked, run, quiet=False, **kw):
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
        if quiet and "error" not in extra:
            return
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

    def pin_active(self) -> bool:
        """Pins need sqpd to release the ones that do not start, which is an
        enforce-mode action. observe shows what enforce would do; advise never pins."""
        return self.cfg["pin"]["enabled"] and self.mode != "advise"

    def node_free(self, nodes, parts) -> dict:
        """name -> (free cpus, free mem, [batch partitions]) for nodes a job could use."""
        batch = set(self.batch_partitions(parts, nodes))
        out = {}
        for n, d in nodes.items():
            ps = sorted(p for p in d["partitions"] if p in batch)
            if ps and d["up"] and self.usable_node(d):
                out[n] = (max(0, d["cpus"] - d["alloc_cpus"]),
                          max(0, d["mem"] - d["alloc_mem"]), ps)
        return out

    def uid(self, user):
        if user not in self.uids:
            try:
                self.uids[user] = pwd.getpwnam(user).pw_uid
            except KeyError:
                self.uids[user] = None
        return self.uids[user]

    def user_room(self, jobs) -> dict | None:
        """uid -> CPUs left under the per-user cap, for users with running jobs."""
        if self.cfg["limits"]["mode"] != "global":
            return None
        qos, cap = self.cfg["limits"]["qos_name"], int(self.limiter.cur_u)
        used = collections.Counter()
        for j in jobs:
            if j["state"] in ("R", "CF") and j["qos"] == qos:
                used[j["user"]] += j["cpus"]
        return {u: cap - c for user, c in used.items() if (u := self.uid(user)) is not None}

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
                    self.sh.jobs = q
                    self.sh.pending = [j for j in q if j["state"] == "PD"]
                    self.sh.pending_at = time.time()
                self.shadow(q)
                if self.cfg["pin"]["enabled"]:
                    self.release_pins(q, time.time())
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
            e = "no usable nodes in any batch partition (all down, drained or excluded?)"
            if self.sh.note_error("score", e):
                self.log("error", where="score", detail=e)
            return
        speed = self.speeds(total)
        table = policy.build_table(self.cfg, total, free, speed)
        # Compare the DECISIONS, not the rendered text: the text embeds a
        # timestamp, so comparing it would rewrite the file every tick and force
        # the plugin to reparse on every submission -- the exact churn the
        # write-on-change rule exists to prevent.
        sig = hash(tuple(sorted((k, v_) for k, vs in table.items()
                                for v_ in (",".join(vs),))))
        with self.sh.lock:
            jobs = list(self.sh.jobs)
        state = dict(nodes=self.node_free(nodes, parts),
                     tiers={p: parts[p]["tier"] for p in total},
                     room=self.user_room(jobs))
        pin = state if self.pin_active() else None
        pin_sig = hash(repr(pin)) if pin else None
        self.snap = dict(table=table, cap=policy.caps(total), total=total, free=free,
                         speed=speed, version=self.version, node_free=state["nodes"],
                         tiers=state["tiers"], room=state["room"])
        # Rewrite when the partition decisions change, when free space changes
        # while pinning (the plugin pins from it), and often enough that the
        # plugin never sees the table as stale: max_age/3, or pin max_age/2.
        age = now - self.last_write
        refresh = age > self.cfg["cadence"]["policy_max_age"] / 3 or \
            (pin is not None and age > self.cfg["pin"]["max_age"] / 2)
        if sig == self.last_sig and pin_sig == self.last_pin_sig and not refresh:
            return
        changed = sig != self.last_sig
        self.last_sig, self.last_pin_sig = sig, pin_sig
        self.last_write = now
        self.version += 1
        rendered = policy.render_lua(table, self.cfg, now, self.version, total, pin)
        self.last_rendered = rendered
        if not changed:                      # free space or refresh only: say nothing
            self.intend("write_policy_table", f"write {self.table_path}", "",
                        self.blocked(("advise", "enforce")),
                        lambda: self._write_atomic(self.table_path, rendered), quiet=True)
            if self.mode == "observe":
                self._write_atomic(self.dryrun_table_path, rendered)
            return
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
        """Log what sqp would do with each newly seen job, next to what Slurm did:
        its partitions, and whether and where it would pin the node. The plugin
        acts at submission, which a dry run cannot intercept, so this is how its
        effect is made visible."""
        ids = {j["jobid"] for j in jobs}
        if self.seen is None:            # placed before we were watching
            self.seen = ids
            return
        if self.snap is None:            # no table yet; try these again next poll
            return
        s = self.snap
        for j in jobs:
            if j["jobid"] in self.seen:
                continue
            actual = [p for p in j["partition"].split(",") if p]
            # GPU, interactive and other partitions are routed by the plugin
            # before the table is consulted; sqp has no opinion on them.
            if j.get("gpu") or not set(actual) & set(s["cap"]):
                continue
            self.log("placement", **self.evaluate(j, actual, s))
        self.seen = ids

    def evaluate(self, j, actual, s) -> dict:
        """What sqp would do with one job, as a placement record."""
        cpus = max(1, j["cpus"])
        mem = max(j.get("req_mem") or j["mem"], 512)
        would, key, refit = policy.plugin_lookup(s["table"], s["cap"], self.cfg, cpus, mem,
                                                 j["timelimit"])
        running = j["state"] in ("R", "CF")
        if running:
            verdict = "allowed" if actual[0] in would else "excluded"
        else:
            verdict = "same" if set(actual) == set(would) else "different"
        cost = policy.score_partitions(cpus, mem, s["total"], s["free"], s["speed"],
                                       self.demand)
        why = (f"{cpus}c x {mem // cpus} MB/CPU, {j['timelimit']} min -> bucket "
               f"{key[0]},{key[1]},{key[2]} of table v{s['version']}: "
               f"{','.join(s['table'].get(key, []))}")
        if refit:
            why += f"; refit to the job's real size -> {','.join(would)}"

        # In advise/enforce the plugin has already acted. A node requirement is
        # sqp's own pin if the plugin marked it so; read the mark back.
        sqp_pin = sqp_from = ""
        if j.get("req_nodes") and self.mode != "observe":
            try:
                note = slurm.admin_comment(j["jobid"])
            except slurm.SlurmError:
                note = ""
            if note.startswith("sqp:pin="):
                sqp_pin, _, sqp_from = note[len("sqp:pin="):].partition(";from=")
        # Recompute against what the job was given: sqp's partitions before the
        # pin, or, once the plugin acts, the partitions it set.
        allowed = sqp_from.split(",") if sqp_from else \
            (actual if self.mode != "observe" else would)

        # The node. A running job's own allocation is added back to its node, so
        # the choice is made against the cluster as it was just before it started.
        node = j.get("nodelist") if running else ""
        nf = dict(s["node_free"])
        if node in nf:
            fc, fm, ps = nf[node]
            nf[node] = (fc + cpus, fm + mem, ps)
        pin, pin_parts = None, []
        room = (s["room"] or {}).get(self.uid(j["user"]))
        if not self.cfg["pin"]["enabled"]:
            pin_why = "pinning is off"
        # A job the plugin pinned passed its checks, including the one-node limit
        # squeue cannot show.
        elif (reason := policy.pin_eligible(dict(j, req_nodes="", ntasks=1)
                                            if sqp_pin else j)):
            pin_why = reason
        elif room is not None and room < cpus and j["qos"] == self.cfg["limits"]["qos_name"]:
            pin_why = f"the user is at the per-user CPU cap ({room} CPUs left)"
        else:
            pin, pin_parts, info = policy.pick_node(cpus, mem, allowed, nf, s["tiers"],
                                                   self.demand, self.cfg["pin"]["min_gain"],
                                                   self.cfg["pin"]["min_ratio_gain"])
            pin_why = info["why"]
            if pin and node:
                pin_why += ("; Slurm chose the same node" if pin == node
                            else f"; Slurm chose {node}")
        return dict(jobid=j["jobid"], user=j["user"], name=j["name"], state=j["state"],
                    reason=j["reason"] if not running else "", submit=j.get("submit"),
                    node=node, cpus=cpus, mem_mb=mem, minutes=j["timelimit"],
                    actual=",".join(actual), would=",".join(would), verdict=verdict,
                    differences=self.differences(actual, would, cpus, mem, cost, s, running),
                    pin=pin, pin_parts=",".join(pin_parts), pin_why=pin_why,
                    acted=self.mode != "observe", sqp_pin=sqp_pin, sqp_from=sqp_from,
                    why=why, cost={p: round(c, 1) for c, p in cost})

    @staticmethod
    def differences(actual, would, cpus, mem, cost, s, running) -> list[str]:
        """Each partition the two sides disagree on, with the reason in words."""
        room = {p for _, p in cost}

        def reason(p):
            if p not in s["total"]:
                return "not a partition sqp assigns"
            if not policy.feasible(cpus, mem, {p: s["total"][p]}):
                return "no node there is big enough"
            if p not in room:
                return "no node there has room now"
            return "a poorer fit for this job's shape"
        if running:
            p = actual[0]
            return [] if p in would else [f"sqp would not have allowed {p}: {reason(p)}"]
        out = [f"adds {p}" + (": it fits about as well now" if p in room else "")
               for p in sorted(set(would) - set(actual))]
        out += [f"drops {p}: {reason(p)}" for p in sorted(set(actual) - set(would))]
        return out

    def release_pins(self, jobs, now):
        """Drop the pin from any job the plugin pinned that did not start. Allowed
        in enforce mode even with the disable file present: a pin left behind
        can hold a job to one node, so undoing pins is always safe to do."""
        after = self.cfg["pin"]["release_after"]
        for j in jobs:
            jid = j["jobid"]
            if (j["state"] != "PD" or not j.get("req_nodes") or jid in self.released
                    or not j.get("submit") or now - j["submit"] < after):
                continue
            self.released.add(jid)
            try:
                note = slurm.admin_comment(jid)
            except slurm.SlurmError as e:
                self.log("error", where="release", detail=repr(e))
                continue
            if not note.startswith("sqp:pin="):
                continue                 # the user's own --nodelist: never touch it
            node, _, parts = note[len("sqp:pin="):].partition(";from=")
            argvs = slurm.cmd_release_pin(jid, parts, f"sqp:released={node}")
            self.intend("release_pin", " && ".join(slurm.cmdline(a) for a in argvs),
                        f"pinned at submission but still pending after "
                        f"{now - j['submit']:.0f} s ({j['reason']})",
                        None if self.mode == "enforce" else f"mode={self.mode}",
                        lambda: all([self.apply_retrying(a) for a in argvs]),
                        jobid=jid, node=node, parts=parts)
        self.released &= {j["jobid"] for j in jobs}

    @staticmethod
    def apply_retrying(argv, tries=3, wait=1.0) -> bool:
        """slurm.apply, for job updates. Slurm answers some updates with EAGAIN
        ("Resource temporarily unavailable") while it is busy with the job, and
        the same update succeeds a moment later. A job that has started in the
        meantime needs no further change."""
        for i in range(tries):
            try:
                return slurm.apply(argv)
            except slurm.SlurmError as e:
                if "no longer pending" in str(e):
                    return True
                if "temporarily unavailable" not in str(e) or i == tries - 1:
                    raise
                time.sleep(wait)
        return False

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
        if self.cfg["limits"]["mode"] == "perjob":
            info["warnings"].append(
                "limits.mode = perjob is not implemented yet: no limits or QOS will be "
                "changed. Use global or off.")
        if self.cfg["limits"]["mode"] == "global":
            lc, qos = self.cfg["limits"], self.cfg["limits"]["qos_name"]
            try:
                u, a = slurm.qos_cpu_limits(qos)
                info["qos_live"] = dict(qos=qos, per_user=u, per_account=a)
                if (u, a) != (lc["base_cpu_per_user"], lc["base_cpu_per_account"]):
                    info["warnings"].append(
                        f"QOS {qos} is MaxTRESPU cpu={u} MaxTRESPA cpu={a}, config base "
                        f"is {lc['base_cpu_per_user']}/{lc['base_cpu_per_account']}; "
                        "in enforce mode sqp sets the QOS to the config's base at startup")
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
        # Jobs a pulse could release: held by a CPU cap, in the pulsed QOS, and
        # small enough for some node's free space right now.
        nf = self.node_free(nodes, parts)
        held = [j for j in pend if j["reason"] in slurm.CAP_REASONS
                and j["qos"] == self.cfg["limits"]["qos_name"]
                and any(fc >= j["cpus"] and fm >= (j.get("req_mem") or j["mem"])
                        for fc, fm, _ in nf.values())]

        if self.cfg["limits"]["mode"] == "global":
            before = (self.limiter.cur_u, self.limiter.cur_a)
            change = self.limiter.observe(idle_frac, len(held))
            if change:
                self.set_caps(change, before, idle_frac, len(held))
        self._write_atomic(self.status_path, json.dumps(dict(
            ts=time.time(), mode=self.mode, version=self.version,
            idle_fraction=round(idle_frac, 4), total_cpu=total_cpu,
            capped_jobs=len(capped), pending=len(pend),
            limits=self.limiter.state(), errors=self.sh.errors), indent=1))

    def set_caps(self, caps, before, idle_frac=None, held=None):
        """Log and, in enforce, apply a change of the per-user/per-account caps.
        Going back to base is allowed even with the disable file present: it only
        ever makes the cluster more conservative."""
        per_user, per_acct = caps
        self.log("limits", idle_fraction=idle_frac, per_user=per_user,
                 per_account=per_acct, held_jobs=held)
        unset = float("inf")                    # a QOS with no cap set reports None
        lowering = per_user <= (before[0] or unset) and per_acct <= (before[1] or unset)
        blocked = self.blocked(("enforce",)) if not lowering else \
            (None if self.mode == "enforce" else f"mode={self.mode}")
        argv = slurm.cmd_set_qos_cpu_limits(self.cfg["limits"]["qos_name"], per_user, per_acct)
        self.intend("set_qos_cpu_limits", slurm.cmdline(argv), self.limiter.why, blocked,
                    lambda: slurm.apply(argv, timeout=30.0),
                    qos=self.cfg["limits"]["qos_name"], before_user=before[0],
                    before_account=before[1], per_user=per_user, per_account=per_acct,
                    idle_fraction=idle_frac, held_jobs=held)

    def restore_caps(self, when: str):
        """Put the caps back to base if the live QOS differs: at startup, in case a
        previous run died mid-pulse, and at shutdown, in case this one is in one."""
        if self.cfg["limits"]["mode"] != "global" or self.mode != "enforce":
            return
        base = (self.limiter.base_u, self.limiter.base_a)
        try:
            live = slurm.qos_cpu_limits(self.cfg["limits"]["qos_name"])
        except slurm.SlurmError as e:
            self.log("error", where="limits", detail=repr(e))
            return
        if live != base:
            self.limiter.reset()
            self.limiter.why = f"restore base caps at {when}"
            self.set_caps(base, live)

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
        self.open_logs()
        self.log("start", version=__import__("sqp").__version__,
                 limits_mode=self.cfg["limits"]["mode"],
                 cadence=self.cfg["cadence"])
        self.preflight()
        self.restore_caps("startup")
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
            self.restore_caps("shutdown")
            self.log("stop")


def main(argv=None):
    ap = argparse.ArgumentParser(prog="sqpd")
    ap.add_argument("-c", "--config")
    ap.add_argument("--mode", choices=("observe", "advise", "enforce"))
    ap.add_argument("--dry-run", action="store_true",
                    help="force mode=observe: log every action it would take, "
                         "with the command and the reason, and change nothing")
    ap.add_argument("--log-file", help="override [general] log_file")
    ap.add_argument("--text-log", help="override [general] text_log")
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
    if a.text_log is not None:
        cfg["general"]["text_log"] = a.text_log
    if a.state_dir:
        cfg["general"]["state_dir"] = a.state_dir
    if a.print_config:
        print(config.dump_defaults()); return 0

    d = Daemon(cfg)
    if a.once:
        # the configured logs here too, so --once and a real run produce the
        # same records rather than differing by invocation
        d.open_logs()
        d.preflight()
        d.sh.nodes, d.sh.parts = slurm.nodes(), slurm.partitions()
        d.sh.nodes_at = time.time()
        try:
            d.sh.jobs = slurm.queue()
        except slurm.SlurmError as e:
            d.log("error", where="queue", detail=repr(e))
        d.score_once(d.sh.nodes, d.sh.parts, time.time())
        d.act_once()
        try:                    # a single pass has no "new" jobs: shadow them all
            d.seen = set()
            d.shadow(d.sh.jobs)
        except slurm.SlurmError as e:
            d.log("error", where="queue", detail=repr(e))
        print(d.last_rendered or "(no table produced)")
        return 0
    d.run()
    return 0


if __name__ == "__main__":
    sys.exit(main())
