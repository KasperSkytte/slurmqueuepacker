# slurmqueuepacker

A state-aware partition-placement supplement for Slurm.

It does not replace the scheduler, reorder the queue, or touch fair-share. It decides
only which partitions a job is *allowed* to land in, and it decides that from what the
cluster looks like right now — something Slurm's `job_submit/lua` plugin cannot do on
its own, because the Lua layer has no handle on cluster state and runs synchronously
inside `slurmctld`.

**Design document:** `docs/design.html`

## Status

**v<!-- x-release-please-start-version -->1.0.0<!-- x-release-please-end -->.** The daemon runs and the plugin is tested end to end against a real `slurmctld`
carrying biocloud's topology, including every failure path. Not yet run on a production
cluster; the next step is a dry run there (see [Dry run on a live cluster](#dry-run-on-a-live-cluster)).
Default mode is `observe`: compute and log every decision, change nothing.

## Installing

Needs Python 3.11+ (standard library only) and the Slurm commands `scontrol`, `squeue`
and `sacctmgr`. Run it on the `slurmctld` host. Tested on Slurm 26.05.

### Modes

sqp does only what its `mode` allows, whether you start it by hand or systemd does:

| mode | reads the cluster | writes the policy table the plugin uses | changes QOS limits |
|---|---|---|---|
| `observe` (default) | yes | no | no |
| `advise` | yes | yes | no |
| `enforce` | yes | yes | yes |

In `observe` it runs only `scontrol show`, `squeue` and `sacctmgr show`, and logs what
it *would* do. Any other command is refused before a process is started, so this holds
even under a Slurm admin account. `--dry-run` is the same as `--mode observe`.

### Timing

All in `[cadence]`, in seconds:

| setting | default | what happens |
|---|---|---|
| `node_poll_interval` | 1 | reads nodes and partitions (`scontrol show`) |
| `queue_poll_interval` | 8 | reads the queue (`squeue`) |
| `score_interval` | 1 | rebuilds the placement table; it is rewritten only when a decision changes, or every `policy_max_age / 3` (100 s) so the plugin does not treat it as stale |
| `act_interval` | 15 | reconsiders QOS limits. Raising takes `[limits] hysteresis` (5) idle checks in a row, so at least 75 s; lowering back to base is immediate |

sqp never changes the partition of a job that is already submitted. Placement happens
only at submission, when the plugin reads the current table.

The QOS limits it changes are on an existing QOS, `[limits] qos_name` (default `normal`);
nothing needs creating. The `flex` QOS belongs to `limits.mode = "perjob"`, which is not
implemented yet.

### 1. Install

<!-- x-release-please-start-version -->
```sh
sudo git clone --branch v1.0.0 https://github.com/kasperskytte/slurmqueuepacker /opt/slurmqueuepacker
cd /opt/slurmqueuepacker
python3 tests/test_policy.py                  # ends in ALL PASS
```
<!-- x-release-please-end -->

Run the `python3 -m sqp...` commands from this directory.

### 2. Dry run

```sh
python3 -m sqp.daemon --dry-run --state-dir ~/sqp-dry --log-file ~/sqp-dry/decisions.jsonl
```

It runs in the foreground until Ctrl-C. Read the log from another terminal at any time:
`python3 -m sqp.report ~/sqp-dry/decisions.jsonl --summary`.

Check the `preflight` record first: the partitions sqp will use, and whether your QOS
caps match `[limits]` in the config. More in [Dry run on a live cluster](#dry-run-on-a-live-cluster).

### 3. Run it as a service (still `observe`)

```sh
sudo mkdir -p /etc/sqp
python3 -m sqp.daemon --print-config | sudo tee /etc/sqp/sqp.toml
sudo cp systemd/sqpd.service /etc/systemd/system/
sudo systemctl daemon-reload && sudo systemctl enable --now sqpd
```

The service runs in the mode set in `/etc/sqp/sqp.toml`, which is `observe` as
generated. It logs to `/var/log/sqp/decisions.jsonl`. The defaults are biocloud's;
check `[limits]` (base caps must match the live QOS) and `[topology]` (partition speeds).

### 4. Go live

1. **Plugin.** `lua/job_submit.lua` replaces your `job_submit.lua` and carries biocloud's
   routing rules (GPU, interactive, Open OnDemand), so merge in your own and set the
   constants at the top of the file. Copy it next to `slurm.conf`, set
   `JobSubmitPlugins=lua` and run `scontrol reconfigure`. **This changes placement:**
   until sqp writes a table, the plugin applies its static `SLIM`/`FAT` rule.
2. **`advise`.** Set `mode = "advise"` and `sudo systemctl restart sqpd`. The plugin now
   uses sqp's table.
3. **`enforce`.** Set `mode = "enforce"` and restart. sqp now also changes QOS limits.

### Upgrading

```sh
cd /opt/slurmqueuepacker && sudo git fetch --tags && sudo git checkout vX.Y.Z
sudo systemctl restart sqpd
```

If `lua/` changed (`sudo git diff --stat <old tag> vX.Y.Z -- lua/`), merge it into your
installed plugin and run `scontrol reconfigure`.

### Backing out

- **Instantly:** `sudo touch /etc/sqp/disable`. The plugin falls back to its static
  rule on the next submission, and sqp stops writing the table and changing limits.
- **For good:** restore your old `job_submit.lua`, run `scontrol reconfigure`, then
  `sudo systemctl disable --now sqpd`.
- **QOS limits** raised in `enforce` stay raised after sqp stops. Reset them with
  `sudo sacctmgr -i modify qos normal set MaxTRESPU=cpu=864 MaxTRESPA=cpu=1760`,
  using your QOS name and base values.

## Releases

Versions and `CHANGELOG.md` are managed by
[release-please](https://github.com/googleapis/release-please). Write commits to `main`
as [Conventional Commits](https://www.conventionalcommits.org/): `fix:` makes a patch
release, `feat:` a minor one, and `feat!:` or a `BREAKING CHANGE:` footer a major one.
Other types (`chore:`, `docs:`, `test:`, ...) do not trigger a release. release-please
keeps a release PR open; merging it tags the release, publishes it on GitHub and bumps
the version in `sqp/__init__.py` and this README.

## Layout

```
sqp/
  config.py       every tunable, one TOML file, documented defaults
  policy.py       phi, scoring, bucket table, Lua rendering  (authoritative)
  slurm.py        scontrol/squeue/sacctmgr interface
  limits.py       global elastic limit controller + per-job promoter
  daemon.py       the four-thread control loop
  report.py       reads the decision log back as a timeline and summary
lua/
  job_submit.lua  the plugin: one table lookup, everything else is fallback
etc/sqp.toml      example configuration
systemd/          unit file
tests/
  test_policy.py    behavioural tests for the placement core
  testcluster.sh    throwaway slurmctld with real topology, unprivileged
tools/
  dump2sqlite.py   mysqldump of the Slurm accounting DB -> standalone SQLite
  normalize.py     typed `jobs` + `jobstats` tables with derived columns
  analyze.py       workload characterisation (first pass)
  stranding.py     per-node occupancy reconstruction; stranded CPU-hours
  stranding2.py    breakdown by partition and by job footprint
  simulate.py      trace-driven replay of the queue against a placement policy
docs/
  design.html      the design document
```

## Running the daemon

```sh
python3 -m sqp.daemon --print-config > /etc/sqp/sqp.toml   # all tunables, documented
python3 -m sqp.daemon -c /etc/sqp/sqp.toml --once          # one pass, table to stdout
python3 -m sqp.daemon -c /etc/sqp/sqp.toml --dry-run       # run, change nothing, log intent
python3 -m sqp.daemon -c /etc/sqp/sqp.toml                 # run
python3 -m sqp.report /var/log/sqp/decisions.jsonl         # read the decision log
python3 tests/test_policy.py                               # behavioural tests
```

### Why the plugin filters feasibility itself

The bucket table is indexed by (memory-per-CPU, CPUs, walltime), and its top buckets are
open-ended. A 24-CPU/2.2 TB job and a 24-CPU/0.86 TB job share a bucket, so a partition set
chosen for the smaller one can be handed to the larger, which none of its partitions can
hold. The daemon therefore emits each partition's largest node alongside the table, and the
plugin drops partitions that cannot hold the job — recomputing feasibility across all
partitions if the set it was given is unusable. Corrections are logged as `refit`.

### Testing the plugin without a cluster

`tests/testcluster.sh` starts a throwaway `slurmctld` as your own user on port 7817,
with the real node and partition topology and no `slurmd`. Jobs stay `PENDING`, which is
enough: `job_submit` runs at submission, so the partition it chose is visible immediately.

```sh
tests/testcluster.sh start
export SLURM_CONF=/tmp/sqp-cluster/slurm.conf
python3 -m sqp.daemon -c /tmp/sqp-cluster/sqp.toml &     # writes the policy table
sbatch -n 1 --mem=32G -t 10 --wrap='sleep 60'            # check squeue for the partition
scontrol update nodename=bio-node[14-15] state=drain reason=test   # watch decisions change
tests/testcluster.sh stop
```

### Dry run on a live cluster

`--dry-run` (the same as `--mode observe`) runs the full loop against the real cluster
and changes nothing.

```sh
python3 -m sqp.daemon -c etc/sqp.toml --dry-run \
    --state-dir ~/sqp-dry --log-file ~/sqp-dry/decisions.jsonl
python3 -m sqp.report ~/sqp-dry/decisions.jsonl              # timeline + summary
python3 -m sqp.report ~/sqp-dry/decisions.jsonl --summary    # after a week
```

The log is JSONL, one record per line, each with `ts` and a readable `time`:

| event | what it records |
|---|---|
| `preflight` | what this run can change (`actuation`, `writes_table`), the partitions it will use, and whether the live QOS caps differ from `base_cpu_per_*` |
| `action` | every change sqpd would make: `action`, `cmd`, `why`, `executed`, and `blocked` (why it did not run). Policy-table writes include the per-bucket `changes` with before/after partitions and costs |
| `placement` | each job first seen in the queue: where Slurm put it (`actual`), where the plugin would have (`would`), the bucket and refit that led there (`why`), the cost per partition for the job's real shape, and a `verdict` (`same`/`different` for pending jobs, `allowed`/`excluded` for jobs already running) |
| `policy`, `limits` | aggregate state at each table or cap change |
| `error` | each distinct poll or scoring failure, once |

The table it would have written goes to `<state_dir>/policy.dryrun.lua`, not the path the
plugin reads. Limit changes run against a simulated cap, as if every earlier change had
been applied.

Nothing in a dry run depends on the account's Slurm permissions. Three guards, each
enough on its own:

1. Each action checks the mode before it is attempted.
2. Every state-changing command goes through `slurm.apply()`, which runs nothing
   unless the daemon was started in `enforce` mode.
3. Every process sqp starts goes through one function, which outside `enforce` mode
   refuses anything but `scontrol show`, `squeue` and `sacctmgr show`.

The tests check each guard (sections 10 and 10b of `tests/test_policy.py`).

Caveats. Placement is evaluated against the table as it stood when the job was first
polled (up to `queue_poll_interval` after submission), not at the exact moment of
submission. Jobs that start and finish between two polls are not seen. Jobs present
when the daemon starts are not evaluated; `--once` evaluates every job currently queued.

Creating the `disable_file` reverts to the site's static rule on the next submission,
with no restart. If the daemon dies, the table goes stale past `policy_max_age` and the
plugin falls through to the same static rule — degrading to today's behaviour, not to an
outage.

Partitions are discovered from `scontrol show partitions` unless `batch_partitions`
lists them. Either way, three exclusions apply, each overridable in `[topology]`:
`exclude_partitions` (by name, empty by default), `exclude_interactive` (drops any
partition named `interactive`, on by default) and `exclude_gpu_nodes` (ignores nodes whose
`Gres` or `CfgTRES` has a GPU, on by default; a partition left with no nodes is dropped).
The `preflight` log record lists what was kept, what was excluded and why.

Everything is tunable in `sqp.toml`: cadence, demand mix, bucket edges, tolerance,
starvation budgets, limit ceilings and thresholds, cohort keys. Nothing in the daemon
reads a magic number directly.

## Getting the data in

The accounting dump is read directly; it is never restored over a live database
(the dump contains `DROP TABLE` / `CREATE DATABASE slurm_acct_db`). The dump and the
SQLite files derived from it hold user and job records and are kept out of git
(`.gitignore`).

```sh
bzcat slurm_acct_db_backup.bz2 > dump.sql
python3 tools/dump2sqlite.py dump.sql biocloud.sqlite --cluster biocloud
python3 tools/normalize.py biocloud.sqlite
```

`dump2sqlite.py` takes ~30 min on a 5.7 GB dump; `normalize.py` ~4 min.

## Replaying the queue

```sh
python3 tools/simulate.py --start 2026-03-09 --days 14 --warmup 4
```

Replays the real arrival trace — eligible time, shape, recorded runtime, recorded
priority — against each placement policy in turn, and prints work completed, stranded
CPU-hours and wait-time distributions. Policies:

| policy | what it does |
|---|---|
| `actual` | replays the partitions Slurm really assigned (fidelity reference) |
| `static` | reproduces the current `job_submit.lua` rule |
| `feasible` | any partition with a node physically big enough for the job |
| `packer` | gates the feasible set by option-value cost (see the design doc) |

Each policy is replayed twice: with backfill reservation horizons taken from **declared time
limits** (what Slurm does) and from **cohort-p95 history** (`predicted`).

### Units

The unit is the **allocation-hour**, for CPU and for memory. Allocation is what denies hardware
to other people; whether a job uses what it holds is not scored, and job CPU efficiency is
deliberately not modelled. The one efficiency that is modelled is **time**: reservation horizons
can come from declared limits or from cohort history.

### Reading the output honestly

The simulator models a priority-ordered scheduler with per-node backfill reservations computed
from declared deadlines, and enforces `MaxTRESPU=cpu=864` / `MaxTRESPA=cpu=1760`. It does **not**
model fair-share priority decay, `bf_max_job_test`, node drains, or Slurm reservations. Against
recorded history it lands close under load (32,252 vs 28,585 job-hours in the busy window) and
badly off on a quiet cluster (491 vs 5,032). Every run prints the recorded figure for comparison.

**Comparisons between policies within one run are the usable output. Absolute numbers
are not calibrated.**

Multi-node jobs are 0.06% of the trace and are placed as single-node.
