# Changelog

## 1.0.0 (2026-09-24)

First release.

### Features

* sqpd control loop with `observe`, `advise` and `enforce` modes, and a `job_submit.lua`
  plugin that falls back to the site's static rule on any failure
* dry run (`--dry-run`): logs every intended action with its command and reason, and for
  each job where Slurm placed it versus where sqp would have; every state-changing command
  goes through one guard that is off outside `enforce`
* `sqp.report` reads the decision log as a timeline and summary
* partitions discovered automatically; `interactive` partitions and GPU nodes excluded by
  default, each overridable
* global elastic QOS limits, a trace-driven simulator and workload analysis tools
