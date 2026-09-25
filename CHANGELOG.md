# Changelog

## [1.1.0](https://github.com/KasperSkytte/slurmqueuepacker/compare/v1.0.0...v1.1.0) (2026-09-25)


### Features

* pin jobs that can start now to the node that best fits them ([5a7cb11](https://github.com/KasperSkytte/slurmqueuepacker/commit/5a7cb11b6f421eded8a303757f8f251ddc431519))
* raise the QOS CPU caps only in short pulses ([5a7cb11](https://github.com/KasperSkytte/slurmqueuepacker/commit/5a7cb11b6f421eded8a303757f8f251ddc431519))
* readable per-job text log ([5a7cb11](https://github.com/KasperSkytte/slurmqueuepacker/commit/5a7cb11b6f421eded8a303757f8f251ddc431519))


### Bug Fixes

* count powered-down nodes as available ([5a7cb11](https://github.com/KasperSkytte/slurmqueuepacker/commit/5a7cb11b6f421eded8a303757f8f251ddc431519))
* keep all output under --state-dir unless told otherwise ([b5bf659](https://github.com/KasperSkytte/slurmqueuepacker/commit/b5bf659105fa2b1eee94541b9130024488ae2ebc))
* pin jobs waiting on a dependency like any other job ([5a7cb11](https://github.com/KasperSkytte/slurmqueuepacker/commit/5a7cb11b6f421eded8a303757f8f251ddc431519))
* recognise jobs held by the per-account CPU cap ([5a7cb11](https://github.com/KasperSkytte/slurmqueuepacker/commit/5a7cb11b6f421eded8a303757f8f251ddc431519))
* refuse non-read-only Slurm commands outside enforce mode ([89f314c](https://github.com/KasperSkytte/slurmqueuepacker/commit/89f314c9c8d48889f8f6a68970d10ed8f2e76225))
* **systemd:** run sqpd from the install directory ([a8f01af](https://github.com/KasperSkytte/slurmqueuepacker/commit/a8f01af4945a98bd48528efa640d167cfbdd3985))
* **tests:** use built-in defaults instead of a path assumed not to exist ([97edfe9](https://github.com/KasperSkytte/slurmqueuepacker/commit/97edfe958d5b25249d2df33c87cce9eacb5b6e05))
* warn that limits.mode perjob is not implemented ([f04943f](https://github.com/KasperSkytte/slurmqueuepacker/commit/f04943fb73e4354316e97ccdc30d4a27c797284d))

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
