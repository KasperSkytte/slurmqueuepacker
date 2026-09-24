"""Elastic limits.

The QOS caps exist to stop one user swallowing the cluster inside a minute. They
are not a rationing scheme. When the cluster is persistently idle the cap can be
raised -- but raised *uniformly*, for everyone, so that every user faces the same
number and fair-share keeps deciding who fills the room. Raising is slow and
stepwise; lowering is instant.
"""
from __future__ import annotations
import time


class GlobalLimitController:
    """Moves MaxTRESPU/MaxTRESPA on one QOS between a base and a ceiling.

    Asymmetric on purpose: it takes `hysteresis` consecutive idle observations to
    step up, and a single busy observation to snap all the way back. Being slow to
    give and quick to take back is the safe direction -- a cap that is too high for
    a few seconds can commit nodes for days, because jobs are not preemptible.
    """

    def __init__(self, cfg):
        c = cfg["limits"]
        self.base_u = c["base_cpu_per_user"]
        self.base_a = c["base_cpu_per_account"]
        self.ceiling = c["ceiling"]
        self.raise_above = c["raise_above"]
        self.lower_below = c["lower_below"]
        self.hysteresis = c["hysteresis"]
        self.step = c["step"]
        self.qos = c["qos_name"]
        self.cur_u = float(self.base_u)
        self.cur_a = float(self.base_a)
        self.streak = 0
        self.last_change = 0.0
        self.raised_since = None
        self.why = ""

    def observe(self, idle_fraction: float, now: float | None = None):
        """Return (per_user, per_account) if the caps should change, else None."""
        now = now or time.time()
        prev = (self.cur_u, self.cur_a)
        if idle_fraction >= self.raise_above:
            self.streak += 1
            if self.streak >= self.hysteresis:
                self.cur_u = min(self.cur_u * self.step, self.base_u * self.ceiling)
                self.cur_a = min(self.cur_a * self.step, self.base_a * self.ceiling)
                self.why = (f"idle placeable fraction {idle_fraction:.3f} >= raise_above "
                            f"{self.raise_above} for {self.streak} consecutive intervals; "
                            f"step x{self.step}, capped at {self.ceiling}x base")
                self.streak = 0
        elif idle_fraction < self.lower_below:
            self.cur_u, self.cur_a = float(self.base_u), float(self.base_a)
            self.why = (f"idle placeable fraction {idle_fraction:.3f} < lower_below "
                        f"{self.lower_below}; snap back to base")
            self.streak = 0
        else:
            self.streak = 0

        if (round(self.cur_u), round(self.cur_a)) == (round(prev[0]), round(prev[1])):
            return None
        self.last_change = now
        self.raised_since = now if self.cur_u > self.base_u else None
        return int(self.cur_u), int(self.cur_a)

    @property
    def multiple(self) -> float:
        return self.cur_u / self.base_u

    def state(self) -> dict:
        return dict(per_user=int(self.cur_u), per_account=int(self.cur_a),
                    multiple=round(self.multiple, 3), streak=self.streak)


class PerJobPromoter:
    """The surgical alternative: move individual pending jobs to a flex QOS.

    Kept because it is strictly more conservative -- it can only ever affect one
    named job at a time -- but note the measured bias: it promotes only jobs that
    can start immediately, which systematically favours small, easy-to-place work
    over exactly the large high-ratio jobs that wait longest. Global mode has no
    such bias, which is why it is the default.
    """

    def __init__(self, cfg):
        c = cfg["limits"]
        self.qos = c["flex_qos_name"]
        self.ceiling = c["ceiling"]
        self.reserve = c["flex_reserve"]
        self.phi_tol = c["flex_phi_tolerance"]
        self.promoted: dict[str, float] = {}

    def candidates(self, capped_jobs, running_cpu_by_user, base_cap, starving):
        """Overdue-and-capped first: the cap is precisely why they are overdue."""
        out = []
        for j in sorted(capped_jobs,
                        key=lambda j: (j["jobid"] not in starving, -j["priority"])):
            held = running_cpu_by_user.get(j["user"], 0)
            if held + j["cpus"] > base_cap * self.ceiling:
                continue
            out.append(j)
        return out
