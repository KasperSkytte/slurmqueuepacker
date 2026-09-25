"""Elastic limits, as a short pulse.

The QOS caps exist to stop one user swallowing the cluster inside a minute, and
they must stay in place almost all the time: a user who submits a large pool of
jobs while the caps are raised can hold nodes for days, because jobs are not
preemptible. So the caps are never raised for long. When jobs are held only by
the per-user or per-account CPU cap, and they would fit in idle hardware that
nobody else is waiting for, the caps are raised for a minute -- long enough for
the scheduler to start some of those jobs -- and then put back to base. Between
pulses there is a cooldown. The raise is uniform, the same for every user, so
fair-share still decides who gets the room.
"""
from __future__ import annotations
import time


class LimitPulse:
    """Moves MaxTRESPU/MaxTRESPA on one QOS: base, briefly up, back to base."""

    def __init__(self, cfg):
        c = cfg["limits"]
        self.base_u = c["base_cpu_per_user"]
        self.base_a = c["base_cpu_per_account"]
        self.ceiling = c["ceiling"]
        self.raise_above = c["raise_above"]
        self.lower_below = c["lower_below"]
        self.hysteresis = c["hysteresis"]
        self.pulse = c["pulse_seconds"]
        self.cooldown = c["cooldown_seconds"]
        self.qos = c["qos_name"]
        self.cur_u, self.cur_a = self.base_u, self.base_a
        self.streak = 0
        self.raised_at = None
        self.lowered_at = float("-inf")
        self.why = ""

    @property
    def raised(self) -> bool:
        return self.raised_at is not None

    def observe(self, idle_fraction: float, held_that_fit: int, now: float | None = None):
        """Return (per_user, per_account) if the caps should change, else None.

        held_that_fit: pending jobs held by a CPU cap that fit in free space now.
        """
        now = time.time() if now is None else now
        if self.raised:
            if now - self.raised_at >= self.pulse:
                self.why = f"the {self.pulse:.0f} s pulse is over; back to base"
            elif idle_fraction < self.lower_below:
                self.why = (f"the cluster filled up (idle {idle_fraction:.0%} < "
                            f"{self.lower_below:.0%}); back to base early")
            else:
                return None
            return self.reset(now)

        idle = idle_fraction >= self.raise_above
        self.streak = self.streak + 1 if idle and held_that_fit else 0
        if self.streak < self.hysteresis or now - self.lowered_at < self.cooldown:
            return None
        self.streak = 0
        self.raised_at = now
        self.cur_u = int(self.base_u * self.ceiling)
        self.cur_a = int(self.base_a * self.ceiling)
        self.why = (f"{held_that_fit} jobs held only by the CPU cap would fit in idle "
                    f"hardware, and {idle_fraction:.0%} of the cluster has been idle for "
                    f"{self.hysteresis} checks; raise for {self.pulse:.0f} s")
        return self.cur_u, self.cur_a

    def reset(self, now: float | None = None):
        """Back to base. Returns the base caps."""
        self.raised_at = None
        self.lowered_at = time.time() if now is None else now
        self.cur_u, self.cur_a = self.base_u, self.base_a
        return self.cur_u, self.cur_a

    @property
    def multiple(self) -> float:
        return self.cur_u / self.base_u

    def state(self) -> dict:
        return dict(per_user=self.cur_u, per_account=self.cur_a, raised=self.raised,
                    streak=self.streak)


class PerJobPromoter:
    """The surgical alternative: move individual pending jobs to a flex QOS.

    Kept because it is strictly more conservative -- it can only ever affect one
    named job at a time -- but note the measured bias: it promotes only jobs that
    can start immediately, which systematically favours small, easy-to-place work
    over exactly the large high-ratio jobs that wait longest. The global pulse has
    no such bias, which is why it is the default. Not wired into sqpd yet.
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
