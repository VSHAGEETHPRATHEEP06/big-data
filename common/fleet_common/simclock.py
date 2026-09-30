"""Shared simulated clock.

The pipeline compresses time so that a full business day can be demonstrated
in minutes. With the default speed-up of 120:

    1 real second  = 2 simulated minutes
    30 real seconds = 1 simulated hour
    12 real minutes = 1 simulated day

Simulated timestamps are Sri Lanka local time (UTC+05:30) written *without* an
offset, e.g. ``2026-09-01T08:15:00``. Spark, PostgreSQL and Python all treat
them as plain wall-clock values, so hours and days line up with local time.

The clock anchor (real start time, simulated start time, speed-up) lives in one
PostgreSQL row, ``ops.sim_clock``, so every container computes the same
simulated "now". The streaming simulator owns the row. If the stack was
stopped for a while, the simulator re-bases the anchor on start-up so that
simulated time continues from where it stopped instead of jumping ahead.
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta


@dataclass(frozen=True)
class SimClock:
    real_start: float  # unix epoch seconds (real time) of the anchor
    sim_start: datetime  # simulated time at the anchor (naive local time)
    speedup: float  # simulated seconds per real second

    def sim_now(self, real_now: float | None = None) -> datetime:
        real_now = time.time() if real_now is None else real_now
        return self.sim_start + timedelta(seconds=(real_now - self.real_start) * self.speedup)

    def to_real(self, sim_dt: datetime) -> float:
        """Real epoch seconds at which the clock shows ``sim_dt``."""
        return self.real_start + (sim_dt - self.sim_start).total_seconds() / self.speedup

    def sim_date(self, real_now: float | None = None) -> date:
        return self.sim_now(real_now).date()

    @property
    def real_seconds_per_sim_day(self) -> float:
        return 86400.0 / self.speedup


def fmt_sim(ts: datetime) -> str:
    """Format a simulated timestamp the way it appears in events and files."""
    return ts.replace(microsecond=0).isoformat()


def day_start(d: date) -> datetime:
    return datetime(d.year, d.month, d.day)


# ---------------------------------------------------------------------------
# persistence in PostgreSQL (table created by db/init/01_schema.sql)
# ---------------------------------------------------------------------------
def _row_to_clock(row) -> SimClock:
    real_start, sim_start, speedup = row
    return SimClock(float(real_start), sim_start, float(speedup))


def load_clock(conn) -> SimClock | None:
    """Read the shared clock, or None if the simulator has not created it yet."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT extract(epoch FROM real_start), sim_start, speedup "
            "FROM ops.sim_clock WHERE id = 1"
        )
        row = cur.fetchone()
    conn.commit()
    return _row_to_clock(row) if row else None


def ensure_clock(
    conn,
    sim_start: datetime,
    speedup: float,
    resume_gap_seconds: float,
    logger=None,
) -> SimClock:
    """Create the clock if missing. Re-base it when it was paused or re-configured.

    * First start: anchor = (now, ``sim_start``, ``speedup``).
    * Heartbeat older than ``resume_gap_seconds``: the stack was stopped, so the
      anchor moves to "now" while simulated time continues from the last heartbeat.
    * ``speedup`` changed in ``.env``: re-base at the current simulated time.
    """
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO ops.sim_clock (id, real_start, sim_start, speedup, last_heartbeat) "
            "VALUES (1, now(), %s, %s, now()) ON CONFLICT (id) DO NOTHING",
            (sim_start, speedup),
        )
        cur.execute(
            "SELECT extract(epoch FROM real_start), sim_start, speedup, "
            "       extract(epoch FROM last_heartbeat), extract(epoch FROM now()) "
            "FROM ops.sim_clock WHERE id = 1 FOR UPDATE"
        )
        real_start, sim_start_db, speedup_db, heartbeat, now = cur.fetchone()
        clock = SimClock(float(real_start), sim_start_db, float(speedup_db))
        now = float(now)
        paused = heartbeat is not None and now - float(heartbeat) > resume_gap_seconds
        new_speed = abs(clock.speedup - speedup) > 1e-9
        if paused or new_speed:
            # simulated time reached when the clock last ticked
            resume_at = clock.sim_now(float(heartbeat) if paused else now)
            cur.execute(
                "UPDATE ops.sim_clock SET real_start = to_timestamp(%s), sim_start = %s, "
                "speedup = %s, last_heartbeat = to_timestamp(%s), rebase_count = rebase_count + 1 "
                "WHERE id = 1",
                (now, resume_at.replace(microsecond=0), speedup, now),
            )
            clock = SimClock(now, resume_at.replace(microsecond=0), speedup)
            if logger:
                logger.warning(
                    "sim_clock_rebased",
                    extra={
                        "reason": "paused" if paused else "speedup_changed",
                        "resume_sim_time": fmt_sim(clock.sim_start),
                        "speedup": speedup,
                    },
                )
    conn.commit()
    return clock


def heartbeat(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("UPDATE ops.sim_clock SET last_heartbeat = now() WHERE id = 1")
    conn.commit()


def wait_for_clock(conn_factory, logger=None, timeout: float = 600.0, poll: float = 3.0) -> SimClock:
    """Block until the simulator has created the shared clock."""
    deadline = time.time() + timeout
    while True:
        conn = conn_factory()
        try:
            clock = load_clock(conn)
        finally:
            conn.close()
        if clock:
            return clock
        if time.time() > deadline:
            raise TimeoutError("simulated clock not created by the stream simulator")
        if logger:
            logger.info("waiting_for_sim_clock")
        time.sleep(poll)
