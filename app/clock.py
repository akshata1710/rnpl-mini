"""The one place that answers "what time is it?".

Real code would just use datetime.now(timezone.utc). We read the time from
the database plus a shared offset so the demo and tests can jump weeks
ahead, and every process (API, workers, sweeper) sees the same jump.

Lesson: never call datetime.now() deep inside business logic. Ask a clock
you can control, or you can't test anything time-based.
"""
from datetime import datetime, timedelta

import psycopg


def now(conn: psycopg.Connection) -> datetime:
    """Current time in UTC (timezone-aware)."""
    row = conn.execute(
        "SELECT clock_timestamp() + offset_interval AS now FROM dev_clock"
    ).fetchone()
    return row["now"]


def advance(conn: psycopg.Connection, delta: timedelta) -> datetime:
    conn.execute("UPDATE dev_clock SET offset_interval = offset_interval + %s", (delta,))
    return now(conn)


def reset(conn: psycopg.Connection) -> None:
    conn.execute("UPDATE dev_clock SET offset_interval = '0'")
