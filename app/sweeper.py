"""The sweeper: the janitor that guarantees rule #2.

Rule #2: "Never leave a reservation unpaid past its deadline. It ends
either paid or released." Workers can crash; the sweeper cleans up.

Two jobs:
  1. rescue_stuck_plans: IN_PROGRESS for too long -> the worker died.
     Put the plan back to SCHEDULED with the SAME attempt_count, so the
     retry reuses the same idempotency key.
  2. release_overdue_plans: deadline passed and still not paid ->
     RELEASED, and emit an event so Reservations frees the host's calendar.
"""
import logging
import threading
import time

import psycopg

from app import clock, outbox, plans
from app.config import IN_PROGRESS_TIMEOUT
from app.db import connect

log = logging.getLogger("sweeper")


def rescue_stuck_plans(conn: psycopg.Connection) -> int:
    now = clock.now(conn)
    stuck = conn.execute(
        "SELECT * FROM payment_plans WHERE status = 'IN_PROGRESS' AND claimed_at < %s",
        (now - IN_PROGRESS_TIMEOUT,),
    ).fetchall()
    for plan in stuck:
        if plan["cancel_requested"]:
            # Guest cancelled while the dead worker was mid-charge. We don't
            # know if money moved, so refund "whatever that key did".
            plans.cas_update(conn, plan, status="REFUND_PENDING")
        else:
            plans.cas_update(conn, plan, status="SCHEDULED", charge_at=now,
                             claimed_by=None, claimed_at=None)
        log.warning("rescued plan %s from dead worker %s", plan["id"], plan["claimed_by"])
    return len(stuck)


def release_overdue_plans(conn: psycopg.Connection) -> int:
    now = clock.now(conn)
    overdue = conn.execute(
        """SELECT * FROM payment_plans
            WHERE status IN ('SCHEDULED', 'NEEDS_NEW_CARD') AND deadline_at <= %s""",
        (now,),
    ).fetchall()
    released = 0
    for plan in overdue:
        with conn.transaction():
            if plans.cas_update(conn, plan, status="RELEASED"):
                outbox.emit(conn, "PlanReleased",
                            {"reservation_id": plan["reservation_id"], "plan_id": plan["id"]})
                released += 1
    return released


def run_once(conn: psycopg.Connection) -> tuple[int, int]:
    return rescue_stuck_plans(conn), release_overdue_plans(conn)


def run_forever(stop: threading.Event | None = None, every_seconds: float = 5) -> None:
    stop = stop or threading.Event()
    with connect() as conn:
        while not stop.is_set():
            run_once(conn)
            time.sleep(every_seconds)
