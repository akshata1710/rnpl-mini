"""Charge workers (Step 5 on the slide).

The loop, in plain words:
  1. CLAIM:   lock a batch of due SCHEDULED plans (SKIP LOCKED), mark them
              IN_PROGRESS, COMMIT. Now no other worker can take them.
  2. CHARGE:  for each plan, call the processor with key plan-<id>-attempt-<n>.
              No transaction is open during this network call.
  3. RECORD:  in ONE transaction: cas_update the plan with the outcome
              and write the outbox event.

Run several of these at once (`python -m app.cli worker` in many terminals).
They never charge the same plan twice; tests/test_rnpl.py proves it.
"""
import logging
import socket
import threading
import time
from datetime import timedelta

import psycopg

from app import clock, fake_psp, outbox, plans
from app.config import BATCH_SIZE, MAX_SOFT_RETRIES, POLL_SECONDS, RETRY_BACKOFF, UNKNOWN_RETRY_DELAY
from app.db import connect

log = logging.getLogger("worker")


class SimulatedCrash(Exception):
    """Used by tests/demo: 'the worker died right after the processor charged'."""


# ---------------------------------------------------------------------------
# 1. CLAIM
# ---------------------------------------------------------------------------
CLAIM_SQL = """
    SELECT id
      FROM payment_plans
     WHERE status = 'SCHEDULED'
       AND charge_at <= %s
     ORDER BY charge_at          -- oldest first (most urgent)
     LIMIT %s
       FOR UPDATE SKIP LOCKED    -- lock these rows; skip rows another worker locked
"""
# This WHERE + ORDER BY matches the partial index
#   idx_plans_due ON payment_plans (charge_at) WHERE status = 'SCHEDULED'
# exactly, so Postgres walks the index from the oldest charge_at and stops
# after LIMIT rows. Run `EXPLAIN` on it (see GUIDE.md) to see.


def claim_batch(conn: psycopg.Connection, worker_id: str, batch_size: int = BATCH_SIZE) -> list[dict]:
    now = clock.now(conn)
    with conn.transaction():
        ids = [r["id"] for r in conn.execute(CLAIM_SQL, (now, batch_size)).fetchall()]
        if not ids:
            return []
        claimed = conn.execute(
            """
            UPDATE payment_plans
               SET status = 'IN_PROGRESS', claimed_by = %s, claimed_at = %s,
                   version = version + 1, updated_at = now()
             WHERE id = ANY(%s)
            RETURNING *
            """,
            (worker_id, now, ids),
        ).fetchall()
    # <- COMMIT happened here. Locks are released, but the rows now say
    #    IN_PROGRESS, so no other worker's claim query will match them.
    #    We do NOT hold a transaction open while calling the processor
    #    (a slow network call inside a transaction holds locks and a DB
    #    connection hostage).
    return claimed


# ---------------------------------------------------------------------------
# 2. CHARGE
# ---------------------------------------------------------------------------
def idempotency_key(plan: dict) -> str:
    """Same plan + same attempt number = same key = at most one charge.

    attempt_count only goes up after a DEFINITE decline. After a crash or a
    timeout it stays the same, so the retry reuses the key and the processor
    returns the original result instead of charging again."""
    return f"plan-{plan['id']}-attempt-{plan['attempt_count']}"


def charge_plan(conn: psycopg.Connection, plan: dict, crash_after_charge: bool = False) -> str:
    key = idempotency_key(plan)
    try:
        result = fake_psp.charge(key, plan["card_token"], plan["amount_cents"],
                                 plan["currency"], plan_id=plan["id"])
    except fake_psp.ProcessorTimeout:
        result = fake_psp.ChargeResult(outcome="unknown", decline_code="processor_timeout")

    if crash_after_charge:
        # The processor has already committed. We die before recording it.
        # The plan stays IN_PROGRESS until the sweeper rescues it.
        raise SimulatedCrash(f"worker died after charging plan {plan['id']}")

    if result.replayed:
        log.info("plan %s: key %s replayed -> %s (no new charge)", plan["id"], key, result.outcome)
    return record_outcome(conn, plan, result)


# ---------------------------------------------------------------------------
# 3. RECORD
# ---------------------------------------------------------------------------
def _changes_for(plan: dict, result: fake_psp.ChargeResult, now) -> tuple[dict, tuple | None]:
    """Pure function: outcome -> (column changes, outbox event). Easy to unit test."""
    rid, pid = plan["reservation_id"], plan["id"]

    if result.outcome == "succeeded":
        return (
            {"status": "PAID", "psp_charge_id": result.charge_id, "last_error": None},
            ("PaymentSucceeded", {"reservation_id": rid, "plan_id": pid,
                                  "amount_cents": plan["amount_cents"], "currency": plan["currency"]}),
        )

    if result.outcome == "unknown":
        # Did money move? We don't know. Try again soon with the SAME
        # attempt number (same key). The processor will tell us the truth.
        return (
            {"status": "SCHEDULED", "charge_at": now + UNKNOWN_RETRY_DELAY,
             "last_error": result.decline_code},
            None,
        )

    next_attempt = plan["attempt_count"] + 1  # definite decline -> next key

    if result.outcome == "soft_decline" and next_attempt <= MAX_SOFT_RETRIES:
        retry_at = now + RETRY_BACKOFF[next_attempt - 1]
        if retry_at < plan["deadline_at"]:
            return (
                {"status": "SCHEDULED", "attempt_count": next_attempt,
                 "charge_at": retry_at, "last_error": result.decline_code},
                ("PaymentRetryScheduled", {"reservation_id": rid, "plan_id": pid,
                                           "retry_at": retry_at.isoformat(),
                                           "reason": result.decline_code}),
            )

    # Hard decline, or soft decline with no retries/time left: ask the guest.
    return (
        {"status": "NEEDS_NEW_CARD", "attempt_count": next_attempt, "last_error": result.decline_code},
        ("PaymentNeedsNewCard", {"reservation_id": rid, "plan_id": pid,
                                 "reason": result.decline_code,
                                 "deadline_at": plan["deadline_at"].isoformat()}),
    )


def record_outcome(conn: psycopg.Connection, plan: dict, result: fake_psp.ChargeResult) -> str:
    now = clock.now(conn)
    changes, event = _changes_for(plan, result, now)
    with conn.transaction():
        if plans.cas_update(conn, plan, **changes):
            if event:
                outbox.emit(conn, *event)   # same transaction as the status change
            return changes["status"]
        # Lost the version race: someone changed the plan while we were charging.
        return _handle_concurrent_change(conn, plan, result)


def _handle_concurrent_change(conn, plan: dict, result: fake_psp.ChargeResult) -> str:
    current = plans.get_plan(conn, plan["id"])

    if current["status"] == "IN_PROGRESS" and current["cancel_requested"]:
        # Guest cancelled mid-charge. If money moved (or might have), refund it.
        if result.outcome in ("succeeded", "unknown"):
            plans.cas_update(conn, current, status="REFUND_PENDING")
            outbox.emit(conn, "PaymentRefundScheduled",
                        {"reservation_id": plan["reservation_id"], "plan_id": plan["id"]})
            return "REFUND_PENDING"
        plans.cas_update(conn, current, status="CANCELLED")
        return "CANCELLED"

    # Most likely the sweeper decided we were dead and put the plan back to
    # SCHEDULED. Do nothing: the next attempt reuses the same key and the
    # processor replays our result. That's why idempotency matters.
    log.warning("plan %s changed under worker (now %s); leaving it alone", plan["id"], current["status"])
    return "SKIPPED"


# ---------------------------------------------------------------------------
# Refunds (from a cancel after payment, or a cancel during a charge)
# ---------------------------------------------------------------------------
def process_refunds(conn: psycopg.Connection, limit: int = BATCH_SIZE) -> int:
    """No claim/lock step needed here: refunds are idempotent on a fixed key
    per plan, so two workers refunding the same plan is harmless."""
    rows = conn.execute(
        "SELECT * FROM payment_plans WHERE status = 'REFUND_PENDING' ORDER BY id LIMIT %s", (limit,)
    ).fetchall()
    for plan in rows:
        status = fake_psp.refund(refund_key=f"refund-plan-{plan['id']}",
                                 charge_key=idempotency_key(plan))
        with conn.transaction():
            final = "REFUNDED" if status == "refunded" else "CANCELLED"
            if plans.cas_update(conn, plan, status=final):
                if final == "REFUNDED":
                    outbox.emit(conn, "PaymentRefunded",
                                {"reservation_id": plan["reservation_id"], "plan_id": plan["id"],
                                 "amount_cents": plan["amount_cents"], "currency": plan["currency"]})
    return len(rows)


# ---------------------------------------------------------------------------
# Loops
# ---------------------------------------------------------------------------
def run_once(conn: psycopg.Connection, worker_id: str = "worker-test", batch_size: int = BATCH_SIZE) -> int:
    """Claim and process one batch. Returns how many plans were processed."""
    batch = claim_batch(conn, worker_id, batch_size)
    for plan in batch:
        try:
            charge_plan(conn, plan)
        except Exception:
            # Don't let one bad plan kill the loop. The plan stays IN_PROGRESS
            # and the sweeper will hand it back after IN_PROGRESS_TIMEOUT.
            log.exception("plan %s failed", plan["id"])
    process_refunds(conn)
    return len(batch)


def run_forever(worker_id: str | None = None, stop: threading.Event | None = None) -> None:
    worker_id = worker_id or f"{socket.gethostname()}-{threading.get_ident()}"
    stop = stop or threading.Event()
    log.info("worker %s started", worker_id)
    with connect() as conn:
        while not stop.is_set():
            if run_once(conn, worker_id) == 0:
                time.sleep(POLL_SECONDS)   # nothing due; don't hammer the DB
