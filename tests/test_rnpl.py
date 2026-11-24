"""Failure-injection tests: add these EARLY, before launch.
Each test is one story. Read them as documentation.

Needs a running Postgres (docker compose up -d). WIPES the database.
Run:  pytest -v
"""
import threading
from datetime import date, datetime, timedelta, timezone

import pytest

from app import booking, clock, metrics, plans, relay, sweeper, worker
from app.config import CHARGE_BUFFER, IN_PROGRESS_TIMEOUT
from app.db import connect, init_db


# ---------------------------------------------------------------- helpers
@pytest.fixture
def conn():
    init_db()
    with connect() as c:
        yield c


def book(conn, card="tok_ok", listing_id=1, days_ahead=30):
    check_in = clock.now(conn).date() + timedelta(days=days_ahead)
    return booking.create_reservation(conn, listing_id, "guest@example.com", check_in, 2, card)


def jump_to(conn, ts: datetime):
    """Move the shared clock so that 'now' is just past ts."""
    clock.advance(conn, ts - clock.now(conn) + timedelta(seconds=1))


def plan_of(conn, booked):
    return plans.get_plan(conn, booked["plan"]["id"])


def reservation_of(conn, booked):
    return conn.execute("SELECT * FROM reservations WHERE id = %s", (booked["reservation"]["id"],)).fetchone()


def psp_charges(conn, plan_id):
    return conn.execute("SELECT * FROM psp_charges WHERE plan_id = %s ORDER BY created_at", (plan_id,)).fetchall()


def notification_kinds(conn, booked):
    return [r["kind"] for r in conn.execute(
        "SELECT kind FROM notifications WHERE reservation_id = %s ORDER BY id", (booked["reservation"]["id"],))]


# ---------------------------------------------------------------- happy path
def test_happy_path(conn):
    b = book(conn)
    p = b["plan"]
    assert b["reservation"]["status"] == "CONFIRMED_UNPAID"
    assert p["status"] == "SCHEDULED"
    assert p["charge_at"] <= p["deadline_at"] - CHARGE_BUFFER   # charged BEFORE the deadline, with buffer

    assert worker.run_once(conn) == 0          # not due yet -> nothing happens

    jump_to(conn, p["charge_at"])
    assert worker.run_once(conn) == 1
    assert plan_of(conn, b)["status"] == "PAID"
    assert len(psp_charges(conn, p["id"])) == 1

    relay.drain(conn)                          # outbox -> consumers
    assert reservation_of(conn, b)["status"] == "PAID"
    assert notification_kinds(conn, b) == ["ReservationConfirmed", "PaymentSucceeded"]
    assert metrics.reconcile(conn) == []


# ---------------------------------------------------------------- eligibility
def test_eligibility_rules(conn):
    with pytest.raises(booking.NotEligible, match="strict"):
        book(conn, listing_id=4)
    with pytest.raises(booking.NotEligible, match="BRL"):
        book(conn, listing_id=5)
    with pytest.raises(booking.NotEligible, match="too close"):
        book(conn, days_ahead=2)                  # deadline - buffer is already in the past


def test_no_plan_no_booking(conn, monkeypatch):
    """Fail closed: if the plan can't be created, the reservation must not exist."""
    def broken(*a, **k):
        raise RuntimeError("plan service down")
    monkeypatch.setattr(plans, "create_plan", broken)
    with pytest.raises(RuntimeError):
        book(conn)
    assert conn.execute("SELECT count(*) AS n FROM reservations").fetchone()["n"] == 0


def test_deadline_uses_listing_timezone(conn):
    from app.timeutil import cancellation_deadline
    # Tokyo, flexible: 1 day before check-in at 15:00 Tokyo time = 06:00 UTC
    d = cancellation_deadline(date(2026, 12, 20), "Asia/Tokyo", "flexible")
    assert d == datetime(2026, 12, 19, 6, 0, tzinfo=timezone.utc)
    # New York in winter (UTC-5): 15:00 local = 20:00 UTC
    d = cancellation_deadline(date(2026, 12, 20), "America/New_York", "flexible")
    assert d == datetime(2026, 12, 19, 20, 0, tzinfo=timezone.utc)


# ---------------------------------------------------------------- rule 1: never charge twice
def test_many_workers_never_double_charge(conn):
    booked = [book(conn) for _ in range(120)]
    jump_to(conn, max(b["plan"]["charge_at"] for b in booked))

    def run(name):
        with connect() as c:
            while worker.run_once(c, name, batch_size=7):
                pass

    threads = [threading.Thread(target=run, args=(f"w{i}",)) for i in range(8)]
    for t in threads: t.start()
    for t in threads: t.join()

    total = conn.execute("SELECT count(*) AS n, count(DISTINCT plan_id) AS d FROM psp_charges").fetchone()
    assert total == {"n": 120, "d": 120}       # exactly one charge per plan
    assert metrics.status_counts(conn) == {"PAID": 120}
    workers_used = conn.execute("SELECT count(DISTINCT claimed_by) AS n FROM payment_plans").fetchone()["n"]
    assert workers_used > 1                    # the work really was shared


def test_worker_crash_after_charge_is_replayed_not_recharged(conn):
    b = book(conn)
    jump_to(conn, b["plan"]["charge_at"])

    [claimed] = worker.claim_batch(conn, "doomed-worker")
    with pytest.raises(worker.SimulatedCrash):
        worker.charge_plan(conn, claimed, crash_after_charge=True)

    assert len(psp_charges(conn, claimed["id"])) == 1      # money moved...
    assert plan_of(conn, b)["status"] == "IN_PROGRESS"     # ...but we never recorded it
    assert worker.run_once(conn) == 0                      # nobody else can grab it

    clock.advance(conn, IN_PROGRESS_TIMEOUT + timedelta(seconds=1))
    assert sweeper.rescue_stuck_plans(conn) == 1           # janitor puts it back
    assert worker.run_once(conn) == 1                      # same attempt -> same key

    assert plan_of(conn, b)["status"] == "PAID"
    assert len(psp_charges(conn, claimed["id"])) == 1      # STILL one charge


def test_processor_timeout_retries_with_same_key(conn):
    b = book(conn, card="tok_timeout")
    jump_to(conn, b["plan"]["charge_at"])

    worker.run_once(conn)                      # money moves, response lost
    p = plan_of(conn, b)
    assert p["status"] == "SCHEDULED" and p["attempt_count"] == 0   # same attempt number!

    clock.advance(conn, timedelta(minutes=2))
    worker.run_once(conn)                      # same key -> processor replays "succeeded"
    assert plan_of(conn, b)["status"] == "PAID"
    assert len(psp_charges(conn, b["plan"]["id"])) == 1


# ---------------------------------------------------------------- declines
def test_soft_decline_retries_with_backoff_then_succeeds(conn):
    b = book(conn, card="tok_flaky")           # declines twice, then works
    jump_to(conn, b["plan"]["charge_at"])
    for _ in range(3):
        worker.run_once(conn)
        p = plan_of(conn, b)
        if p["status"] == "PAID":
            break
        assert p["status"] == "SCHEDULED"
        jump_to(conn, p["charge_at"])

    p = plan_of(conn, b)
    assert p["status"] == "PAID" and p["attempt_count"] == 2
    keys = [c["idempotency_key"] for c in psp_charges(conn, p["id"])]
    assert len(keys) == 3 and len(set(keys)) == 3  # a NEW key per definite decline


def test_hard_decline_then_guest_updates_card(conn):
    b = book(conn, card="tok_expired")
    jump_to(conn, b["plan"]["charge_at"])
    worker.run_once(conn)
    assert plan_of(conn, b)["status"] == "NEEDS_NEW_CARD"
    relay.drain(conn)
    assert "PaymentNeedsNewCard" in notification_kinds(conn, b)

    plans.update_card(conn, b["reservation"]["id"], "tok_ok")
    worker.run_once(conn)
    assert plan_of(conn, b)["status"] == "PAID"


# ---------------------------------------------------------------- rule 2: paid or released
def test_unpaid_reservation_is_released_at_deadline(conn):
    b = book(conn, card="tok_insufficient_funds")
    jump_to(conn, b["plan"]["charge_at"])
    while plan_of(conn, b)["status"] == "SCHEDULED":
        jump_to(conn, plan_of(conn, b)["charge_at"])
        worker.run_once(conn)
    assert plan_of(conn, b)["status"] == "NEEDS_NEW_CARD"

    jump_to(conn, b["plan"]["deadline_at"])
    assert sweeper.release_overdue_plans(conn) == 1
    relay.drain(conn)
    assert plan_of(conn, b)["status"] == "RELEASED"
    assert reservation_of(conn, b)["status"] == "RELEASED"   # host can rebook
    assert metrics.reconcile(conn) == []


# ---------------------------------------------------------------- races with the guest
def test_cancel_before_charge(conn):
    b = book(conn)
    assert booking.cancel_reservation(conn, b["reservation"]["id"]) == "cancelled"
    jump_to(conn, b["plan"]["charge_at"])
    assert worker.run_once(conn) == 0
    assert psp_charges(conn, b["plan"]["id"]) == []


def test_cancel_during_charge_is_refunded(conn):
    b = book(conn)
    jump_to(conn, b["plan"]["charge_at"])
    [claimed] = worker.claim_batch(conn, "w1")          # worker holds it IN_PROGRESS

    assert booking.cancel_reservation(conn, b["reservation"]["id"]) == "cancel_pending"

    assert worker.charge_plan(conn, claimed) == "REFUND_PENDING"  # version check failed -> saw the flag
    worker.process_refunds(conn)
    assert plan_of(conn, b)["status"] == "REFUNDED"
    relay.drain(conn)
    assert reservation_of(conn, b)["status"] == "CANCELLED"       # late PaymentSucceeded can't resurrect it
    assert metrics.reconcile(conn) == []


def test_cancel_after_payment_is_refunded(conn):
    b = book(conn)
    jump_to(conn, b["plan"]["charge_at"])
    worker.run_once(conn)
    assert booking.cancel_reservation(conn, b["reservation"]["id"]) == "refund_pending"
    worker.run_once(conn)
    assert plan_of(conn, b)["status"] == "REFUNDED"


def test_change_dates_moves_the_charge(conn):
    b = book(conn, days_ahead=30)
    new_check_in = b["reservation"]["check_in"] + timedelta(days=10)
    booking.change_dates(conn, b["reservation"]["id"], new_check_in)
    p = plan_of(conn, b)
    assert p["deadline_at"] - b["plan"]["deadline_at"] == timedelta(days=10)
    assert p["version"] == b["plan"]["version"] + 1


def test_stale_writer_loses_version_check(conn):
    b = book(conn)
    stale = b["plan"]                                          # version 0
    assert plans.cas_update(conn, stale, last_error="first")   # -> version 1
    assert not plans.cas_update(conn, stale, last_error="second")  # still thinks version 0


# ---------------------------------------------------------------- outbox + monitoring
def test_duplicate_event_delivery_has_no_double_effect(conn):
    b = book(conn)
    relay.drain(conn)
    conn.execute("UPDATE outbox SET published_at = NULL")      # simulate relay crash before marking
    relay.drain(conn)                                          # everything delivered AGAIN
    assert notification_kinds(conn, b) == ["ReservationConfirmed"]


def test_backlog_age_and_partial_index(conn):
    b = book(conn)
    jump_to(conn, b["plan"]["charge_at"])
    clock.advance(conn, timedelta(minutes=30))                 # workers "down" for 30 min
    assert metrics.snapshot(conn)["alert"] is True
    worker.run_once(conn)
    assert metrics.backlog_age_seconds(conn) == 0

    conn.execute("SET enable_seqscan = off")                   # tiny table: force planner to show index choice
    plan_text = "\n".join(r["QUERY PLAN"] for r in conn.execute(
        "EXPLAIN " + worker.CLAIM_SQL, (clock.now(conn), 50)))
    assert "idx_plans_due" in plan_text


def test_reconciliation_catches_money_without_a_booking(conn):
    b = book(conn, card="tok_timeout")
    jump_to(conn, b["plan"]["charge_at"])
    worker.run_once(conn)                         # money moved, outcome unknown, plan SCHEDULED
    # Simulate a bad bug: someone releases the plan without checking the processor
    plans.cas_update(conn, plan_of(conn, b), status="RELEASED")
    kinds = [p["kind"] for p in metrics.reconcile(conn)]
    assert kinds == ["charged_but_not_paid"]
