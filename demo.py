"""A narrated tour. Run:  python demo.py
Resets the database, then plays four stories, printing what happens.
"""
import logging
from datetime import timedelta

from app import booking, clock, metrics, plans, relay, sweeper, worker
from app.config import IN_PROGRESS_TIMEOUT
from app.db import connect, init_db

logging.basicConfig(level=logging.WARNING)


def show(conn, title, rid):
    r = conn.execute("SELECT status FROM reservations WHERE id=%s", (rid,)).fetchone()
    p = conn.execute("SELECT * FROM payment_plans WHERE reservation_id=%s", (rid,)).fetchone()
    n = conn.execute("SELECT count(*) AS n FROM psp_charges WHERE plan_id=%s AND outcome='succeeded'",
                     (p["id"],)).fetchone()["n"]
    print(f"  {title:<38} reservation={r['status']:<17} plan={p['status']:<15} "
          f"v{p['version']} attempt={p['attempt_count']} successful_charges={n}")


def jump_to(conn, ts):
    clock.advance(conn, ts - clock.now(conn) + timedelta(seconds=1))


def book(conn, card, listing_id=1):
    check_in = clock.now(conn).date() + timedelta(days=30)
    b = booking.create_reservation(conn, listing_id, "guest@example.com", check_in, 2, card)
    return b["reservation"]["id"], b["plan"]


def main():
    init_db()
    with connect() as conn:
        print("\n=== 1. Happy path: book for $0, charge later, exactly once ===")
        rid, plan = book(conn, "tok_ok")
        print(f"  deadline (UTC): {plan['deadline_at']:%Y-%m-%d %H:%M}   charge_at: {plan['charge_at']:%Y-%m-%d %H:%M}")
        show(conn, "after booking", rid)
        worker.run_once(conn); show(conn, "worker runs today (not due)", rid)
        jump_to(conn, plan["charge_at"])
        worker.run_once(conn); show(conn, "clock -> charge_at, worker runs", rid)
        relay.drain(conn); show(conn, "relay publishes outbox", rid)

        print("\n=== 2. Worker dies right after the card was charged ===")
        rid, plan = book(conn, "tok_ok")
        jump_to(conn, plan["charge_at"])
        [claimed] = worker.claim_batch(conn, "doomed")
        try:
            worker.charge_plan(conn, claimed, crash_after_charge=True)
        except worker.SimulatedCrash:
            pass
        show(conn, "crashed after processor charged", rid)
        clock.advance(conn, IN_PROGRESS_TIMEOUT + timedelta(seconds=1))
        sweeper.rescue_stuck_plans(conn); show(conn, "sweeper rescues it", rid)
        worker.run_once(conn); show(conn, "retry: same key, replayed result", rid)

        print("\n=== 3. Guest cancels while the charge is in flight ===")
        rid, plan = book(conn, "tok_ok")
        jump_to(conn, plan["charge_at"])
        [claimed] = worker.claim_batch(conn, "w1"); show(conn, "worker claimed it", rid)
        print("  guest cancels ->", booking.cancel_reservation(conn, rid))
        worker.charge_plan(conn, claimed); show(conn, "charge finishes, version check fails", rid)
        worker.process_refunds(conn); show(conn, "refund processed", rid)

        print("\n=== 4. Card keeps declining -> released at the deadline ===")
        rid, plan = book(conn, "tok_insufficient_funds")
        while plans.get_plan(conn, plan["id"])["status"] == "SCHEDULED":
            jump_to(conn, plans.get_plan(conn, plan["id"])["charge_at"])
            worker.run_once(conn); show(conn, "attempt", rid)
        jump_to(conn, plan["deadline_at"])
        sweeper.release_overdue_plans(conn); relay.drain(conn); show(conn, "deadline passes, sweeper releases", rid)

        print("\n  Guest's inbox for that last one:")
        for n in conn.execute("SELECT message FROM notifications WHERE reservation_id=%s ORDER BY id", (rid,)):
            print("   -", n["message"])

        print("\n=== Metrics ===")
        snap = metrics.snapshot(conn)
        print("  backlog age:", snap["backlog_age_seconds"], "s | by status:", snap["plans_by_status"])
        print("  reconciliation problems:", snap["reconciliation_problems"] or "none")


if __name__ == "__main__":
    main()
