"""Monitoring: "how do you know you're not falling behind?"

The answer: ONE number, backlog age. Plus a reconciliation
check that compares our plans with the processor's records.

Note how both are plain SQL. That's a big payoff of keeping the schedule
in a table instead of inside a queue: "what is due and unpaid right now?"
is just a query.
"""
import psycopg

from app import clock
from app.config import BACKLOG_ALERT_SECONDS


def backlog_age_seconds(conn: psycopg.Connection) -> float:
    """How long has the OLDEST due-but-unattempted plan been waiting?
    0 = keeping up. Climbing = workers are behind (or dead). Page on it.
    Better than 'queue length': 10,000 plans due this second is fine;
    one plan waiting 2 hours is not."""
    now = clock.now(conn)
    row = conn.execute(
        "SELECT min(charge_at) AS oldest FROM payment_plans WHERE status = 'SCHEDULED' AND charge_at <= %s",
        (now,),
    ).fetchone()
    return 0.0 if row["oldest"] is None else (now - row["oldest"]).total_seconds()


def status_counts(conn) -> dict:
    rows = conn.execute("SELECT status, count(*) AS n FROM payment_plans GROUP BY status").fetchall()
    return {r["status"]: r["n"] for r in rows}


def reconcile(conn) -> list[dict]:
    """Compare our view with the processor's. Every row returned is a bug or
    an incident. In production this runs daily against the payments ledger."""
    problems = []

    # A) The processor took money, but our plan doesn't say PAID / refunding.
    #    (e.g. a charge whose outcome was lost and the plan got released.)
    for r in conn.execute(
        """
        SELECT c.idempotency_key, c.plan_id, p.status
          FROM psp_charges c JOIN payment_plans p ON p.id = c.plan_id
         WHERE c.outcome = 'succeeded'
           AND p.status NOT IN ('PAID', 'REFUND_PENDING', 'REFUNDED', 'IN_PROGRESS', 'SCHEDULED')
           AND NOT EXISTS (SELECT 1 FROM psp_refunds r
                            WHERE r.charge_key = c.idempotency_key AND r.status = 'refunded')
        """
    ).fetchall():
        problems.append({"kind": "charged_but_not_paid", **r})

    # B) We say PAID, but the processor has no successful charge for that plan.
    for r in conn.execute(
        """
        SELECT p.id AS plan_id FROM payment_plans p
         WHERE p.status = 'PAID'
           AND NOT EXISTS (SELECT 1 FROM psp_charges c WHERE c.plan_id = p.id AND c.outcome = 'succeeded')
        """
    ).fetchall():
        problems.append({"kind": "paid_without_charge", **r})

    # C) More than one successful charge for one plan. Rule #1 broken.
    for r in conn.execute(
        """
        SELECT plan_id, count(*) AS charges FROM psp_charges
         WHERE outcome = 'succeeded' GROUP BY plan_id HAVING count(*) > 1
        """
    ).fetchall():
        problems.append({"kind": "double_charge", **r})

    # D) Past deadline and still unpaid. Rule #2 broken (sweeper behind or dead).
    for r in conn.execute(
        """
        SELECT id AS plan_id, deadline_at FROM payment_plans
         WHERE status IN ('SCHEDULED', 'NEEDS_NEW_CARD') AND deadline_at <= %s
        """,
        (clock.now(conn),),
    ).fetchall():
        problems.append({"kind": "unpaid_past_deadline", **r})

    return problems


def snapshot(conn) -> dict:
    age = backlog_age_seconds(conn)
    return {
        "backlog_age_seconds": round(age, 1),
        "alert": age > BACKLOG_ALERT_SECONDS,
        "plans_by_status": status_counts(conn),
        "reconciliation_problems": reconcile(conn),
    }
