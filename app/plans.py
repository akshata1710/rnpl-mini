"""Payment-plan service (Step 4 in the architecture).

Owns the payment_plans table. Everyone else (Booking API, workers,
sweeper) changes a plan only through the functions here, and every change
goes through `cas_update`, the optimistic-locking helper.

State machine (see GUIDE.md for the diagram):

  SCHEDULED --claim--> IN_PROGRESS --success--> PAID
      ^                    |  |--soft decline (retries left)--> SCHEDULED (later)
      |                    |  |--hard decline / out of retries--> NEEDS_NEW_CARD
      |                    |  `--timeout (unknown)--> SCHEDULED (soon, SAME key)
      |                    `--guest cancelled meanwhile--> REFUND_PENDING / CANCELLED
  NEEDS_NEW_CARD --guest updates card--> SCHEDULED
  SCHEDULED / NEEDS_NEW_CARD --deadline passes--> RELEASED
  SCHEDULED / NEEDS_NEW_CARD --guest cancels--> CANCELLED
  PAID --guest cancels--> REFUND_PENDING --refund--> REFUNDED
"""
from datetime import datetime

import psycopg

from app import clock, outbox, timeutil


class PlanConflict(Exception):
    """The plan isn't in a state that allows this change (or kept changing under us)."""


# ---------------------------------------------------------------------------
# The single most important helper in the project.
# ---------------------------------------------------------------------------
def cas_update(conn: psycopg.Connection, plan: dict, **changes) -> bool:
    """Compare-and-set: apply `changes` ONLY if the row's version is still the
    version we read. Returns True if we won, False if someone changed it first.

        UPDATE payment_plans SET ..., version = version + 1
        WHERE id = 42 AND version = 7

    If another writer committed in between, the version is now 8, the WHERE
    matches zero rows, and we know to re-read and decide again. No row is
    locked while we "think", which is why this is called OPTIMISTIC locking.

    (Column names come from our own code, never from users, so building the
    SET clause with an f-string is safe here.)
    """
    set_clause = ", ".join(f"{col} = %({col})s" for col in changes)
    cur = conn.execute(
        f"""
        UPDATE payment_plans
           SET {set_clause}, version = version + 1, updated_at = now()
         WHERE id = %(_id)s AND version = %(_version)s
        """,
        {**changes, "_id": plan["id"], "_version": plan["version"]},
    )
    return cur.rowcount == 1


def get_plan(conn: psycopg.Connection, plan_id: int) -> dict:
    return conn.execute("SELECT * FROM payment_plans WHERE id = %s", (plan_id,)).fetchone()


def get_plan_for_reservation(conn: psycopg.Connection, reservation_id: int) -> dict:
    plan = conn.execute(
        "SELECT * FROM payment_plans WHERE reservation_id = %s", (reservation_id,)
    ).fetchone()
    if plan is None:
        raise PlanConflict(f"no plan for reservation {reservation_id}")
    return plan


# ---------------------------------------------------------------------------
# Called by the Booking API
# ---------------------------------------------------------------------------
def create_plan(conn, reservation: dict, card_token: str, deadline_at: datetime) -> dict:
    """Create the plan. Call INSIDE the booking transaction: if this fails,
    the reservation insert rolls back too ("no plan, no booking")."""
    return conn.execute(
        """
        INSERT INTO payment_plans
            (reservation_id, amount_cents, currency, card_token, deadline_at, charge_at, status)
        VALUES (%s, %s, %s, %s, %s, %s, 'SCHEDULED')
        RETURNING *
        """,
        (
            reservation["id"],
            reservation["amount_cents"],
            reservation["currency"],
            card_token,
            deadline_at,
            timeutil.first_charge_at(deadline_at),
        ),
    ).fetchone()


def reschedule(conn, reservation_id: int, new_deadline: datetime) -> dict:
    """Guest changed dates -> the deadline moved -> move the charge.
    With a table this is one UPDATE. With a delayed queue it would be
    'find the message, delete it, enqueue a new one' (painful)."""
    for _ in range(3):  # retry a few times if we lose the version race
        plan = get_plan_for_reservation(conn, reservation_id)
        if plan["status"] not in ("SCHEDULED", "NEEDS_NEW_CARD"):
            raise PlanConflict(f"cannot change dates while plan is {plan['status']}")
        if cas_update(
            conn, plan,
            deadline_at=new_deadline,
            charge_at=timeutil.first_charge_at(new_deadline),
        ):
            return get_plan(conn, plan["id"])
    raise PlanConflict("plan kept changing; try again")


def cancel_plan(conn, reservation_id: int) -> str:
    """Guest cancelled. What we do depends on where the money is."""
    for _ in range(5):
        plan = get_plan_for_reservation(conn, reservation_id)
        status = plan["status"]

        if status in ("SCHEDULED", "NEEDS_NEW_CARD"):
            # No money has moved. Just stop the charge.
            if cas_update(conn, plan, status="CANCELLED"):
                return "cancelled"

        elif status == "IN_PROGRESS":
            # A worker is talking to the processor RIGHT NOW. We can't know
            # yet whether the card gets charged, so we leave a flag. The
            # worker's final cas_update will fail (we bumped the version),
            # it re-reads, sees the flag, and routes to refund/cancel.
            if cas_update(conn, plan, cancel_requested=True):
                return "cancel_pending"

        elif status == "PAID":
            # Money moved. Refunds are processed by the worker loop
            # (worker.process_refunds), never inside this transaction.
            if cas_update(conn, plan, status="REFUND_PENDING"):
                outbox.emit(conn, "PaymentRefundScheduled",
                            {"reservation_id": reservation_id, "plan_id": plan["id"]})
                return "refund_pending"
        else:
            raise PlanConflict(f"cannot cancel a plan that is {status}")
        # cas_update lost a race -> loop, re-read, decide again
    raise PlanConflict("plan kept changing; try again")


def update_card(conn, reservation_id: int, new_card_token: str) -> dict:
    """Guest fixed their card after a decline.

    The next attempt needs a NEW idempotency key: the old key is permanently
    tied to the old declined result at the processor. The worker already
    bumped attempt_count when it recorded the decline, so the next key is new.

    We only allow this from NEEDS_NEW_CARD, a state reached only after a
    DEFINITE decline. If we allowed it while an attempt's outcome was unknown
    (timeout), a new key could charge a second time. See GUIDE.md deep dive.
    """
    with conn.transaction():
        plan = get_plan_for_reservation(conn, reservation_id)
        if plan["status"] != "NEEDS_NEW_CARD":
            raise PlanConflict(f"card can only be updated after a decline (plan is {plan['status']})")
        ok = cas_update(
            conn, plan,
            card_token=new_card_token,
            status="SCHEDULED",
            charge_at=clock.now(conn),  # try right away
            last_error=None,
        )
        if not ok:
            raise PlanConflict("plan changed; try again")
        return get_plan(conn, plan["id"])
