"""Downstream services that react to payment events.

  Reservations consumer: PaymentSucceeded -> reservation PAID
                         PlanReleased     -> reservation RELEASED (host can rebook)
  Notifications consumer: receipts, "update your card", "we'll retry", ...

Each consumer de-duplicates with processed_events. The dedupe INSERT and
the actual effect happen in ONE transaction, so a duplicate delivery is a
no-op. That's how "at least once delivery" becomes "exactly once effect".
"""
import logging
from datetime import datetime

from app import broker, timeutil
from app.db import connect

log = logging.getLogger("consumers")


def _first_time(conn, consumer: str, event_id: int) -> bool:
    cur = conn.execute(
        "INSERT INTO processed_events (consumer, event_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
        (consumer, event_id),
    )
    return cur.rowcount == 1


def reservations_consumer(event: dict) -> None:
    p = event["payload"]
    with connect() as conn, conn.transaction():
        if not _first_time(conn, "reservations", event["id"]):
            return
        if event["event_type"] == "PaymentSucceeded":
            # "AND status = 'CONFIRMED_UNPAID'" so a late event can't
            # resurrect a reservation the guest already cancelled.
            conn.execute(
                "UPDATE reservations SET status = 'PAID' WHERE id = %s AND status = 'CONFIRMED_UNPAID'",
                (p["reservation_id"],),
            )
        elif event["event_type"] == "PlanReleased":
            conn.execute(
                "UPDATE reservations SET status = 'RELEASED' WHERE id = %s AND status = 'CONFIRMED_UNPAID'",
                (p["reservation_id"],),
            )
            log.info("reservation %s released: host calendar is open again", p["reservation_id"])


# Events carry times in UTC (ISO strings). `t` turns one into the LISTING's
# local time, via the single helper in timeutil. Never the server's zone.
MESSAGES = {
    "ReservationConfirmed":  lambda p, t: f"You're booked! $0 today. We'll charge {p['amount_cents']/100:.2f} {p['currency']} around {p['charge_at']}.",
    "PaymentSucceeded":      lambda p, t: f"Receipt: we charged {p['amount_cents']/100:.2f} {p['currency']}.",
    "PaymentRetryScheduled": lambda p, t: f"Your payment didn't go through ({p['reason']}). We'll try again on {t(p['retry_at'])}.",
    "PaymentNeedsNewCard":   lambda p, t: f"Please update your card ({p['reason']}) before {t(p['deadline_at'])} or the reservation will be released.",
    "PlanReleased":          lambda p, t: "We couldn't collect payment before the deadline, so your reservation was released.",
    "PaymentRefunded":       lambda p, t: f"Refunded {p['amount_cents']/100:.2f} {p['currency']}.",
    "ReservationCancelled":  lambda p, t: "Your reservation is cancelled.",
}


def notifications_consumer(event: dict) -> None:
    render = MESSAGES.get(event["event_type"])
    if render is None:
        return
    p = event["payload"]
    with connect() as conn, conn.transaction():
        if not _first_time(conn, "notifications", event["id"]):
            return
        tz = conn.execute(
            """SELECT l.timezone FROM reservations r JOIN listings l ON l.id = r.listing_id
                WHERE r.id = %s""", (p["reservation_id"],)).fetchone()["timezone"]

        def local(iso_utc: str) -> str:
            return timeutil.to_local(datetime.fromisoformat(iso_utc), tz).strftime("%b %d, %H:%M %Z")

        msg = render(p, local)
        conn.execute(
            "INSERT INTO notifications (reservation_id, kind, message) VALUES (%s, %s, %s)",
            (p["reservation_id"], event["event_type"], msg),
        )
        log.info("[email to guest of reservation %s] %s", p["reservation_id"], msg)


def register() -> None:
    broker.subscribe("PaymentSucceeded", reservations_consumer)
    broker.subscribe("PlanReleased", reservations_consumer)
    broker.subscribe("*", notifications_consumer)
