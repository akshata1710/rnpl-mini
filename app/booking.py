"""Booking API + Reservations (Steps 1-3 on the slide).

  1. Checkout asks: is this stay eligible for $0 today?  (server-side check)
  2. Guest confirms.
  3. Create the reservation AND its payment plan in one transaction.
     No plan, no booking: we fail closed.
"""
from datetime import date, datetime

import psycopg

from app import clock, outbox, plans, timeutil
from app.config import CHARGE_BUFFER, EXCLUDED_CURRENCIES, FREE_CANCEL_DAYS, SUPPORTED_CURRENCIES


class NotEligible(Exception):
    pass


class BookingError(Exception):
    pass


def get_listing(conn, listing_id: int) -> dict:
    listing = conn.execute("SELECT * FROM listings WHERE id = %s", (listing_id,)).fetchone()
    if listing is None:
        raise BookingError(f"listing {listing_id} not found")
    return listing


def check_eligibility(conn, listing: dict, check_in: date) -> datetime:
    """Server-side, so a market can be switched off without an app release.
    Returns the free-cancellation deadline if eligible."""
    if listing["cancellation_policy"] not in FREE_CANCEL_DAYS:
        raise NotEligible(f"{listing['cancellation_policy']} cancellation policy is not eligible")
    if listing["currency"] in EXCLUDED_CURRENCIES or listing["currency"] not in SUPPORTED_CURRENCIES:
        raise NotEligible(f"currency {listing['currency']} not supported yet")

    deadline = timeutil.cancellation_deadline(check_in, listing["timezone"], listing["cancellation_policy"])
    if deadline - CHARGE_BUFFER <= clock.now(conn):
        raise NotEligible("too close to the cancellation deadline; pay in full now")
    return deadline


def create_reservation(conn: psycopg.Connection, listing_id: int, guest_email: str,
                       check_in: date, nights: int, card_token: str) -> dict:
    listing = get_listing(conn, listing_id)
    deadline = check_eligibility(conn, listing, check_in)
    amount = listing["nightly_price_cents"] * nights

    with conn.transaction():
        # (A real system would also check the calendar for overlapping stays here.)
        reservation = conn.execute(
            """
            INSERT INTO reservations (listing_id, guest_email, check_in, nights, amount_cents, currency, status)
            VALUES (%s, %s, %s, %s, %s, %s, 'CONFIRMED_UNPAID')
            RETURNING *
            """,
            (listing_id, guest_email, check_in, nights, amount, listing["currency"]),
        ).fetchone()

        # If this raises, the INSERT above is rolled back. There is never a
        # confirmed, unpaid reservation that nothing is going to charge.
        plan = plans.create_plan(conn, reservation, card_token, deadline)

        outbox.emit(conn, "ReservationConfirmed", {
            "reservation_id": reservation["id"],
            "amount_cents": amount,
            "currency": listing["currency"],
            "charge_at": timeutil.to_local(plan["charge_at"], listing["timezone"]).strftime("%b %d, %H:%M %Z"),
        })
    return {"reservation": reservation, "plan": plan}


def change_dates(conn: psycopg.Connection, reservation_id: int, new_check_in: date) -> dict:
    with conn.transaction():
        res = conn.execute(
            "SELECT * FROM reservations WHERE id = %s FOR UPDATE", (reservation_id,)
        ).fetchone()
        if res is None or res["status"] != "CONFIRMED_UNPAID":
            raise BookingError("only unpaid, confirmed reservations can change dates here")
        listing = get_listing(conn, res["listing_id"])
        new_deadline = check_eligibility(conn, listing, new_check_in)
        conn.execute("UPDATE reservations SET check_in = %s WHERE id = %s", (new_check_in, reservation_id))
        plan = plans.reschedule(conn, reservation_id, new_deadline)
    return {"reservation_id": reservation_id, "plan": plan}


def cancel_reservation(conn: psycopg.Connection, reservation_id: int) -> str:
    with conn.transaction():
        res = conn.execute(
            "SELECT * FROM reservations WHERE id = %s FOR UPDATE", (reservation_id,)
        ).fetchone()
        if res is None or res["status"] not in ("CONFIRMED_UNPAID", "PAID"):
            raise BookingError("reservation can't be cancelled")
        conn.execute("UPDATE reservations SET status = 'CANCELLED' WHERE id = %s", (reservation_id,))
        what_happened = plans.cancel_plan(conn, reservation_id)
        outbox.emit(conn, "ReservationCancelled", {"reservation_id": reservation_id, "plan": what_happened})
    return what_happened
