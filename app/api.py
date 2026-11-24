"""HTTP layer (FastAPI). Thin on purpose: it parses requests and calls
booking.py / plans.py. All the interesting logic lives there.

Run:  uvicorn app.api:app --reload      then open http://localhost:8000/docs
"""
from datetime import date, timedelta
from typing import Iterator

import psycopg
from fastapi import Depends, FastAPI, HTTPException
from pydantic import BaseModel, Field

from app import booking, clock, metrics, plans
from app.db import connect

app = FastAPI(title="Reserve Now, Pay Later (mini)")


def get_conn() -> Iterator[psycopg.Connection]:
    with connect() as conn:   # one connection per request (a real app would use a pool)
        yield conn


class BookRequest(BaseModel):
    listing_id: int = 1
    guest_email: str = "guest@example.com"
    check_in: date
    nights: int = Field(2, gt=0)
    card_token: str = "tok_ok"


class ChangeDates(BaseModel):
    check_in: date


class NewCard(BaseModel):
    card_token: str


class Advance(BaseModel):
    days: float = 0
    hours: float = 0
    minutes: float = 0


def _http_errors(fn):
    try:
        return fn()
    except booking.NotEligible as e:
        raise HTTPException(422, f"not eligible for pay later: {e}")
    except (booking.BookingError, plans.PlanConflict) as e:
        raise HTTPException(409, str(e))


@app.post("/reservations")
def create_reservation(body: BookRequest, conn=Depends(get_conn)):
    return _http_errors(lambda: booking.create_reservation(
        conn, body.listing_id, body.guest_email, body.check_in, body.nights, body.card_token))


@app.get("/reservations/{reservation_id}")
def get_reservation(reservation_id: int, conn=Depends(get_conn)):
    res = conn.execute("SELECT * FROM reservations WHERE id = %s", (reservation_id,)).fetchone()
    if res is None:
        raise HTTPException(404, "not found")
    return {
        "reservation": res,
        "plan": conn.execute("SELECT * FROM payment_plans WHERE reservation_id = %s", (reservation_id,)).fetchone(),
        "notifications": conn.execute(
            "SELECT kind, message, created_at FROM notifications WHERE reservation_id = %s ORDER BY id",
            (reservation_id,)).fetchall(),
    }


@app.post("/reservations/{reservation_id}/change-dates")
def change_dates(reservation_id: int, body: ChangeDates, conn=Depends(get_conn)):
    return _http_errors(lambda: booking.change_dates(conn, reservation_id, body.check_in))


@app.post("/reservations/{reservation_id}/cancel")
def cancel(reservation_id: int, conn=Depends(get_conn)):
    return {"result": _http_errors(lambda: booking.cancel_reservation(conn, reservation_id))}


@app.post("/reservations/{reservation_id}/card")
def update_card(reservation_id: int, body: NewCard, conn=Depends(get_conn)):
    return _http_errors(lambda: plans.update_card(conn, reservation_id, body.card_token))


@app.get("/metrics")
def get_metrics(conn=Depends(get_conn)):
    return metrics.snapshot(conn)


# ---- dev-only helpers -------------------------------------------------------
@app.get("/dev/clock")
def get_clock(conn=Depends(get_conn)):
    return {"now": clock.now(conn)}


@app.post("/dev/clock/advance")
def advance_clock(body: Advance, conn=Depends(get_conn)):
    return {"now": clock.advance(conn, timedelta(days=body.days, hours=body.hours, minutes=body.minutes))}
