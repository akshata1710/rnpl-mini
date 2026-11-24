"""THE helper that turns a check-in date into deadlines.

A classic scheduling bug: one reminder path used the server's time
zone instead of the listing's, and guests got reminders at 3 am. The fix
and the lesson: exactly ONE function turns a deadline into a time, and it
always uses the LISTING's time zone. Everything is stored in UTC.
"""
import random
from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

from app.config import CHARGE_BUFFER, CHECK_IN_HOUR, FREE_CANCEL_DAYS, JITTER_SECONDS


def cancellation_deadline(check_in: date, listing_tz: str, policy: str) -> datetime:
    """Free cancellation ends N days before check-in, at check-in hour, LOCAL time.

    Example: check-in 2026-12-20 at a Tokyo listing, flexible (1 day):
      local deadline = 2026-12-19 15:00 Asia/Tokyo
      returned       = 2026-12-19 06:00 UTC
    """
    local_deadline = datetime.combine(
        check_in - timedelta(days=FREE_CANCEL_DAYS[policy]),
        time(CHECK_IN_HOUR),
        tzinfo=ZoneInfo(listing_tz),   # <- the listing's zone, never the server's
    )
    return local_deadline.astimezone(timezone.utc)


def first_charge_at(deadline_utc: datetime) -> datetime:
    """When to make the first attempt: deadline minus the buffer, minus jitter.

    Jitter only moves the charge EARLIER, so we never eat into the buffer.
    """
    jitter = timedelta(seconds=random.randint(0, JITTER_SECONDS))
    return deadline_utc - CHARGE_BUFFER - jitter


def to_local(ts_utc: datetime, listing_tz: str) -> datetime:
    """For display / notifications only."""
    return ts_utc.astimezone(ZoneInfo(listing_tz))
