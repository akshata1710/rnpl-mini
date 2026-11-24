"""Every tunable number in one place, so you can see the knobs."""
import os
from datetime import timedelta

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://rnpl:rnpl@localhost:5432/rnpl")

# --- Eligibility -------------------------------------------------------
# Free cancellation ends this many days before check-in (at CHECK_IN_HOUR local time).
# Strict listings are not eligible: the host has no time to rebook.
FREE_CANCEL_DAYS = {"flexible": 1, "moderate": 5}
CHECK_IN_HOUR = 15  # 3 pm in the LISTING's time zone

SUPPORTED_CURRENCIES = {"USD", "EUR", "GBP", "CAD", "AUD", "JPY", "MXN"}
# Excluded at launch in the real rollout (different local payment rules).
EXCLUDED_CURRENCIES = {"BRL", "INR", "TRY"}

# --- Scheduling --------------------------------------------------------
# Charge this long BEFORE the free-cancellation deadline. The buffer is
# the time we have for retries and for the guest to fix their card.
CHARGE_BUFFER = timedelta(days=2)

# Spread charges out so midnight / holidays don't all land in one second.
JITTER_SECONDS = 600

# --- Workers -----------------------------------------------------------
BATCH_SIZE = 50          # plans claimed per query
POLL_SECONDS = 2         # sleep when there is nothing due

# Soft declines (insufficient funds, "try again later"): retry with backoff.
# 6h + 12h + 24h = 42h, which fits inside the 48h buffer. That is not an
# accident: the buffer is sized to hold the retry schedule.
RETRY_BACKOFF = [timedelta(hours=6), timedelta(hours=12), timedelta(hours=24)]
MAX_SOFT_RETRIES = len(RETRY_BACKOFF)

# Processor timed out: we DON'T KNOW if money moved. Retry soon with the SAME key.
UNKNOWN_RETRY_DELAY = timedelta(minutes=1)

# --- Sweeper -----------------------------------------------------------
# A plan stuck IN_PROGRESS longer than this means its worker died.
IN_PROGRESS_TIMEOUT = timedelta(minutes=5)

# --- Alerting ----------------------------------------------------------
# Page someone if the oldest due-but-unattempted plan waits longer than this.
BACKLOG_ALERT_SECONDS = 600
