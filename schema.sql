-- =====================================================================
-- Reserve Now, Pay Later (mini) — the whole database in one file.
--
-- In the real system these tables belong to DIFFERENT services
-- (Reservations, Payment Plans, the payment processor, Notifications).
-- Here they share one Postgres database so you can run everything
-- on a laptop, but each section says who would own it in real life.
-- =====================================================================


-- ---------------------------------------------------------------------
-- Dev clock (learning aid only).
-- Charges happen days or weeks after booking. Instead of waiting, every
-- part of the system asks this table "what time is it?" and the demo
-- and tests move the clock forward. All processes share it because it
-- lives in the database.
-- ---------------------------------------------------------------------
CREATE TABLE dev_clock (
    id              INT PRIMARY KEY DEFAULT 1 CHECK (id = 1),   -- exactly one row
    offset_interval INTERVAL NOT NULL DEFAULT '0'
);
INSERT INTO dev_clock DEFAULT VALUES;


-- ---------------------------------------------------------------------
-- Owned by: Listings / Booking API
-- ---------------------------------------------------------------------
CREATE TABLE listings (
    id                  BIGSERIAL PRIMARY KEY,
    name                TEXT   NOT NULL,
    timezone            TEXT   NOT NULL,          -- IANA name, e.g. 'America/New_York'
    cancellation_policy TEXT   NOT NULL CHECK (cancellation_policy IN ('flexible', 'moderate', 'strict')),
    nightly_price_cents BIGINT NOT NULL CHECK (nightly_price_cents > 0),
    currency            TEXT   NOT NULL           -- ISO 4217, e.g. 'USD'
);


-- ---------------------------------------------------------------------
-- Owned by: Reservations service
-- ---------------------------------------------------------------------
CREATE TABLE reservations (
    id           BIGSERIAL PRIMARY KEY,
    listing_id   BIGINT NOT NULL REFERENCES listings(id),
    guest_email  TEXT   NOT NULL,
    check_in     DATE   NOT NULL,
    nights       INT    NOT NULL CHECK (nights > 0),
    amount_cents BIGINT NOT NULL,
    currency     TEXT   NOT NULL,
    status       TEXT   NOT NULL CHECK (status IN (
                     'CONFIRMED_UNPAID',  -- booked for $0, host calendar blocked
                     'PAID',              -- charge went through
                     'CANCELLED',         -- guest cancelled
                     'RELEASED'           -- never paid by the deadline; host can rebook
                 )),
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);


-- ---------------------------------------------------------------------
-- Owned by: Payment-plan service  (the part "I owned" in the script)
--
-- One row = one promise to charge one reservation later.
-- ---------------------------------------------------------------------
CREATE TABLE payment_plans (
    id               BIGSERIAL PRIMARY KEY,
    reservation_id   BIGINT NOT NULL UNIQUE REFERENCES reservations(id),
    amount_cents     BIGINT NOT NULL,
    currency         TEXT   NOT NULL,
    card_token       TEXT   NOT NULL,     -- a token from the processor's vault, NEVER a card number
    deadline_at      TIMESTAMPTZ NOT NULL, -- free-cancellation deadline (stored in UTC)
    charge_at        TIMESTAMPTZ NOT NULL, -- when a worker should try next (deadline - buffer, or a retry time)
    status           TEXT   NOT NULL CHECK (status IN (
                         'SCHEDULED',       -- waiting for charge_at
                         'IN_PROGRESS',     -- a worker claimed it and is talking to the processor
                         'PAID',            -- done
                         'NEEDS_NEW_CARD',  -- hard decline or out of retries; waiting for the guest
                         'CANCELLED',       -- guest cancelled before any money moved
                         'REFUND_PENDING',  -- money may have moved but the guest cancelled; refund it
                         'REFUNDED',        -- refund done
                         'RELEASED'         -- deadline passed unpaid; reservation released
                     )),
    version          INT    NOT NULL DEFAULT 0,      -- optimistic locking: every write bumps it
    attempt_count    INT    NOT NULL DEFAULT 0,      -- part of the idempotency key
    cancel_requested BOOLEAN NOT NULL DEFAULT false, -- guest cancelled while a charge was in flight
    psp_charge_id    TEXT,                           -- processor's id once paid
    last_error       TEXT,
    claimed_by       TEXT,                           -- which worker holds it (debugging)
    claimed_at       TIMESTAMPTZ,                    -- when it was claimed (the sweeper uses this)
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- THE index. The workers' claim query only ever looks at SCHEDULED rows
-- ordered by charge_at. PAID / CANCELLED / RELEASED rows pile up forever
-- but never enter this index, so it stays small and fast.
CREATE INDEX idx_plans_due ON payment_plans (charge_at) WHERE status = 'SCHEDULED';

-- Small helper indexes for the sweeper.
CREATE INDEX idx_plans_in_progress ON payment_plans (claimed_at) WHERE status = 'IN_PROGRESS';
CREATE INDEX idx_plans_unpaid_deadline ON payment_plans (deadline_at) WHERE status IN ('SCHEDULED', 'NEEDS_NEW_CARD');


-- ---------------------------------------------------------------------
-- Owned by: Payment-plan service. The transactional outbox.
-- An event is written here IN THE SAME TRANSACTION as the state change,
-- and a relay later publishes it (to Kafka in real life).
-- ---------------------------------------------------------------------
CREATE TABLE outbox (
    id           BIGSERIAL PRIMARY KEY,      -- doubles as the event id
    event_type   TEXT  NOT NULL,
    payload      JSONB NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ                 -- NULL = not yet published
);
CREATE INDEX idx_outbox_unpublished ON outbox (id) WHERE published_at IS NULL;


-- ---------------------------------------------------------------------
-- Owned by: each CONSUMER. Remembers which events it already handled,
-- because the relay delivers "at least once" (duplicates are possible).
-- ---------------------------------------------------------------------
CREATE TABLE processed_events (
    consumer   TEXT   NOT NULL,
    event_id   BIGINT NOT NULL,
    PRIMARY KEY (consumer, event_id)
);


-- ---------------------------------------------------------------------
-- Owned by: Notifications service
-- ---------------------------------------------------------------------
CREATE TABLE notifications (
    id             BIGSERIAL PRIMARY KEY,
    reservation_id BIGINT NOT NULL,
    kind           TEXT   NOT NULL,
    message        TEXT   NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);


-- ---------------------------------------------------------------------
-- Owned by: THE PAYMENT PROCESSOR (Stripe/Adyen + Airbnb's pay-in platform).
-- In real life this is someone else's database. Our fake processor
-- writes here with its OWN connection, so our transactions can never
-- roll it back — exactly like a real external API.
-- ---------------------------------------------------------------------
CREATE TABLE psp_charges (
    idempotency_key TEXT PRIMARY KEY,   -- the processor remembers every key it has seen
    plan_id         BIGINT NOT NULL,    -- "metadata" we attach, used for reconciliation
    card_token      TEXT   NOT NULL,
    amount_cents    BIGINT NOT NULL,
    currency        TEXT   NOT NULL,
    outcome         TEXT   NOT NULL CHECK (outcome IN ('succeeded', 'soft_decline', 'hard_decline')),
    decline_code    TEXT,
    charge_id       TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE psp_refunds (
    refund_key  TEXT PRIMARY KEY,       -- idempotency key for the refund
    charge_key  TEXT NOT NULL,          -- which charge to refund
    status      TEXT NOT NULL CHECK (status IN ('refunded', 'nothing_to_refund')),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
