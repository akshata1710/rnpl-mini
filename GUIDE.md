# The rnpl-mini guide

This guide goes in order:

1. [What this models](#1-what-this-models)
2. [Dependencies: what each one is and why it's here](#2-dependencies)
3. [The architecture and why it flows this way](#3-the-architecture-and-why-it-flows-this-way)
4. [File by file: what each piece of code does](#4-file-by-file)
5. [The life of one booking, traced through the code](#5-the-life-of-one-booking)
6. [The plan state machine](#6-the-plan-state-machine)
7. [Where every transaction starts and ends](#7-transaction-map)
8. [Deep dives](#8-deep-dives)
9. [What this mini version leaves out on purpose](#9-what-this-leaves-out)
10. [Exercises](#10-exercises)
11. [Design question → file to point at](#11-design-question--file-to-point-at)

---

## 1. What this models

The design in one sentence:

> A payment-plan service records when each reservation must be charged. A pool of workers picks up plans as they come due and charges them through the pay-in platform with idempotency keys. Then events move the reservation to paid, or to retry, remind and finally release.

Each component of a production design, and where it lives here:

| Component | In production | Here |
|---|---|---|
| 1–2 Checkout + Booking API | separate service | `app/booking.py` (`check_eligibility`) |
| 3 Reservations | separate service + DB | `reservations` table, `booking.py`, `consumers.reservations_consumer` |
| 4 Payment-plan service | Python/FastAPI + Postgres | `app/plans.py` + `payment_plans` table |
| 5 Charge workers | pods on Kubernetes | `app/worker.py` (threads or processes) |
| 6 Pay-in platform, PSP, vault | pay-in platform + a PSP such as Stripe or Adyen | `app/fake_psp.py` + `psp_charges` table |
| Redis lock + dedupe | Redis | idempotency key primary key in `psp_charges` |
| Outbox + Kafka | outbox table + Kafka | `outbox` table, `app/relay.py`, `app/broker.py` (in-memory) |
| Notifications | separate service | `consumers.notifications_consumer` + `notifications` table |
| Sweeper for stuck plans | cron or worker | `app/sweeper.py` |
| Backlog-age alert, reconciliation | metrics + daily job | `app/metrics.py` |

The two correctness rules are what the tests check:

1. **Never charge a guest twice.** Covered by `test_many_workers_never_double_charge`, `test_worker_crash_after_charge_is_replayed_not_recharged` and `test_processor_timeout_retries_with_same_key`.
2. **Never leave a reservation unpaid past its deadline.** Covered by `test_unpaid_reservation_is_released_at_deadline` and the `unpaid_past_deadline` reconciliation check.

---

## 2. Dependencies

| Dependency | What it is | Why it's here | What you'd learn by reading its docs |
|---|---|---|---|
| **PostgreSQL 16** | Relational database | Stores the schedule. Every hard problem here (claiming work, races, events) is solved with a Postgres feature. | `SELECT ... FOR UPDATE SKIP LOCKED`, partial indexes, READ COMMITTED isolation, `ON CONFLICT` |
| **psycopg 3** (`psycopg[binary]`) | Python driver for Postgres | Sends SQL and gets rows back. We use raw SQL instead of an ORM so you can see exactly what hits the database. | `conn.transaction()`, `autocommit`, `dict_row` |
| **FastAPI** | Web framework | The HTTP layer (`app/api.py`). Thin on purpose. | Dependencies (`Depends`), request models |
| **Pydantic** (comes with FastAPI) | Data validation | Validates request bodies (`BookRequest`, etc.) | `BaseModel`, `Field` |
| **uvicorn** | ASGI server | Runs the FastAPI app | – |
| **pytest** | Test runner | The failure-injection tests | fixtures, `monkeypatch` |
| `zoneinfo` (standard library) | Time-zone database | Listing-local deadlines, DST | `ZoneInfo`, `astimezone` |
| Docker Compose | Runs Postgres locally | One command to get a database | – |

**Left out on purpose**, and what stands in for each:

- **Redis.** In the real system it holds a short lock and a dedupe record per idempotency key. Here the processor's `psp_charges.idempotency_key` primary key plus `INSERT ... ON CONFLICT DO NOTHING` does the same job.
- **Kafka.** Replaced by `broker.py`, an in-memory publish/subscribe of about 15 lines. The outbox pattern is unchanged; only the transport is simpler.
- **Kubernetes.** Replaced by "start more processes" (`python -m app.cli worker` in several terminals) or threads (`app.cli all`). SKIP LOCKED is what makes that safe either way.
- **A real payment processor.** Replaced by `fake_psp.py`, which still behaves like an external system: it commits on its own connection, and your rollbacks can't undo it.

---

## 3. The architecture and why it flows this way

```
                 (1) eligible?          (3) one transaction:
 Guest ──► Booking API ───────────►  INSERT reservation  ─┐
           booking.py                INSERT payment_plan  ├─ fail closed
                                     INSERT outbox event ─┘
                                            │
                                            ▼
                              payment_plans  (status=SCHEDULED, charge_at=T)
                                            │
          (5) every 2s: claim due rows ◄────┘
     ┌──────────── worker.py ───────────────────────────────────────────┐
     │ tx A: SELECT ... FOR UPDATE SKIP LOCKED; UPDATE → IN_PROGRESS     │
     │ (no tx) fake_psp.charge(key = plan-<id>-attempt-<n>)  ──► (6) PSP │
     │ tx B: cas_update(plan, outcome) + INSERT outbox event             │
     └──────────────────────────────────────────────────────────────────┘
                                            │
                    relay.py: outbox ──► broker.py ──► consumers.py
                                                        ├─ reservations: PAID / RELEASED
                                                        └─ notifications: receipt, "update card", ...
     sweeper.py: IN_PROGRESS too long → SCHEDULED      deadline passed & unpaid → RELEASED
     metrics.py: backlog age, reconciliation (our plans vs processor's charges)
```

### Why each arrow is the way it is

**Why is the schedule a table, not a delayed queue?** Plans change after they're created. Guests change dates (`plans.reschedule`, one UPDATE), cancel (`plans.cancel_plan`) and fix cards (`plans.update_card`). A message already sitting in a queue is hard to find and edit. Charges are also weeks out, and queues like SQS cap delays at 15 minutes. A table also gives you auditing for free: "what's due and unpaid?" is a single query (`metrics.py`).

**Why are reservation and plan created in one transaction?** If they were separate, a crash between them could leave a confirmed reservation that nothing will ever charge, and the host loses a night. One transaction means both exist or neither does. In the real system they're two services, so "fail closed" means the Booking API calls the plan service synchronously and aborts the booking if that call fails.

**Why does the worker commit before calling the processor?** A network call can take seconds. Keeping the transaction open would hold row locks and a database connection for all that time. Instead the worker marks rows `IN_PROGRESS` and commits. The status itself now says "taken", so the lock isn't needed anymore.

**Why record the outcome and the event together?** If you update the status and then publish to Kafka separately, a crash in between loses the event: the plan says PAID, but the reservation never hears about it. The outbox makes the event part of the same commit.

**Why are downstream services consumers instead of being called directly?** If the worker called Notifications directly, a slow Notifications service would slow down charging. With events, each consumer goes at its own pace. The cost is eventual consistency: a few seconds where the plan is PAID and the reservation still says CONFIRMED_UNPAID. You can see that in the demo, between "worker runs" and "relay publishes".

**Why a sweeper?** Workers crash. Something independent must notice and repair, or rule #2 breaks silently.

---

## 4. File by file

Read them in this order. Each file's top docstring repeats the key idea.

### `schema.sql` — the whole data model
- `dev_clock`: a single-row table holding a time offset. A learning aid, so you can "jump weeks ahead".
- `listings`, `reservations`: owned by other services in real life.
- `payment_plans`: **the core table.** Key columns:
  - `charge_at`: when to try next. The worker query and the index are both built around it.
  - `deadline_at`: free-cancellation deadline in UTC. Never changes unless dates change.
  - `status`: the state machine (section 6).
  - `version`: optimistic locking. Every write bumps it.
  - `attempt_count`: part of the idempotency key.
  - `cancel_requested`: "guest cancelled while a charge was in flight".
  - `claimed_at` / `claimed_by`: which worker holds it, and since when. The sweeper uses these.
- `idx_plans_due`: **partial index** on `charge_at WHERE status = 'SCHEDULED'`.
- `outbox`, `processed_events`: event publishing and consumer dedupe.
- `psp_charges`, `psp_refunds`: the "external" processor's records.

### `app/config.py` — every knob
Buffer (2 days), retry backoff (6h, 12h, 24h), batch size, timeouts. Note the comment: **the retries add up to 42h, which fits inside the 48h buffer.** That's the buffer decision turned into numbers.

### `app/db.py` — connections
`connect()` opens an **autocommit** connection pinned to **UTC**. Autocommit means nothing is transactional unless you write `with conn.transaction():`, so every transaction is visible in the code. `init_db()` drops and recreates the schema and seeds five listings.

### `app/clock.py` — "what time is it?"
`now(conn)` returns database time plus the offset. Every time-based decision goes through it. This is **dependency injection for time**: without it you can't test "what happens three weeks from now".

### `app/timeutil.py` — the ONE place deadlines become times
- `cancellation_deadline(check_in, listing_tz, policy)`: N days before check-in, at 15:00 **listing-local**, converted to UTC.
- `first_charge_at(deadline)`: deadline − buffer − random jitter (jitter only makes the charge *earlier*).
- `to_local(ts, tz)`: for display.

This is the fix for a classic scheduling bug: reminders sent in the server's time zone instead of the listing's.

### `app/booking.py` — Booking API + Reservations (steps 1–3)
- `check_eligibility`: policy flexible/moderate, currency supported and not excluded, and not too close to the deadline. It's server-side, so you can turn off a market without an app release.
- `create_reservation`: one transaction: reservation + `plans.create_plan` + `ReservationConfirmed` event.
- `change_dates`: locks the reservation row, recomputes the deadline and calls `plans.reschedule`.
- `cancel_reservation`: marks the reservation CANCELLED and calls `plans.cancel_plan`, which decides what happens to the money.

### `app/plans.py` — payment-plan service (step 4)
- `cas_update(conn, plan, **changes)`: **the most important function in the project.** `UPDATE ... SET ..., version = version + 1 WHERE id = ? AND version = ?`. It returns False if someone else changed the row first. Every plan write anywhere goes through it.
- `create_plan`, `reschedule`, `update_card`: straightforward, all through `cas_update`.
- `cancel_plan`: the branching heart of guest races:
  - SCHEDULED / NEEDS_NEW_CARD → `CANCELLED` (no money moved)
  - IN_PROGRESS → set `cancel_requested` (the worker will sort it out)
  - PAID → `REFUND_PENDING`

### `app/fake_psp.py` — the processor (step 6)
- `charge(key, token, amount, currency, plan_id)`:
  1. Key seen before → return the stored result (`replayed=True`). **No second charge.**
  2. Otherwise decide by card token and `INSERT ... ON CONFLICT DO NOTHING`. If two identical requests race, only one insert wins and the loser reads the winner's result.
  3. `tok_timeout`: the money moves, then it raises `ProcessorTimeout`. This simulates a response lost on the network.
- `refund(refund_key, charge_key)`: refunds "whatever that charge key did", idempotently.

### `app/worker.py` — charge workers (step 5)
- `CLAIM_SQL` + `claim_batch`: the SKIP LOCKED claim (deep dive 8.1).
- `idempotency_key(plan)`: `plan-<id>-attempt-<n>`.
- `charge_plan`: calls the processor and turns a timeout into outcome `"unknown"`. The `crash_after_charge` flag is for failure injection.
- `_changes_for(plan, result, now)`: a **pure function** mapping outcome to (column changes, event). All the retry policy lives here:
  - succeeded → PAID
  - unknown → SCHEDULED in 1 minute, **same attempt number**
  - soft decline with retries and time left → SCHEDULED at now + backoff, attempt + 1
  - otherwise → NEEDS_NEW_CARD, attempt + 1
- `record_outcome`: one transaction: `cas_update` + outbox event. If the CAS fails, it calls `_handle_concurrent_change`.
- `_handle_concurrent_change`: either the guest cancelled mid-charge (→ REFUND_PENDING or CANCELLED) or the sweeper took the plan back (→ do nothing; the next attempt replays the same key).
- `process_refunds`: refunds REFUND_PENDING plans. It needs no claim step because refunds are idempotent.
- `run_once` / `run_forever`: the loop. An exception on one plan is logged, and that plan stays IN_PROGRESS for the sweeper.

### `app/sweeper.py` — the janitor
- `rescue_stuck_plans`: IN_PROGRESS longer than 5 minutes → SCHEDULED (same attempt, same key). If the guest also cancelled → REFUND_PENDING.
- `release_overdue_plans`: deadline passed and still SCHEDULED or NEEDS_NEW_CARD → RELEASED + `PlanReleased` event.

### `app/outbox.py` — `emit(conn, type, payload)`
One INSERT. It must be called inside the caller's transaction, and that's the whole trick.

### `app/relay.py` — outbox → broker
Locks unpublished rows (SKIP LOCKED again, so several relays are safe), publishes each one and marks them published. If anything raises, nothing is marked and the batch is redelivered. That's **at-least-once** delivery.

### `app/broker.py` — fake Kafka
`subscribe(type, handler)` and `publish(event)`. That's it.

### `app/consumers.py` — Reservations and Notifications
- `_first_time(conn, consumer, event_id)`: inserts into `processed_events`. It returns False on a duplicate. Because it runs in the **same transaction** as the effect, duplicates are no-ops.
- `reservations_consumer`: PAID / RELEASED, guarded by `AND status = 'CONFIRMED_UNPAID'`, so a late event can't bring back a cancelled reservation.
- `notifications_consumer`: renders messages, with times in the **listing's** time zone.

### `app/metrics.py` — "how do you know you're not falling behind?"
- `backlog_age_seconds`: age of the oldest due-but-unclaimed plan. This is the alert.
- `reconcile`: four SQL checks comparing our plans with the processor's charges: charged but not paid, paid without a charge, double charge, and unpaid past the deadline.

### `app/api.py` — HTTP
Thin FastAPI wrappers, plus `/dev/clock` endpoints to move time.

### `app/cli.py` — entry points
`init-db`, `worker`, `sweeper`, `relay`, `all`, `metrics`.

### `tests/test_rnpl.py` — 18 stories
Read them as documentation. Each one sets up a failure (crash, timeout, race, decline) and asserts that rules 1 and 2 still hold.

### `demo.py` — narrated tour
Four stories printed step by step, with the version number and the number of successful charges at each step.

---

## 5. The life of one booking

A guest books listing 1 (Brooklyn, flexible), checking in 30 days from now, with `tok_ok`.

1. **`booking.create_reservation`**
   - `check_eligibility`: flexible ✓, USD ✓. `timeutil.cancellation_deadline` = check-in − 1 day at 15:00 New York time, converted to UTC. That's later than now + 2 days ✓.
   - `with conn.transaction():`
     - `INSERT reservations (... 'CONFIRMED_UNPAID')`
     - `plans.create_plan` → `INSERT payment_plans (... status 'SCHEDULED', version 0, attempt 0, charge_at = deadline − 2 days − jitter)`
     - `outbox.emit('ReservationConfirmed', ...)`
   - COMMIT.
2. **Relay** publishes `ReservationConfirmed` → the notifications consumer sends "You're booked! $0 today...".
3. **Weeks pass.** Each worker's `CLAIM_SQL` returns nothing, because `charge_at > now`.
4. **`charge_at` arrives.** Worker A runs `claim_batch`:
   - tx A: `SELECT id ... FOR UPDATE SKIP LOCKED` → [plan 1]; `UPDATE ... IN_PROGRESS, version 1`. COMMIT.
   - Worker B's claim, running at the same moment, skips the locked row and then no longer matches it once it's IN_PROGRESS.
5. **`charge_plan`**: key `plan-1-attempt-0` → `fake_psp.charge` → new key → "succeeded", `ch_abc...` stored. The processor has now committed.
6. **`record_outcome`**: tx B: `cas_update(plan v1 → PAID, v2)` ✓ + `outbox.emit('PaymentSucceeded')`. COMMIT.
7. **Relay** publishes `PaymentSucceeded`:
   - reservations consumer: dedupe ✓ → `UPDATE reservations SET status='PAID'`
   - notifications consumer: dedupe ✓ → "Receipt: we charged 400.00 USD."
8. **`metrics.reconcile`** → [] (our records and the processor's agree).

Now replay it with a crash between steps 5 and 6. Run `pytest -k crash -v` and read that test.

---

## 6. The plan state machine

```
                     ┌────────────── guest updates card ───────────────┐
                     │                                                 │
   create ──► SCHEDULED ──claim──► IN_PROGRESS ──succeeded──► PAID     │
               ▲   │                  │  │  │                   │      │
   soft decline│   │                  │  │  └─hard decline / ───┼──► NEEDS_NEW_CARD
   (retry later)   │                  │  │    out of retries    │      │
   or unknown  └───┼──────────────────┘  │                      │      │
   (same key)      │                     │ guest cancelled      │      │
                   │                     │ mid-charge           │      │
                   │                     ▼                      ▼      │
                   │              REFUND_PENDING ◄── guest cancels after paying
                   │                     │
                   │                     ▼
                   │                 REFUNDED
                   │
                   ├── guest cancels ──► CANCELLED ◄── (from NEEDS_NEW_CARD too)
                   └── deadline passes ► RELEASED  ◄── (from NEEDS_NEW_CARD too)

   Sweeper: IN_PROGRESS for > 5 min ──► SCHEDULED (same attempt) or REFUND_PENDING
```

Terminal states: **PAID, REFUNDED, CANCELLED, RELEASED.** Rule #2 in state-machine terms: every plan eventually reaches a terminal state, and no plan sits in SCHEDULED or NEEDS_NEW_CARD after its deadline.

---

## 7. Transaction map

| Where | What's inside one transaction | Why together |
|---|---|---|
| `booking.create_reservation` | reservation + plan + event | fail closed: no reservation without a plan |
| `booking.change_dates` | lock reservation, update it, reschedule plan | dates and charge time can't disagree |
| `booking.cancel_reservation` | reservation CANCELLED + plan decision + event | consistent cancel |
| `worker.claim_batch` | SELECT FOR UPDATE SKIP LOCKED + mark IN_PROGRESS | atomic "this is mine" |
| *(none)* `fake_psp.charge` | network call **outside** any transaction | don't hold locks during I/O |
| `worker.record_outcome` | cas_update + event | status and event can't diverge |
| `sweeper.release_overdue_plans` | per plan: cas_update + event | same |
| `relay.relay_once` | lock unpublished rows, publish, mark | at-least-once |
| each consumer | dedupe insert + effect | exactly-once *effect* |
| `fake_psp.*` | its **own** connection | it's "another company" |

---

## 8. Deep dives

These are the topics a design review would push on. For each one: the concept, where it is in the code, and the follow-up question.

### 8.1 `FOR UPDATE SKIP LOCKED`

- `FOR UPDATE` locks the selected rows until your transaction ends.
- Plain `FOR UPDATE`: worker B **waits** for A's locks, then gets the same rows. That's serialized and slow.
- `SKIP LOCKED`: B **skips** locked rows and takes the next ones, so N workers get N different batches in parallel.
- It's safe because we **commit `IN_PROGRESS` before the network call**. Once A commits, the lock is gone, but the claim query only matches `status = 'SCHEDULED'`, so nobody else will pick the row up.
- Postgres is in READ COMMITTED by default. Look up what happens when an `UPDATE ... WHERE version = 7` blocks on a row lock and the other transaction commits version 8: Postgres re-checks the WHERE against the new row version, so it matches 0 rows. That's why the CAS is correct even under contention.
- **Follow-up:** "Why not `SELECT` without a lock and then `UPDATE ... WHERE status = 'SCHEDULED'`?" That works too (it's a CAS on status), but under contention many workers grab the same top 50 rows and most updates fail. SKIP LOCKED spreads the work out.
- Proof: `test_many_workers_never_double_charge` (8 threads, batches of 7, 120 plans → exactly 120 charges).

### 8.2 The partial index

```sql
CREATE INDEX idx_plans_due ON payment_plans (charge_at) WHERE status = 'SCHEDULED';
```
- It only contains rows that are *waiting*. PAID, CANCELLED and RELEASED rows pile up forever but never enter it, so the index size tracks the **pending** work, not total history.
- The claim query's `WHERE status = 'SCHEDULED' AND charge_at <= now ORDER BY charge_at LIMIT 50` matches it exactly. Postgres walks the index in order and stops after 50 rows.
- Try it: `EXPLAIN ANALYZE` the `CLAIM_SQL` in `psql` after inserting 100k plans (exercise 2). On a tiny table Postgres prefers a sequential scan, which is why the test sets `enable_seqscan = off`.
- **Follow-up:** "The status changes on every claim. Isn't that index churn?" Yes. Each update adds or removes index entries and leaves dead tuples for VACUUM. At high volume you'd tune autovacuum on this table.

### 8.3 Optimistic locking (`version` + `cas_update`)

- Read the row (version 7) → decide → `UPDATE ... WHERE version = 7`. If 0 rows matched, someone beat you: re-read and decide again.
- **Optimistic** because nothing is locked while you think. That's good when conflicts are rare, and they are here.
- **Pessimistic** alternative: `SELECT ... FOR UPDATE` and hold the lock. `booking.change_dates` does that on the *reservation* row because it's short and local.
- The big race, guest cancels while the worker is charging:
  1. Worker claimed it (v1, IN_PROGRESS).
  2. Cancel sees IN_PROGRESS → `cas_update(cancel_requested=True)` → v2.
  3. Worker finishes → `cas_update(..., WHERE version = 1)` → **fails**.
  4. Worker re-reads, sees `cancel_requested` → REFUND_PENDING.
- **Follow-up:** "What if the cancel lands first?" Then the plan is CANCELLED before any worker claims it, the claim query never matches, and no charge happens (`test_cancel_before_charge`).

### 8.4 Idempotency keys

- Key = `plan-<id>-attempt-<n>`. The processor stores the result per key, so the same key twice means one charge and the same answer.
- **When does `attempt_count` go up?** Only after a **definite** decline (soft or hard). Never after a crash or timeout, because there we don't know what happened, and **reusing the key is how we ask the processor**.
- Why a timeout must reuse the key: if the money moved and you retried with a *new* key, the guest gets charged twice. `test_processor_timeout_retries_with_same_key` proves the right behaviour.
- Why a definite decline must use a *new* key: the old key is permanently bound to "declined", so retrying it would just replay the decline.
- Why `update_card` is only allowed from NEEDS_NEW_CARD: that state is only reached after a definite decline. If you allowed a card change while an unknown outcome was pending, the next attempt with a new card would need a new key… and the old key might have charged the old card. (Real systems resolve the unknown first by querying the processor.)
- Real processors also reject a reused key with **different parameters** (a different amount, say). Our fake doesn't check this. Exercise 4.
- Two identical requests at once: `INSERT ... ON CONFLICT DO NOTHING` lets one win. Production systems often use a Redis lock for the same purpose.

### 8.5 Transactional outbox and at-least-once delivery

- **Problem:** "update DB, then publish to Kafka" is two systems. Any crash between them leaves them inconsistent. Two-phase commit across Postgres and Kafka is painful and usually unavailable.
- **Fix:** write the event into an `outbox` table **in the same transaction**. A relay publishes it later.
- The relay can publish and then crash before marking the row → the event is published twice. That's **at least once**, and it can't be avoided cheaply.
- **Consumers make it exactly-once in effect**: `processed_events (consumer, event_id)` insert + the effect in one transaction (`test_duplicate_event_delivery_has_no_double_effect`).
- Also make effects naturally safe: `UPDATE reservations SET status='PAID' WHERE ... AND status='CONFIRMED_UNPAID'`.
- **Ordering:** one relay publishing in `id` order keeps the order. With Kafka you'd key messages by `reservation_id` so one reservation's events stay in one partition, in order.
- **Follow-up:** "What about Debezium / CDC?" Change-data-capture reads the outbox from Postgres's write-ahead log instead of polling it. It's the same pattern with lower latency.

### 8.6 Time

- **Store UTC, compute in the listing's zone.** `timeutil.cancellation_deadline` builds the local 15:00 with `ZoneInfo(listing_tz)` and converts. `test_deadline_uses_listing_timezone` checks Tokyo and New York.
- **DST:** "15:00 local, one day before" may be 23 or 25 hours from the previous 15:00. Computing in local wall-clock time and then converting handles it. Adding `timedelta(hours=24)` to a UTC time does not.
- **The DB session time zone** is pinned to UTC in `db.connect()`. Without that, Postgres returns timestamps in the server's configured zone. The demo showed `-05:00` before that fix, which is a small version of the reminder bug.
- **Jitter:** many deadlines are exactly 15:00 local. Without jitter, every New York booking would charge in the same second. Jitter only subtracts, so the buffer is never eaten.
- **Clock injection:** `clock.now(conn)` everywhere means tests can jump weeks ahead. Calling `datetime.now()` deep in business logic is a testing dead end.

### 8.7 Retries: soft vs hard declines, sized to the buffer

- **Soft** (insufficient funds, try again later): the same card may work later → backoff 6h, 12h, 24h.
- **Hard** (expired, stolen): retrying is pointless → ask the guest right away (NEEDS_NEW_CARD + email).
- **Unknown** (timeout): not a decline at all → retry in 1 minute with the **same** key.
- The 2-day buffer exists so this whole schedule fits *before* the host's deadline. That's the buffer decision in numbers. In production you'd tune the buffer with data (how long guests take to fix cards). Here it's a config value.
- `_changes_for` also stops retrying if the next retry would land after the deadline.

### 8.8 The sweeper, leases, and "dead or just slow?"

- `claimed_at` + `IN_PROGRESS_TIMEOUT` act as a **lease**: if you haven't finished in 5 minutes, you've lost the plan.
- But what if the worker was only slow? It finishes after the sweeper reset the plan. Its `cas_update` fails (the sweeper bumped the version), and `_handle_concurrent_change` does **nothing**. Later, a worker re-claims the plan with the same key, and the processor replays the slow worker's result. **Idempotency is what makes an imprecise timeout safe.**
- The timeout must be well above the processor's own timeout, or you'll "rescue" plans that are still in flight on every slow day.

### 8.9 Fail closed

- "No plan, no booking" (`test_no_plan_no_booking` breaks `create_plan` and checks that no reservation exists).
- The general rule for money systems: when unsure, refuse the action you can't safely undo. A failed booking is annoying. A free night nobody will ever charge for is a real loss.

### 8.10 Monitoring: backlog age and reconciliation

- **Backlog age** = now − the oldest `charge_at` among due SCHEDULED plans. It beats queue length: 10,000 plans due this second is fine, while one plan waiting 2 hours is an incident. In the real system the workers autoscale on it (instead of CPU) and it pages someone.
- **Reconciliation** compares two independent records of the truth (ours and the processor's). It catches bugs your code doesn't know it has. `test_reconciliation_catches_money_without_a_booking` creates such a bug on purpose.
- The lesson: build these before launch, not after.

### 8.11 Money representation

- Integers in **minor units** (`amount_cents`), never floats. `0.1 + 0.2 != 0.3`.
- Gotcha you can see in this code: **JPY has no minor unit.** The Tokyo listing stores 1,800,000 "cents", and the receipt prints `36000.00 JPY`, which is wrong. Real systems store an exponent per currency (ISO 4217: USD 2, JPY 0, BHD 3). Exercise 5.
- Each plan charges in the currency the guest booked in. There's no conversion in the worker.

---

## 9. What this leaves out

Know these so you can say "in the real system we also had…":

- **One database for everything.** Real services each own their tables and talk over APIs and events. The fake processor lives in the same DB, but on its own connection and commits.
- **Releasing a plan with an unknown outcome.** If the last attempt timed out and the deadline passes, the sweeper releases the plan even though money might have moved. Reconciliation would flag it (check A). A real system would ask the processor before releasing. Exercise 3.
- **Calendar and overlap checks** at booking time.
- **Reminder emails** before `charge_at` (they'd go through `timeutil`, like everything else involving time).
- **Rate limiting per processor** (Redis is a common choice). One way: a token bucket per PSP in `worker.py`.
- **Connection pooling** (`psycopg_pool`), auth, retries on DB errors, graceful shutdown, structured logs and metrics export.
- **Polling cost.** Fine here. At scale you might poll less often, use `LISTEN/NOTIFY`, or split the table by time.
- **Refund policy.** Here, cancelling after payment always refunds in full. That's reasonable because we charge before the free-cancellation deadline, but real policies are more complex.

---

## 10. Exercises

Ordered from easy to hard. Each one teaches one deep dive.

1. **Watch SKIP LOCKED with your own eyes.** Open two `psql` sessions. In both, `BEGIN;` then run `CLAIM_SQL` with `LIMIT 1`. Compare the rows each gets. Then remove `SKIP LOCKED` and repeat (the second session will hang).
2. **Measure the partial index.** Insert 200k PAID plans and 1k SCHEDULED ones (`generate_series`). Compare `\di+ idx_plans_due` with a full index on `charge_at`, and `EXPLAIN ANALYZE` the claim query with and without it.
3. **Fix the unknown-outcome release gap.** Before releasing, if `last_error = 'processor_timeout'`, call `fake_psp.charge` with the same key to learn the truth. Write the test first.
4. **Make the fake processor reject a reused key with a different amount** (like Stripe does), and add a test.
5. **Fix JPY.** Add a currency-exponent map and use it in notifications.
6. **Add reminders.** A `reminder_at` column (24h before `charge_at`, computed in `timeutil`) and a `ChargeReminder` event. Test it with a listing in `Asia/Kolkata` (UTC+5:30).
7. **Kill a real worker.** Run `python -m app.cli worker` in three terminals plus `relay` and `sweeper`, create 500 bookings, jump the clock, and `kill -9` one worker mid-run. Then check `python -m app.cli metrics`.
8. **Swap the broker for Redis Streams or real Kafka.** Only `broker.py` and `relay.py` should need to change. If anything else changes, the boundary leaked.

---

## 11. Design question → file to point at

| They ask | Point at |
|---|---|
| "Why not a cron job / delayed queue?" | §3, `plans.reschedule`, `metrics.py` |
| "How do two workers not charge the same plan?" | `worker.CLAIM_SQL`, §8.1, `test_many_workers_never_double_charge` |
| "Worker dies halfway?" | `sweeper.rescue_stuck_plans`, §8.8, `test_worker_crash_after_charge...` |
| "Guest cancels during the charge?" | `plans.cancel_plan`, `worker._handle_concurrent_change`, §8.3 |
| "Processor times out?" | `worker._changes_for` (unknown branch), §8.4 |
| "How do you keep the claim query fast?" | `idx_plans_due`, §8.2 |
| "How do you know you're behind?" | `metrics.backlog_age_seconds`, §8.10 |
| "Why the outbox?" | `outbox.py`, `relay.py`, `consumers._first_time`, §8.5 |
| "Hardest bug?" | `timeutil.py`, `db.connect` UTC pin, §8.6 |
| "How did you test it?" | `tests/test_rnpl.py`: every test is a failure injection |
| "How does this apply to Rippling?" | Replace "charge a card on `charge_at`" with "provision apps on start date / revoke access on termination date". Same table scheduler, same idempotency keys toward third parties (Slack, Google Workspace), same "what happens when one call fails partway" (§8.3, §8.8). |
