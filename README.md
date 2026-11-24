# rnpl-mini — Reserve Now, Pay Later, small enough to read in one sitting

A learning version of a "book for $0 now, charge automatically before the
free-cancellation deadline" system. It's built for understanding, not scale.
About 1,300 lines of heavily commented Python in `app/`, one Postgres database and no other infrastructure.

**Start with [GUIDE.md](GUIDE.md).** It explains every file, every dependency,
why the architecture flows the way it does, and the deep dives.

## Run it

```bash
# 1. Postgres
docker compose up -d

# 2. Python deps (3.11+)
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 3. Tables + sample listings
python -m app.cli init-db

# 4a. The narrated tour (fastest way to see everything)
python demo.py

# 4b. The failure-injection tests (WIPE the database)
pytest -v

# 4c. Or run it "for real": two terminals
python -m app.cli all                 # 2 workers + sweeper + relay
uvicorn app.api:app --reload          # http://localhost:8000/docs
```

Then, in the Swagger UI at `/docs`:
1. `POST /reservations` with a check-in about 20 days out and `card_token: "tok_expired"`
2. `POST /dev/clock/advance` with `{"days": 18}` (jump past `charge_at`)
3. `GET /reservations/1`: plan is `NEEDS_NEW_CARD`, with an email asking for a new card
4. `POST /reservations/1/card` with `{"card_token": "tok_ok"}`, wait 2 seconds, then `GET` again: `PAID`
5. `GET /metrics`

Card tokens: `tok_ok`, `tok_insufficient_funds`, `tok_flaky`, `tok_expired`, `tok_stolen`, `tok_timeout`.
Listings: 1 Brooklyn (USD, flexible), 2 Paris (EUR, moderate), 3 Tokyo (JPY, flexible),
4 Aspen (strict, not eligible), 5 São Paulo (BRL, not eligible).
