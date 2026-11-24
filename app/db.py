"""Database connection + setup.

We use psycopg 3 in AUTOCOMMIT mode and open transactions explicitly with
`with conn.transaction():`. That makes every transaction boundary visible
in the code, which is the whole point of this project.
"""
from pathlib import Path

import psycopg
from psycopg.rows import dict_row

from app.config import DATABASE_URL

SCHEMA_FILE = Path(__file__).resolve().parent.parent / "schema.sql"


def connect() -> psycopg.Connection:
    """A new connection. Rows come back as dicts: row["status"].

    The session time zone is pinned to UTC. Otherwise timestamps come back
    in whatever zone the DB SERVER is configured for, which is exactly the
    "server time zone vs listing time zone" bug.
    """
    return psycopg.connect(DATABASE_URL, row_factory=dict_row, autocommit=True,
                           options="-c timezone=UTC")


def init_db(seed: bool = True) -> None:
    """Drop everything and recreate the schema (dev only!)."""
    with connect() as conn:
        conn.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
        conn.execute(SCHEMA_FILE.read_text())
        if seed:
            seed_listings(conn)


def seed_listings(conn: psycopg.Connection) -> None:
    conn.execute(
        """
        INSERT INTO listings (id, name, timezone, cancellation_policy, nightly_price_cents, currency) VALUES
          (1, 'Brooklyn loft',      'America/New_York', 'flexible', 20000, 'USD'),
          (2, 'Paris studio',       'Europe/Paris',     'moderate', 15000, 'EUR'),
          (3, 'Tokyo apartment',    'Asia/Tokyo',       'flexible', 1800000, 'JPY'),
          (4, 'Aspen cabin',        'America/Denver',   'strict',   50000, 'USD'),
          (5, 'Sao Paulo flat',     'America/Sao_Paulo','flexible', 40000, 'BRL');
        SELECT setval('listings_id_seq', 5);
        """
    )
