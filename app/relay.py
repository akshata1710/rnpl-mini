"""Outbox relay: reads unpublished outbox rows and publishes them.

    outbox row (written with the state change)
        -> relay publishes it
        -> marks published_at

If the relay crashes after publishing but before marking, the event is
published AGAIN next time. That's "at least once". Consumers must
de-duplicate (see consumers.py). Exactly-once delivery over a network is
not something you get for free; exactly-once EFFECT is what you build.
"""
import threading
import time

import psycopg

from app import broker, consumers
from app.db import connect


def relay_once(conn: psycopg.Connection, limit: int = 100) -> int:
    with conn.transaction():
        rows = conn.execute(
            """
            SELECT * FROM outbox
             WHERE published_at IS NULL
             ORDER BY id
             LIMIT %s
               FOR UPDATE SKIP LOCKED     -- several relays can run safely
            """,
            (limit,),
        ).fetchall()
        for row in rows:
            broker.publish({
                "id": row["id"],
                "event_type": row["event_type"],
                "payload": row["payload"],
            })
            # If publish() raises, this transaction rolls back, nothing is
            # marked, and every row in the batch is retried next time
            # (including ones already delivered -> duplicates -> dedupe).
        if rows:
            conn.execute(
                "UPDATE outbox SET published_at = now() WHERE id = ANY(%s)",
                ([r["id"] for r in rows],),
            )
    return len(rows)


def drain(conn: psycopg.Connection) -> int:
    """Publish until the outbox is empty (tests/demo)."""
    consumers.register()
    total = 0
    while n := relay_once(conn):
        total += n
    return total


def run_forever(stop: threading.Event | None = None) -> None:
    stop = stop or threading.Event()
    consumers.register()
    with connect() as conn:
        while not stop.is_set():
            if relay_once(conn) == 0:
                time.sleep(0.5)
