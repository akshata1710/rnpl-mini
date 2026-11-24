"""Transactional outbox: write the event in the SAME transaction as the change.

Why not publish to Kafka directly after the UPDATE?
  * UPDATE commits, then the process dies before publishing -> event lost.
  * Publish first, then the UPDATE fails -> event for a change that never happened.
Writing both in one DB transaction makes them succeed or fail together.
The relay (relay.py) publishes the rows later.
"""
import psycopg
from psycopg.types.json import Jsonb


def emit(conn: psycopg.Connection, event_type: str, payload: dict) -> None:
    """Must be called INSIDE the caller's `with conn.transaction():` block."""
    conn.execute(
        "INSERT INTO outbox (event_type, payload) VALUES (%s, %s)",
        (event_type, Jsonb(payload)),
    )
