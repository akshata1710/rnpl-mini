"""A fake payment processor (Step 6: pay-in platform + PSP + vault).

Pretend this is Stripe or Adyen behind a pay-in platform. Two things
make it behave like a real external system:

1. It uses its OWN database connection and commits immediately. Our
   transactions cannot roll it back. Once it says "charged", money moved.
2. It remembers every idempotency key. Same key again -> same result
   again, and NO second charge. This is the safety net under everything.

Card tokens decide what happens (we never see card numbers, only tokens):

  tok_ok                 always succeeds
  tok_insufficient_funds always soft-declines (retryable)
  tok_flaky              soft-declines on its first 2 keys, then succeeds
  tok_expired            hard decline (retrying won't help)
  tok_stolen             hard decline
  tok_timeout            charge SUCCEEDS but the first response for each
                         key is "lost" (we raise ProcessorTimeout). Our side
                         can't tell whether money moved.
"""
import uuid
from dataclasses import dataclass

from app.db import connect


class ProcessorTimeout(Exception):
    """The request may or may not have gone through. Outcome UNKNOWN."""


@dataclass
class ChargeResult:
    outcome: str                 # 'succeeded' | 'soft_decline' | 'hard_decline'
    charge_id: str | None = None
    decline_code: str | None = None
    replayed: bool = False       # True = we returned a stored result for a repeated key


def _decide(conn, card_token: str) -> tuple[str, str | None]:
    if card_token in ("tok_ok", "tok_timeout"):
        return "succeeded", None
    if card_token == "tok_insufficient_funds":
        return "soft_decline", "insufficient_funds"
    if card_token == "tok_flaky":
        seen = conn.execute(
            "SELECT count(*) AS n FROM psp_charges WHERE card_token = 'tok_flaky'"
        ).fetchone()["n"]
        return ("soft_decline", "try_again_later") if seen < 2 else ("succeeded", None)
    if card_token == "tok_expired":
        return "hard_decline", "expired_card"
    if card_token == "tok_stolen":
        return "hard_decline", "stolen_card"
    return "hard_decline", "invalid_token"


def charge(idempotency_key: str, card_token: str, amount_cents: int,
           currency: str, plan_id: int) -> ChargeResult:
    with connect() as conn:
        # 1. Seen this key before? Return the SAME answer. Never charge twice.
        existing = conn.execute(
            "SELECT * FROM psp_charges WHERE idempotency_key = %s", (idempotency_key,)
        ).fetchone()
        if existing:
            return ChargeResult(existing["outcome"], existing["charge_id"],
                                existing["decline_code"], replayed=True)

        # 2. New key: "talk to the bank".
        outcome, code = _decide(conn, card_token)
        charge_id = f"ch_{uuid.uuid4().hex[:12]}" if outcome == "succeeded" else None

        # 3. Store the result under the key. ON CONFLICT handles two requests
        #    with the same key arriving at the same moment: only one insert
        #    wins, the loser reads the winner's row. (Real processors use a
        #    lock for this, often in Redis.)
        row = conn.execute(
            """
            INSERT INTO psp_charges
                (idempotency_key, plan_id, card_token, amount_cents, currency, outcome, decline_code, charge_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (idempotency_key) DO NOTHING
            RETURNING *
            """,
            (idempotency_key, plan_id, card_token, amount_cents, currency, outcome, code, charge_id),
        ).fetchone()
        if row is None:
            row = conn.execute(
                "SELECT * FROM psp_charges WHERE idempotency_key = %s", (idempotency_key,)
            ).fetchone()
            return ChargeResult(row["outcome"], row["charge_id"], row["decline_code"], replayed=True)

    # 4. The money has moved (committed above). For tok_timeout we now
    #    "lose" the response, like a network timeout on the way back.
    if card_token == "tok_timeout":
        raise ProcessorTimeout(f"no response for {idempotency_key}")
    return ChargeResult(outcome, charge_id, code)


def refund(refund_key: str, charge_key: str) -> str:
    """Refund whatever the charge with `charge_key` did, if anything.
    Idempotent on refund_key. Returns 'refunded' or 'nothing_to_refund'."""
    with connect() as conn:
        existing = conn.execute(
            "SELECT status FROM psp_refunds WHERE refund_key = %s", (refund_key,)
        ).fetchone()
        if existing:
            return existing["status"]
        charge_row = conn.execute(
            "SELECT outcome FROM psp_charges WHERE idempotency_key = %s", (charge_key,)
        ).fetchone()
        status = "refunded" if charge_row and charge_row["outcome"] == "succeeded" else "nothing_to_refund"
        conn.execute(
            """INSERT INTO psp_refunds (refund_key, charge_key, status) VALUES (%s, %s, %s)
               ON CONFLICT (refund_key) DO NOTHING""",
            (refund_key, charge_key, status),
        )
        return conn.execute(
            "SELECT status FROM psp_refunds WHERE refund_key = %s", (refund_key,)
        ).fetchone()["status"]
