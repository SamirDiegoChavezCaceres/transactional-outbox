"""The outbox store: apply a business change and record its event atomically.

The dual-write problem: if you change the database and then publish an event as
two separate steps, a crash in between leaves the two out of sync. The fix is to
write the business row and an ``outbox`` row **in one transaction**. Either both
land or neither does. A separate relay publishes the outbox rows afterwards.
"""

from __future__ import annotations

import json
import sqlite3
import time
import uuid
from dataclasses import dataclass
from typing import List, Optional

_SCHEMA = """
CREATE TABLE IF NOT EXISTS accounts (
    id TEXT PRIMARY KEY,
    balance INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS outbox (
    id TEXT PRIMARY KEY,
    aggregate TEXT NOT NULL,
    type TEXT NOT NULL,
    payload TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    created_at REAL NOT NULL
);
"""


@dataclass
class Event:
    id: str
    aggregate: str
    type: str
    payload: dict
    status: str
    created_at: float


def _row_to_event(row: sqlite3.Row) -> Event:
    return Event(row["id"], row["aggregate"], row["type"],
                 json.loads(row["payload"]), row["status"], row["created_at"])


class OutboxStore:
    def __init__(self, path: str = ":memory:") -> None:
        self.conn = sqlite3.connect(path)
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript(_SCHEMA)

    # --- the heart: one transaction for the change AND the event ---------
    def apply_change(self, account_id: str, delta: int, event_type: str) -> str:
        """Adjust an account balance and enqueue an event, atomically.

        A negative resulting balance raises, which rolls back *both* the balance
        change and the outbox row.
        """
        with self.conn:  # commits on success, rolls back on any exception
            cur = self.conn.cursor()
            cur.execute(
                "INSERT INTO accounts (id, balance) VALUES (?, ?) "
                "ON CONFLICT(id) DO UPDATE SET balance = balance + ?",
                (account_id, delta, delta),
            )
            balance = cur.execute(
                "SELECT balance FROM accounts WHERE id = ?", (account_id,)
            ).fetchone()["balance"]
            if balance < 0:
                raise ValueError("insufficient funds")
            event_id = str(uuid.uuid4())
            payload = json.dumps({"account_id": account_id, "delta": delta, "balance": balance})
            cur.execute(
                "INSERT INTO outbox (id, aggregate, type, payload, status, created_at) "
                "VALUES (?, ?, ?, ?, 'pending', ?)",
                (event_id, account_id, event_type, payload, time.time()),
            )
            return event_id

    # --- relay helpers ---------------------------------------------------
    def claim_pending(self, limit: int = 100) -> List[Event]:
        """Mark pending rows as 'sending' and return them, so a second relay
        does not pick the same events."""
        with self.conn:
            rows = self.conn.execute(
                "SELECT * FROM outbox WHERE status = 'pending' ORDER BY created_at LIMIT ?",
                (limit,),
            ).fetchall()
            for r in rows:
                self.conn.execute("UPDATE outbox SET status = 'sending' WHERE id = ?", (r["id"],))
        return [_row_to_event(r) for r in rows]

    def mark_sent(self, event_id: str) -> None:
        with self.conn:
            self.conn.execute("UPDATE outbox SET status = 'sent' WHERE id = ?", (event_id,))

    def release(self, event_id: str) -> None:
        """Return a claimed-but-not-delivered event to 'pending' for retry."""
        with self.conn:
            self.conn.execute(
                "UPDATE outbox SET status = 'pending' WHERE id = ? AND status = 'sending'",
                (event_id,),
            )

    # --- inspection ------------------------------------------------------
    def balance(self, account_id: str) -> Optional[int]:
        row = self.conn.execute(
            "SELECT balance FROM accounts WHERE id = ?", (account_id,)
        ).fetchone()
        return row["balance"] if row else None

    def count(self, status: str) -> int:
        return self.conn.execute(
            "SELECT COUNT(*) AS n FROM outbox WHERE status = ?", (status,)
        ).fetchone()["n"]
