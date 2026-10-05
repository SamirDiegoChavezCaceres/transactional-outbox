"""An idempotent consumer: process each event id at most once.

At-least-once delivery means the same event can arrive twice (a retry after a
publish that actually went through, a redelivery after a crash). The consumer
records the ids it has handled and skips duplicates, so the effect happens once.
"""

from __future__ import annotations

import sqlite3
from typing import Callable

from .store import Event


class IdempotentConsumer:
    def __init__(self, path: str = ":memory:") -> None:
        self.conn = sqlite3.connect(path)
        self.conn.execute("CREATE TABLE IF NOT EXISTS processed (event_id TEXT PRIMARY KEY)")

    def handle(self, event: Event, action: Callable[[Event], None]) -> bool:
        """Run ``action`` for this event unless it was already processed.

        Returns True if the action ran, False if it was a duplicate.
        """
        with self.conn:
            seen = self.conn.execute(
                "SELECT 1 FROM processed WHERE event_id = ?", (event.id,)
            ).fetchone()
            if seen:
                return False
            action(event)
            self.conn.execute("INSERT INTO processed (event_id) VALUES (?)", (event.id,))
            return True
