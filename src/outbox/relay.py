"""The relay: read pending outbox rows and publish them, at least once.

It claims pending events, hands each to ``publish``, and marks it ``sent`` only
if publishing succeeded. If ``publish`` raises (the broker is down), the event is
released back to ``pending`` and retried on the next run, so nothing is lost.
Because an event can therefore be delivered more than once, consumers must be
idempotent (see ``IdempotentConsumer``).
"""

from __future__ import annotations

from typing import Callable

from .store import Event, OutboxStore

Publish = Callable[[Event], None]


def relay_once(store: OutboxStore, publish: Publish, limit: int = 100) -> int:
    """Deliver the currently pending events. Returns how many were sent."""
    delivered = 0
    for event in store.claim_pending(limit):
        try:
            publish(event)
        except Exception:
            store.release(event.id)   # leave it for the next run
            continue
        store.mark_sent(event.id)
        delivered += 1
    return delivered
