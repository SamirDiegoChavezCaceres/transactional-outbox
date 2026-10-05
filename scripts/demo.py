"""Walk the transactional outbox end to end.

    python scripts/demo.py
"""

from __future__ import annotations

from outbox import IdempotentConsumer, OutboxStore, relay_once


def rule(title: str) -> None:
    print(f"\n=== {title} ===")


def main() -> None:
    store = OutboxStore()

    rule("1. One transaction writes the change AND the event")
    store.apply_change("acct-1", 100, "money_deposited")
    print(f"  balance(acct-1) = {store.balance('acct-1')}  |  pending events = {store.count('pending')}")

    rule("2. If the business rule fails, nothing is written (atomic rollback)")
    try:
        store.apply_change("acct-1", -500, "money_withdrawn")
    except ValueError as exc:
        print(f"  rejected: {exc}")
    print(f"  balance still {store.balance('acct-1')}  |  pending still {store.count('pending')} (no stray event)")

    rule("3. The broker is down: the event is kept, not lost")
    def broker_down(_event):
        raise RuntimeError("broker unavailable")
    sent = relay_once(store, broker_down)
    print(f"  delivered = {sent}  |  pending = {store.count('pending')} (released for retry)")

    rule("4. The broker recovers: the relay delivers it")
    published = []
    sent = relay_once(store, published.append)
    print(f"  delivered = {sent}  |  sent = {store.count('sent')}  |  pending = {store.count('pending')}")

    rule("5. At-least-once, so the consumer is idempotent")
    consumer = IdempotentConsumer()
    applied = []
    event = published[0]
    first = consumer.handle(event, lambda e: applied.append(e.id))
    again = consumer.handle(event, lambda e: applied.append(e.id))   # duplicate delivery
    print(f"  first delivery handled = {first}  |  duplicate handled = {again}")
    print(f"  action ran {len(applied)} time (exactly once)")


if __name__ == "__main__":
    main()
