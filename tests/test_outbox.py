import pytest

from outbox import IdempotentConsumer, OutboxStore, relay_once


def test_change_and_event_are_written_together():
    s = OutboxStore()
    s.apply_change("a", 100, "deposited")
    assert s.balance("a") == 100
    assert s.count("pending") == 1


def test_failed_change_rolls_back_both():
    s = OutboxStore()
    s.apply_change("a", 100, "deposited")
    with pytest.raises(ValueError):
        s.apply_change("a", -500, "withdrawn")   # would go negative
    assert s.balance("a") == 100                 # business change rolled back
    assert s.count("pending") == 1               # and no stray event


def test_relay_delivers_and_marks_sent():
    s = OutboxStore()
    s.apply_change("a", 100, "deposited")
    published = []
    sent = relay_once(s, published.append)
    assert sent == 1
    assert s.count("sent") == 1 and s.count("pending") == 0
    # nothing left to deliver on a second run
    assert relay_once(s, published.append) == 0
    assert len(published) == 1


def test_publish_failure_keeps_event_pending():
    s = OutboxStore()
    s.apply_change("a", 100, "deposited")

    def broker_down(_event):
        raise RuntimeError("down")

    assert relay_once(s, broker_down) == 0
    assert s.count("pending") == 1               # released for retry, not lost
    # a later healthy run delivers it
    assert relay_once(s, lambda e: None) == 1


def test_idempotent_consumer_dedups():
    s = OutboxStore()
    s.apply_change("a", 100, "deposited")
    published = []
    relay_once(s, published.append)
    event = published[0]

    consumer = IdempotentConsumer()
    applied = []
    assert consumer.handle(event, lambda e: applied.append(e.id)) is True
    assert consumer.handle(event, lambda e: applied.append(e.id)) is False
    assert len(applied) == 1
