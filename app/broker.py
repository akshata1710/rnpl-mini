"""A 15-line stand-in for Kafka.

Kafka gives you: durable topics, partitions, consumer groups, replay.
For learning we only need "publish an event, every subscriber gets it".
The important behaviours we keep:
  * delivery is AT LEAST ONCE (the relay may publish an event twice),
  * each consumer handles events independently, with its own DB transaction.

In production you'd publish with the reservation_id as the Kafka message
key, so all events for one reservation land in one partition, in order.
"""
from collections import defaultdict
from typing import Callable

Handler = Callable[[dict], None]
_subscribers: dict[str, list[Handler]] = defaultdict(list)


def subscribe(event_type: str, handler: Handler) -> None:
    """event_type '*' means 'everything'."""
    if handler not in _subscribers[event_type]:
        _subscribers[event_type].append(handler)


def publish(event: dict) -> None:
    for handler in _subscribers[event["event_type"]] + _subscribers["*"]:
        handler(event)   # if a handler raises, the relay will retry the event later
