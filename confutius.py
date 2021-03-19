from __future__ import annotations

from functools import wraps
import logging
import concurrent.futures
import json
import signal
from contextlib import ContextDecorator

import confluent_kafka
from random import randint
import time
from dataclasses import dataclass


logging.basicConfig(level=logging.NOTSET)
logger = logging.getLogger(__name__)


@dataclass
class Topic:
    name: str


class Consumer(ContextDecorator):
    """
    Kafka consumers as a context manager.

    Example
    -------
    >>> with Consumer(topic) as consumer:
           ...
    """

    def __init__(self, topic: Topic, group: str = "eventbus"):
        super().__init__()
        self.topic = topic
        self.group = group

    def __repr__(self):
        return f"<{self.__class__.__name__}(topic=Topic({self.topic.name}), group='{self.group}')>"

    def __enter__(self):
        # TODO: confluent_kafka.KafkaError
        self.consumer = confluent_kafka.Consumer(
            {
                "bootstrap.servers": "localhost:9092",
                "group.id": self.group,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )
        self.consumer.subscribe([self.topic.name])
        return self.consumer

    def __exit__(self, type, value, traceback):
        self.consumer.close()

        if type and value and traceback:
            logger.exception(f"Error: {self}", exc_info=True)


def agent(topic: Topic=None, poll_timeout = 5):
    """
    Decorator to create a method into an agent.

    An agent is a simple method that consumes/listens to a specific topic on the broker.
    """
    def _action_decorator(func):
        # 'func' is the actual method we are decorating
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Subscribe a new consumer to a topic
            with Consumer(topic) as consumer:

                # Poll topic for an event
                raw_event = consumer.poll(poll_timeout)
                if raw_event is None:
                    return None
                elif raw_event.error():
                    logger.error("Consumer error: {}".format(raw_event.error()))
                    return None

                # Parse/deserialize event
                # TODO: validate schema for event and type check
                try:
                    event = json.loads(raw_event.value().decode("utf-8"))
                except json.JSONDecodeError:
                    logger.error("Error decoding JSON event.")
                    return None

                # Pass only the event to our method
                result = func(event)

                # Only commit on success, no exceptions
                consumer.commit()

                return result

        return wrapper

    return _action_decorator


# Topics
# ------
topic_a = Topic("topic_a")
topic_b = Topic("topic_b")


# Agents
# -------
@agent(topic=topic_a)
def method_a(event):
    logger.info(f"Consumer A received: {event}")
    time.sleep(randint(0, 2))
    return "A"


@agent(topic_b)
def method_b(event):
    logger.info(f"Consumer B received: {event}")
    time.sleep(randint(0, 2))
    return "B"


INTERRUPTED = False

def handle_interrupt(signum, _):
    logger.info(f"Received signal {signum}, quitting consumers")
    global INTERRUPTED
    INTERRUPTED = True


def main(max_workers=2):
    logger.info("Starting Confutius")

    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
        while True:
            if INTERRUPTED:
                # TODO: Figure out if all kafka consumers are closed, cleaned up.
                break

            future_a = executor.submit(method_a)
            future_b = executor.submit(method_b)
            for future in concurrent.futures.as_completed([future_a, future_b]):
                # TODO: Exception handling
                data = future.result()

            logger.info("Tick...")
            time.sleep(5)


if __name__ == "__main__":
    signal.signal(signal.SIGINT, handle_interrupt)
    signal.signal(signal.SIGTERM, handle_interrupt)
    signal.signal(signal.SIGQUIT, handle_interrupt)

    main()
