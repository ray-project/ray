Events
======

Introduction
------------

In order to allow an event to trigger a workflow, workflows support pluggable event systems. Using the event framework provides a few properties.

1. Waits for events efficiently (without requiring a running workflow task while waiting).
2. Supports exactly-once event delivery semantics while providing fault tolerance.

Like other workflow tasks, events support fault tolerance via checkpointing. When an event occurs, the event is checkpointed, then optionally committed.


Using events
------------

Workflow events are a special type of workflow task. They "finish" when the event occurs. `workflow.wait_for_event(EventListenerType` can be used to create an event task.


.. code-block:: python

    from ray import workflow
    import time

    # Create an event which finishes after 60 seconds.
    event1_task = workflow.wait_for_event(workflow.event_listener.TimerListener, time.time() + 60)

    # Create another event which finishes after 30 seconds.
    event2_task = workflow.wait_for_event(workflow.event_listener.TimerListener, time.time() + 30)

    @ray.remote
    def gather(*args):
        return args

    # Gather will run after 60 seconds, when both event1 and event2 are done.
    workflow.create(gather.bind(event1_task, event_2_task)).run()


Custom event listeners
----------------------

Custom event listeners can be written by subclassing the EventListener interface.

.. code-block:: python

    class EventListener:
        def __init__(self):
            """Optional constructor. Only the constructor with now arguments will be
              called."""
            pass

        async def poll_for_event(self, *args, **kwargs) -> Event:
            """Should return only when the event is received."""
            raise NotImplementedError

        async def event_checkpointed(self, event: Event) -> None:
            """Optional. Called after an event has been checkpointed and a transaction can
              be safely committed."""
            pass

The `listener.poll_for_events()` coroutine should finish when the event is done. Arguments to `workflow.wait_for_event` are passed to `poll_for_events()`. For example, an event listener which sleeps until a timestamp can be written as:

.. code-block:: python

    class TimerListener(EventListener):
        async def poll_for_event(self, timestamp):
            await asyncio.sleep(timestamp - time.time())


The `event_checkpointed` routine can be overridden to support systems with exactly once delivery semantics which typically follow a pattern of:

1. Wait for event.
2. Process event.
3. Commit event.

After the workflow finishes checkpointing the event, the event listener will be invoked and can free the event. For example, to guarantee that events are consumed from a `kafkaesque<https://docs.confluent.io/clients-confluent-kafka-python/current/overview.html#synchronous-commits>`  queue:


.. code-block:: python

    KafkaEventType = ...

    class QueueEventListener:

        def __init__(self):
            # Initialize the poll consumer.
            self.consumer = Consumer({'enable.auto.commit': False})


        async def poll_for_event(self, topic) -> KafkaEventType:
            self.consumer.subscribe(topic)

            message = await self.consumer.poll()
            return message

        async def event_checkpointed(self, event: KafkaEventType) -> None:
             self.consuemr.commit(event, asynchronous=False)


(Advanced) Event listener semantics
-----------------------------------

When writing complex event listeners, there are a few properties the author should be aware of.

* The event listener **definition** must be serializable
* Event listener instances are _not_ serialized.
* Event listeners should be **stateless**.
