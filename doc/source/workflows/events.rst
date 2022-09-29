Events
======

To allow an event to trigger a workflow, Ray Workflows supports pluggable event systems. Using the event framework provides a few properties.

1. Waits for events efficiently (without requiring a running workflow task while waiting).
2. Supports exactly-once event delivery semantics while providing fault tolerance.

Like other workflow tasks, events support fault tolerance via checkpointing. When an event occurs, the event is checkpointed, then optionally committed.


Using events
------------

Workflow events are a special type of workflow task. They "finish" when the event occurs. `workflow.wait_for_event(EventListenerType)` can be used to create an event task.


.. code-block:: python

    import time
    import ray
    from ray import workflow

    # Create an event which finishes after 60 seconds.
    event1_task = workflow.wait_for_event(workflow.event_listener.TimerListener, time.time() + 60)

    # Create another event which finishes after 30 seconds.
    event2_task = workflow.wait_for_event(workflow.event_listener.TimerListener, time.time() + 30)

    @ray.remote
    def gather(*args):
        return args

    # Gather will run after 60 seconds when both event1 and event2 are done.
    workflow.run(gather.bind(event1_task, event2_task))


HTTP events
-----------

Workflow supports sending external events via HTTP.
An HTTP event listener in the workflow is used to connect to an HTTP endpoint.
Below is an end-to-end example of using HTTP events in a workflow.


``HTTPListener`` is used to listen for HTTP events in a workflow. Each ``HTTPListener`` subscribes to a unique `workflow_id` and `event_key` pair. To send an event to the listener, an HTTP request from
an external client should specify ``workflow_id`` as part of the request URL and the ``event_key`` and ``event_payload`` keys in the JSON request body (see below).

.. literalinclude:: ./doc_code/wait_for_event_http.py
   :language: python
   :start-after: __wait_for_event_begin__
   :end-before: __wait_for_event_end__

An HTTP endpoint at ``http://hostname:port/event/send_event/<workflow_id>`` can be used to send an event. Locally,
the endpoint may be reached at ``http://127.0.0.1:8000/event/send_event/<workflow_id>``.
Note that the HTTP request must include the same ``workflow_id``.
Each request should also include a JSON body with two fields: ``event_key`` and ``event_payload``, as shown in the example
below. The ``event_key`` field should match the argument passed to ``workflow.wait_for_event()`` on the listener side. In the workflow, once an HTTP event is received, the event task will return the value of the ``event_payload`` field.

In summary, to trigger an HTTP event in the workflow, an external client should have:

* the HTTP endpoint address (e.g. `http://127.0.0.1:8000/event/send_event`)
* the ``workflow_id`` (e.g. "workflow_receive_event_by_http")
* a valid JSON formatted message with the fields ``event_key`` and ``event_payload``, where ``event_key`` matches the one used in the workflow

The HTTP request will receive a reply once the event has been received by the workflow. The returned status code can be:

1. 200: event was successfully processed.
2. 500: event processing failed.
3. 404: either ``workflow_id`` or ``event_key`` cannot be found, likely due to event is received before the targeted workflow task is ready.

The code snippet below shows an example of the external client sending an HTTP request.

.. literalinclude:: ./doc_code/wait_for_event_http.py
   :language: python
   :start-after: __submit_event_begin__
   :end-before: __submit_event_end__

Custom event listeners
----------------------

Custom event listeners can be written by subclassing the EventListener interface.

.. code-block:: python

    class EventListener:
        def __init__(self):
            """Optional constructor. Only the constructor with no arguments will be
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


The `event_checkpointed` routine can be overridden to support systems with exactly-once delivery semantics which typically follows a pattern of:

1. Wait for an event.
2. Process the event.
3. Commit the event.

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
