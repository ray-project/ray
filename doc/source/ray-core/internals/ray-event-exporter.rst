.. _ray-event-exporter:

Ray Event Exporter Infrastructure
==================================

This document is based on Ray version 2.52.1.

Ray's event exporting infrastructure collects events from C++ components (GCS, workers) and Python components, buffers and merges them, and exports them to external HTTP services. This document explains how events flow through the system from creation to final export.

Architecture Overview
---------------------

Ray's event system uses a multi-stage pipeline:

1. **C++ Components**: GCS and worker processes create events implementing `RayEventInterface <https://github.com/ray-project/ray/blob/4ebdc0abe5e5a551625fe7f87053c7e668a6ff74/src/ray/observability/ray_event_interface.h#L24>`__. Raylet does not emit any Ray events, but there are no technical limitations preventing it from doing so.
2. **Event Buffering**: Events are buffered in a bounded circular buffer
3. **Event Merging**: Events with the same entity ID and type are merged before export
4. **gRPC Export**: Events are exported via gRPC to the aggregator agent
5. **Python Aggregation**: The `AggregatorAgent <https://github.com/ray-project/ray/blob/4ebdc0abe5e5a551625fe7f87053c7e668a6ff74/python/ray/dashboard/modules/aggregator/aggregator_agent.py>`__ receives and buffers events
6. **HTTP Publishing**: Events are filtered, converted to JSON, and published to external HTTP services

The following diagram shows the high-level flow:

.. code-block:: text

  C++ Components (GCS, workers)
    ↓ (Create events via RayEventInterface)
  RayEventRecorder (C++)
    ↓ (Buffer & merge events)
    ↓ (gRPC export via EventAggregatorClient)
  AggregatorAgent (Python)
    ↓ (Add to MultiConsumerEventBuffer)
  RayEventPublisher
    ↓ (Filter & convert to JSON)
    ↓ (HTTP POST)
  External HTTP Service

Event Types and Structure
-------------------------

Ray events are structured using protobuf messages with a base `RayEvent` message that contains event-specific nested messages.

Event Types
~~~~~~~~~~~

Events are categorized by type, defined in the `EventType` enum in `events_base_event.proto <https://github.com/ray-project/ray/blob/4ebdc0abe5e5a551625fe7f87053c7e668a6ff74/src/ray/protobuf/public/events_base_event.proto#L49>`__:

- **TASK_DEFINITION_EVENT**: Task definition information
- **TASK_LIFECYCLE_EVENT**: Task state transitions (this covers both normal tasks and actor tasks)
- **ACTOR_TASK_DEFINITION_EVENT**: Actor task definition
- **ACTOR_DEFINITION_EVENT**: Actor definition
- **ACTOR_LIFECYCLE_EVENT**: Actor state transitions
- **DRIVER_JOB_DEFINITION_EVENT**: Driver job definition
- **DRIVER_JOB_LIFECYCLE_EVENT**: Driver job state transitions
- **NODE_DEFINITION_EVENT**: Node definition
- **NODE_LIFECYCLE_EVENT**: Node state transitions
- **TASK_PROFILE_EVENT**: Task profiling data

Event Structure
~~~~~~~~~~~~~~~

The base `RayEvent <https://github.com/ray-project/ray/blob/4ebdc0abe5e5a551625fe7f87053c7e668a6ff74/src/ray/protobuf/public/events_base_event.proto#L32>`__ message contains:

- **event_id**: Unique identifier for the event
- **source_type**: Component that generated the event
- **event_type**: Type of event (from EventType enum)
- **timestamp**: When the event was created
- **severity**: Event severity level (TRACE, DEBUG, INFO, WARNING, ERROR, FATAL)
- **message**: Optional string message
- **session_name**: Ray session identifier
- **Nested event messages**: One of the event-specific messages (e.g., `task_definition_event`, `actor_lifecycle_event`)

Entity ID Concept
~~~~~~~~~~~~~~~~~

The entity ID is a unique identifier for the entity associated with an event. It's used for two purposes:

1. **Association**: Links execution events with definition events (e.g., task lifecycle events with task definition events)
2. **Merging**: Groups events with the same entity ID and type for merging before export

For example:
- Task events use `task_id + task_attempt` as the entity ID
- Actor events use `actor_id` as the entity ID
- Driver job events use `job_id` as the entity ID

Event Recording and Buffering (C++ Side)
-----------------------------------------

C++ components record events through the `RayEventRecorder <https://github.com/ray-project/ray/blob/4ebdc0abe5e5a551625fe7f87053c7e668a6ff74/src/ray/observability/ray_event_recorder.h>`__ class, which provides thread-safe event buffering and export.

RayEventRecorder
~~~~~~~~~~~~~~~~

The `RayEventRecorder` is a thread-safe event recorder that:

- Maintains a bounded circular buffer for events
- Merges events with the same entity ID and type before export
- Periodically exports events via gRPC to the aggregator agent using `EventAggregatorClient <https://github.com/ray-project/ray/blob/4ebdc0abe5e5a551625fe7f87053c7e668a6ff74/src/ray/rpc/event_aggregator_client.h>`__
- Tracks dropped events when the buffer is full

Adding Events
~~~~~~~~~~~~~

Events are added to the recorder via the `AddEvents() <https://github.com/ray-project/ray/blob/4ebdc0abe5e5a551625fe7f87053c7e668a6ff74/src/ray/observability/ray_event_recorder.cc#L92>`__ method, which accepts a vector of `RayEventInterface` pointers. The method:

1. Checks if event recording is enabled (via `enable_ray_event` config)
2. Calculates if adding events would exceed the buffer size
3. Drops old events if necessary and records metrics for dropped events
4. Adds new events to the circular buffer

Buffer Management
~~~~~~~~~~~~~~~~~

The recorder uses a `boost::circular_buffer <https://github.com/ray-project/ray/blob/4ebdc0abe5e5a551625fe7f87053c7e668a6ff74/src/ray/observability/ray_event_recorder.h#L66>`__ to store events. When the buffer is full:

- Oldest events are dropped to make room for new ones
- Dropped events are tracked via the `dropped_events_counter` metric
- The metric includes the source component name for tracking
- The default buffer size is 10,000 events, but it can be configured via the `RAY_ray_event_recorder_max_queued_events` environment variable

Event Export from C++ (gRPC)
------------------------------

Events are exported from C++ components to the aggregator agent using gRPC. The export process is initiated by calling `StartExportingEvents() <https://github.com/ray-project/ray/blob/4ebdc0abe5e5a551625fe7f87053c7e668a6ff74/src/ray/observability/ray_event_recorder.cc#L37>`__.

StartExportingEvents
~~~~~~~~~~~~~~~~~~~~

This method:

1. Checks if event recording is enabled
2. Verifies it hasn't been called before (should only be called once)
3. Sets up a `PeriodicalRunner` to periodically call `ExportEvents()`
4. Uses the configured export interval (`ray_events_report_interval_ms`)

ExportEvents Process
~~~~~~~~~~~~~~~~~~~~

The `ExportEvents() <https://github.com/ray-project/ray/blob/4ebdc0abe5e5a551625fe7f87053c7e668a6ff74/src/ray/observability/ray_event_recorder.cc#L52>`__ method performs the following steps:

1. **Check Buffer**: Returns early if the buffer is empty
2. **Group Events**: Groups events by entity ID and type using a hash map
3. **Merge Events**: Events with the same key are merged using the `Merge() <https://github.com/ray-project/ray/blob/4ebdc0abe5e5a551625fe7f87053c7e668a6ff74/src/ray/observability/ray_event_interface.h#L55>`__ method
4. **Serialize**: Each merged event is serialized to a `RayEvent` protobuf via `Serialize() <https://github.com/ray-project/ray/blob/4ebdc0abe5e5a551625fe7f87053c7e668a6ff74/src/ray/observability/ray_event_interface.h#L58>`__
5. **Send via gRPC**: Events are sent to the aggregator agent via `EventAggregatorClient::AddEvents() <https://github.com/ray-project/ray/blob/4ebdc0abe5e5a551625fe7f87053c7e668a6ff74/src/ray/rpc/event_aggregator_client.h>`__
6. **Clear Buffer**: The buffer is cleared after successful export

Event Merging Logic
~~~~~~~~~~~~~~~~~~~

Event merging is an optimization that reduces data size by combining related events. Events with the same entity ID and type are merged:

- **Definition Events**: Typically don't change when merged (e.g., actor definition)
- **Lifecycle Events**: State transitions are appended to form a time series (e.g., task state transitions: started → running → completed)

The merging maintains the order of events while combining them into a single event with all state transitions.

Error Handling
~~~~~~~~~~~~~~

If the gRPC export fails:

- An error is logged
- The process continues (doesn't crash)
- The next export interval will attempt to send events again
- Events remain in the buffer until successfully exported (or the buffer is full and old events are dropped)

Event Reception and Buffering (Python Side)
---------------------------------------------

The `AggregatorAgent <https://github.com/ray-project/ray/blob/4ebdc0abe5e5a551625fe7f87053c7e668a6ff74/python/ray/dashboard/modules/aggregator/aggregator_agent.py>`__ receives events from C++ components via a gRPC service and buffers them for publishing.

AggregatorAgent
~~~~~~~~~~~~~~~

The `AggregatorAgent` is a dashboard agent module that:

- Implements `EventAggregatorServiceServicer` for gRPC event reception
- Maintains a `MultiConsumerEventBuffer` for event storage
- Manages `RayEventPublisher` instances for publishing to external http endpoints
- Tracks metrics for events received, buffer and publisher operations

AddEvents gRPC Handler
~~~~~~~~~~~~~~~~~~~~~~~

The `AddEvents() <https://github.com/ray-project/ray/blob/4ebdc0abe5e5a551625fe7f87053c7e668a6ff74/python/ray/dashboard/modules/aggregator/aggregator_agent.py#L165>`__ method is the gRPC handler that receives events:

1. Checks if event processing is enabled
2. Iterates through events in the request
3. Records metrics for each received event
4. Adds each event to the `MultiConsumerEventBuffer` via `add_event() <https://github.com/ray-project/ray/blob/4ebdc0abe5e5a551625fe7f87053c7e668a6ff74/python/ray/dashboard/modules/aggregator/multi_consumer_event_buffer.py#L62>`__
5. Handles errors if adding events fails

MultiConsumerEventBuffer
~~~~~~~~~~~~~~~~~~~~~~~~~

The `MultiConsumerEventBuffer <https://github.com/ray-project/ray/blob/4ebdc0abe5e5a551625fe7f87053c7e668a6ff74/python/ray/dashboard/modules/aggregator/multi_consumer_event_buffer.py>`__ is an asyncio-friendly buffer that:

- **Supports Multiple Consumers**: Each consumer has an independent cursor index. RayEventPublisher and other consumers share this same buffer.
- **Tracks Evictions**: When the buffer is full, oldest events are dropped and tracked per consumer
- **Bounded Buffer**: Uses `deque` with `maxlen` to limit buffer size
- **Asyncio-Safe**: Uses `asyncio.Lock` and `asyncio.Condition` for synchronization

Key operations:

- **add_event()**: Adds an event to the buffer, dropping oldest if full
- **wait_for_batch()**: Waits for a batch of events up to `max_batch_size`, with timeout. The timeout only applies when there is at least one event in the buffer. If the buffer is empty, `wait_for_batch()` can block indefinitely.
- **register_consumer()**: Registers a new consumer with a unique name

Event Filtering
~~~~~~~~~~~~~~~

The agent checks if events can be exposed to external services via `_can_expose_event() <https://github.com/ray-project/ray/blob/4ebdc0abe5e5a551625fe7f87053c7e668a6ff74/python/ray/dashboard/modules/aggregator/aggregator_agent.py#L195>`__. Only events whose type is in the `EXPOSABLE_EVENT_TYPES` set are allowed to be published externally.

Event Publishing to HTTP
------------------------

Events are published to external HTTP services by the `RayEventPublisher`, which reads from the event buffer and sends HTTP POST requests.

RayEventPublisher
~~~~~~~~~~~~~~~~~

The `RayEventPublisher <https://github.com/ray-project/ray/blob/4ebdc0abe5e5a551625fe7f87053c7e668a6ff74/python/ray/dashboard/modules/aggregator/publisher/ray_event_publisher.py>`__ runs a worker loop that:

1. Registers as a consumer of the `MultiConsumerEventBuffer`
2. Continuously waits for batches of events via `wait_for_batch()`
3. Publishes batches using the configured `PublisherClientInterface`
4. Handles retries with exponential backoff on failures
5. Records metrics for publish success, failures, and latency

The publisher runs in an async context and uses `asyncio` for non-blocking operations.

AsyncHttpPublisherClient
~~~~~~~~~~~~~~~~~~~~~~~~~

The `AsyncHttpPublisherClient <https://github.com/ray-project/ray/blob/4ebdc0abe5e5a551625fe7f87053c7e668a6ff74/python/ray/dashboard/modules/aggregator/publisher/async_publisher_client.py#L60>`__ handles HTTP publishing:

1. **Event Filtering**: Filters events using `events_filter_fn` (typically `_can_expose_event`)
2. **JSON Conversion**: Converts protobuf events to JSON dictionaries
   - Uses `message_to_json()` from protobuf
   - Optionally preserves proto field names or converts to camelCase
   - Runs in `ThreadPoolExecutor` to avoid blocking the event loop
3. **HTTP POST**: Sends filtered events as JSON to the configured endpoint
4. **Error Handling**: Catches exceptions and returns failure status
5. **Session Management**: Uses `aiohttp.ClientSession` for HTTP requests

Batch Publishing
~~~~~~~~~~~~~~~~

Events are published in batches:

- Batch size is limited by `max_batch_size` (default: 10,000 events)
- Batches are created by `wait_for_batch()` which waits up to a timeout for events
- Larger batches reduce HTTP request overhead but increase latency

Retry Logic
~~~~~~~~~~~

The publisher implements retry logic with exponential backoff:

- Retries failed publishes up to `max_retries` times (default: infinite)
- Uses exponential backoff with jitter between retries
- If max retries are exhausted, we drop the events and record a metric for dropped events

Configuration
~~~~~~~~~~~~~

HTTP publishing is configured via environment variables:

- **RAY_DASHBOARD_AGGREGATOR_AGENT_EVENTS_EXPORT_ADDR**: HTTP endpoint URL (e.g., `http://localhost:8080/events`)
- **RAY_DASHBOARD_AGGREGATOR_AGENT_EXPOSABLE_EVENT_TYPES**: Comma-separated list of event types to expose
- **RAY_DASHBOARD_AGGREGATOR_AGENT_PUBLISH_EVENTS_TO_EXTERNAL_HTTP_SERVICE**: Enable/disable flag (default: True)

Creating New Event Types
-------------------------

To create a new event type, follow these steps:

Step 1: Define Protobuf Message
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Create a new `.proto` file in `src/ray/protobuf/public/` following the naming convention `events_<name>_event.proto`. For example, see `events_task_definition_event.proto <https://github.com/ray-project/ray/blob/4ebdc0abe5e5a551625fe7f87053c7e668a6ff74/src/ray/protobuf/public/events_task_definition_event.proto>`__.

Define your event-specific message with the fields you need:

.. code-block:: protobuf

  syntax = "proto3";
  package ray.rpc.events;
  
  message MyNewEvent {
    // Define your event-specific fields here
    string entity_id = 1;
    // ... other fields
  }

Step 2: Add to Base Event
~~~~~~~~~~~~~~~~~~~~~~~~~~

Update `events_base_event.proto <https://github.com/ray-project/ray/blob/4ebdc0abe5e5a551625fe7f87053c7e668a6ff74/src/ray/protobuf/public/events_base_event.proto>`__:

1. Add import for your new proto file
2. Add new `EventType` enum value (e.g., `MY_NEW_EVENT = 11`)
3. Add new field to `RayEvent` message (e.g., `MyNewEvent my_new_event = 18`)

Step 3: Implement RayEventInterface
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Create a C++ class that implements `RayEventInterface <https://github.com/ray-project/ray/blob/4ebdc0abe5e5a551625fe7f87053c7e668a6ff74/src/ray/observability/ray_event_interface.h>`__. The easiest approach is to extend `RayEvent<T>` template class, as shown in `ray_actor_definition_event.h <https://github.com/ray-project/ray/blob/4ebdc0abe5e5a551625fe7f87053c7e668a6ff74/src/ray/observability/ray_actor_definition_event.h>`__.

You need to implement:

- **GetEntityId()**: Return a unique identifier for the entity (e.g., task ID + attempt, actor ID)
- **MergeData()**: Implement merging logic for events with the same entity ID
  - Definition events typically don't change when merged
  - Lifecycle events append state transitions
- **SerializeData()**: Convert the event data to a `RayEvent` protobuf
- **GetEventType()**: Return the `EventType` enum value for this event

See `ray_actor_definition_event.cc <https://github.com/ray-project/ray/blob/4ebdc0abe5e5a551625fe7f87053c7e668a6ff74/src/ray/observability/ray_actor_definition_event.cc>`__ for a complete example.

Step 4: Update Exposable Event Types (if needed)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If your event should be exposed to external HTTP services, add it to `DEFAULT_EXPOSABLE_EVENT_TYPES <https://github.com/ray-project/ray/blob/4ebdc0abe5e5a551625fe7f87053c7e668a6ff74/python/ray/dashboard/modules/aggregator/aggregator_agent.py#L56>`__ in `aggregator_agent.py`. Alternatively, users can configure it via the `RAY_DASHBOARD_AGGREGATOR_AGENT_EXPOSABLE_EVENT_TYPES` environment variable.

Step 5: Update RayEventRecorder to publish your new event type
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Use `RayEventRecorder::AddEvent() <https://github.com/ray-project/ray/blob/4ebdc0abe5e5a551625fe7f87053c7e668a6ff74/src/ray/observability/ray_event_recorder.cc#L92>`__ to add your new event type to the buffer.

Step 6: Update AggregatorAgent to publish your new event type
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Update `AggregatorAgent <https://github.com/ray-project/ray/blob/4ebdc0abe5e5a551625fe7f87053c7e668a6ff74/python/ray/dashboard/modules/aggregator/aggregator_agent.py#L56>`__ to publish your new event type.
