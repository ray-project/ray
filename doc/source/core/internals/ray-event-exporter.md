---
myst:
  html_meta:
    description: "Internals of Ray's event exporter: event types and structure, the entity ID concept, and C++-side event recording and buffering."
---

(ray-event-exporter)=

# Ray event exporter infrastructure

This page describes Ray version 2.52.1.

Ray's event exporting infrastructure collects events from C++ components, such as the GCS and worker processes, and from Python components. It buffers and merges the events, then exports them to external HTTP services. This page explains how events flow through the system from creation to final export.

## Architecture overview

Ray's event system uses a multi-stage pipeline:

1. **C++ components**: The GCS and worker processes create events that implement [RayEventInterface](https://github.com/ray-project/ray/blob/4ebdc0abe5e5a551625fe7f87053c7e668a6ff74/src/ray/observability/ray_event_interface.h#L24). The raylet doesn't emit any Ray events, but no technical limitation prevents it from doing so.
1. **Event buffering**: A bounded circular buffer holds the events.
1. **Event merging**: Ray merges events with the same entity ID and type before export.
1. **gRPC export**: Ray exports the events through gRPC to the aggregator agent.
1. **Python aggregation**: The [AggregatorAgent](https://github.com/ray-project/ray/blob/4ebdc0abe5e5a551625fe7f87053c7e668a6ff74/python/ray/dashboard/modules/aggregator/aggregator_agent.py) receives and buffers the events.
1. **HTTP publishing**: Ray filters the events, converts them to JSON, and publishes them to external HTTP services.

The following diagram shows the high-level flow:

```text
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
```

## Event types and structure

Ray defines events as protobuf messages, with a base `RayEvent` message that contains event-specific nested messages.

### Event types

Each event has a type. The `EventType` enum in [events_base_event.proto](https://github.com/ray-project/ray/blob/4ebdc0abe5e5a551625fe7f87053c7e668a6ff74/src/ray/protobuf/public/events_base_event.proto#L49) defines the following types:

- **TASK_DEFINITION_EVENT**: Task definition information
- **TASK_LIFECYCLE_EVENT**: Task state transitions for both normal tasks and actor tasks
- **ACTOR_TASK_DEFINITION_EVENT**: Actor task definition
- **ACTOR_DEFINITION_EVENT**: Actor definition
- **ACTOR_LIFECYCLE_EVENT**: Actor state transitions
- **DRIVER_JOB_DEFINITION_EVENT**: Driver job definition
- **DRIVER_JOB_LIFECYCLE_EVENT**: Driver job state transitions
- **NODE_DEFINITION_EVENT**: Node definition
- **NODE_LIFECYCLE_EVENT**: Node state transitions
- **TASK_PROFILE_EVENT**: Task profiling data

### Event structure

The base [RayEvent](https://github.com/ray-project/ray/blob/4ebdc0abe5e5a551625fe7f87053c7e668a6ff74/src/ray/protobuf/public/events_base_event.proto#L32) message contains the following fields:

- **event_id**: Unique identifier for the event
- **source_type**: Component that generated the event
- **event_type**: Type of event, from the `EventType` enum
- **timestamp**: Time of event creation
- **severity**: Event severity level, one of `TRACE`, `DEBUG`, `INFO`, `WARNING`, `ERROR`, or `FATAL`
- **message**: Optional string message
- **session_name**: Ray session identifier
- **Nested event messages**: One of the event-specific messages, such as `task_definition_event` or `actor_lifecycle_event`

### Entity ID concept

The entity ID uniquely identifies the entity associated with an event. Ray uses it for two purposes:

1. **Association**: Links execution events with definition events. For example, it links task lifecycle events with task definition events.
1. **Merging**: Groups events with the same entity ID and type for merging before export.

Each kind of entity uses its own entity ID, as the following examples show:

- Task events use `task_id + task_attempt` as the entity ID.
- Actor events use `actor_id` as the entity ID.
- Driver job events use `job_id` as the entity ID.

## Event recording and buffering (C++ side)

C++ components record events through the [RayEventRecorder](https://github.com/ray-project/ray/blob/4ebdc0abe5e5a551625fe7f87053c7e668a6ff74/src/ray/observability/ray_event_recorder.h) class, which provides thread-safe event buffering and export.

### RayEventRecorder

The `RayEventRecorder` is a thread-safe event recorder that does the following:

- Maintains a bounded circular buffer for events.
- Merges events with the same entity ID and type before export.
- Periodically exports events through gRPC to the aggregator agent using [EventAggregatorClient](https://github.com/ray-project/ray/blob/4ebdc0abe5e5a551625fe7f87053c7e668a6ff74/src/ray/rpc/event_aggregator_client.h).
- Tracks dropped events when the buffer is full.

### Adding events

C++ components add events to the recorder through the [AddEvents()](https://github.com/ray-project/ray/blob/4ebdc0abe5e5a551625fe7f87053c7e668a6ff74/src/ray/observability/ray_event_recorder.cc#L92) method, which accepts a vector of `RayEventInterface` pointers. The method does the following:

1. Checks whether event recording is enabled through the `enable_ray_event` config.
1. Calculates whether adding the events would exceed the buffer size.
1. Drops old events if necessary and records metrics for dropped events.
1. Adds the new events to the circular buffer.

### Buffer management

The recorder stores events in a [boost::circular_buffer](https://github.com/ray-project/ray/blob/4ebdc0abe5e5a551625fe7f87053c7e668a6ff74/src/ray/observability/ray_event_recorder.h#L66). When the buffer is full, the following happens:

- The recorder drops the oldest events to make room for new ones.
- The `dropped_events_counter` metric tracks the dropped events.
- The metric includes the source component name for tracking.

The default buffer size is 10,000 events. To change it, set the `RAY_ray_event_recorder_max_queued_events` environment variable.

## Event export from C++ (gRPC)

C++ components export events to the aggregator agent through gRPC. A call to [StartExportingEvents()](https://github.com/ray-project/ray/blob/4ebdc0abe5e5a551625fe7f87053c7e668a6ff74/src/ray/observability/ray_event_recorder.cc#L37) starts the export process.

### StartExportingEvents

The `StartExportingEvents()` method does the following:

1. Checks whether event recording is enabled.
1. Verifies that this is the first call. Call it only once.
1. Sets up a `PeriodicalRunner` to call `ExportEvents()` periodically.
1. Uses the configured export interval, `ray_events_report_interval_ms`.

### ExportEvents process

The [ExportEvents()](https://github.com/ray-project/ray/blob/4ebdc0abe5e5a551625fe7f87053c7e668a6ff74/src/ray/observability/ray_event_recorder.cc#L52) method performs the following steps:

1. **Check buffer**: Returns early if the buffer is empty.
1. **Group events**: Groups events by entity ID and type using a hash map.
1. **Merge events**: Merges events with the same key using the [Merge()](https://github.com/ray-project/ray/blob/4ebdc0abe5e5a551625fe7f87053c7e668a6ff74/src/ray/observability/ray_event_interface.h#L55) method.
1. **Serialize**: Serializes each merged event to a `RayEvent` protobuf through [Serialize()](https://github.com/ray-project/ray/blob/4ebdc0abe5e5a551625fe7f87053c7e668a6ff74/src/ray/observability/ray_event_interface.h#L58).
1. **Send through gRPC**: Sends the events to the aggregator agent through [EventAggregatorClient::AddEvents()](https://github.com/ray-project/ray/blob/4ebdc0abe5e5a551625fe7f87053c7e668a6ff74/src/ray/rpc/event_aggregator_client.h).
1. **Clear buffer**: Clears the buffer after a successful export.

### Event merging logic

Event merging is an optimization that reduces data size by combining related events. Ray merges events with the same entity ID and type as follows:

- **Definition events**: Typically don't change when merged. Actor definition events are one example.
- **Lifecycle events**: Merging appends state transitions to form a time series. For example, a task's state transitions from started to running to completed.

Merging preserves the order of events while it combines them into a single event with all state transitions.

### Error handling

If the gRPC export fails, the following happens:

- The recorder logs an error.
- The process continues and doesn't crash.
- The next export interval attempts to send the events again.
- Events remain in the buffer until an export succeeds. If the buffer fills first, the recorder drops the oldest events.

## Event reception and buffering (Python side)

The [AggregatorAgent](https://github.com/ray-project/ray/blob/4ebdc0abe5e5a551625fe7f87053c7e668a6ff74/python/ray/dashboard/modules/aggregator/aggregator_agent.py) receives events from C++ components through a gRPC service and buffers them for publishing.

### AggregatorAgent

The `AggregatorAgent` is a dashboard agent module that does the following:

- Implements `EventAggregatorServiceServicer` for gRPC event reception.
- Maintains a `MultiConsumerEventBuffer` for event storage.
- Manages `RayEventPublisher` instances for publishing to external HTTP endpoints.
- Tracks metrics for received events, buffer operations, and publisher operations.

### AddEvents gRPC handler

The [AddEvents()](https://github.com/ray-project/ray/blob/4ebdc0abe5e5a551625fe7f87053c7e668a6ff74/python/ray/dashboard/modules/aggregator/aggregator_agent.py#L165) method is the gRPC handler that receives events. It does the following:

1. Checks whether event processing is enabled.
1. Iterates through the events in the request.
1. Records metrics for each received event.
1. Adds each event to the `MultiConsumerEventBuffer` through [add_event()](https://github.com/ray-project/ray/blob/4ebdc0abe5e5a551625fe7f87053c7e668a6ff74/python/ray/dashboard/modules/aggregator/multi_consumer_event_buffer.py#L62).
1. Handles errors if adding events fails.

### MultiConsumerEventBuffer

The [MultiConsumerEventBuffer](https://github.com/ray-project/ray/blob/4ebdc0abe5e5a551625fe7f87053c7e668a6ff74/python/ray/dashboard/modules/aggregator/multi_consumer_event_buffer.py) is an asyncio-friendly buffer with the following properties:

- **Supports multiple consumers**: Each consumer has an independent cursor index. `RayEventPublisher` and other consumers share this buffer.
- **Tracks evictions**: When the buffer is full, it drops the oldest events and tracks the evictions per consumer.
- **Bounded buffer**: Uses `deque` with `maxlen` to limit the buffer size.
- **Safe for asyncio**: Uses `asyncio.Lock` and `asyncio.Condition` for synchronization.

The buffer provides the following key operations:

- **add_event()**: Adds an event to the buffer, dropping the oldest event if the buffer is full.
- **wait_for_batch()**: Waits for a batch of events up to `max_batch_size`, with a timeout. The timeout applies only when the buffer holds at least one event. If the buffer is empty, `wait_for_batch()` can block indefinitely.
- **register_consumer()**: Registers a new consumer with a unique name.

### Event filtering

The agent checks whether it can expose an event to external services through [_can_expose_event()](https://github.com/ray-project/ray/blob/4ebdc0abe5e5a551625fe7f87053c7e668a6ff74/python/ray/dashboard/modules/aggregator/aggregator_agent.py#L195). Ray publishes externally only the events whose type is in the `EXPOSABLE_EVENT_TYPES` set.

## Event publishing to HTTP

The `RayEventPublisher` publishes events to external HTTP services. It reads from the event buffer and sends HTTP POST requests.

### RayEventPublisher

The [RayEventPublisher](https://github.com/ray-project/ray/blob/4ebdc0abe5e5a551625fe7f87053c7e668a6ff74/python/ray/dashboard/modules/aggregator/publisher/ray_event_publisher.py) runs a worker loop that does the following:

1. Registers as a consumer of the `MultiConsumerEventBuffer`.
1. Continuously waits for batches of events through `wait_for_batch()`.
1. Publishes batches using the configured `PublisherClientInterface`.
1. Handles retries with exponential backoff on failures.
1. Records metrics for publish successes, failures, and latency.

The publisher runs in an async context and uses `asyncio` for non-blocking operations.

### AsyncHttpPublisherClient

The [AsyncHttpPublisherClient](https://github.com/ray-project/ray/blob/4ebdc0abe5e5a551625fe7f87053c7e668a6ff74/python/ray/dashboard/modules/aggregator/publisher/async_publisher_client.py#L60) handles HTTP publishing in the following steps:

1. **Event filtering**: Filters events using `events_filter_fn`, typically `_can_expose_event`.
1. **JSON conversion**: Converts protobuf events to JSON dictionaries.
   - Uses `message_to_json()` from protobuf.
   - Optionally preserves proto field names or converts them to camelCase.
   - Runs in `ThreadPoolExecutor` to avoid blocking the event loop.
1. **HTTP POST**: Sends filtered events as JSON to the configured endpoint.
1. **Error handling**: Catches exceptions and returns a failure status.
1. **Session management**: Uses `aiohttp.ClientSession` for HTTP requests.

### Batch publishing

The publisher sends events in batches, which work as follows:

- `max_batch_size` limits the batch size. The default is 10,000 events.
- `wait_for_batch()` creates the batches. It waits up to a timeout for events.
- Larger batches reduce HTTP request overhead but increase latency.

### Retry logic

The publisher implements retry logic with exponential backoff, as follows:

- Retries a failed publish up to `max_retries` times. The default is infinite.
- Uses exponential backoff with jitter between retries.
- If it exhausts the retries, it drops the events and records a metric for dropped events.

### Configuration

Configure HTTP publishing with the following environment variables:

- **RAY_DASHBOARD_AGGREGATOR_AGENT_EVENTS_EXPORT_ADDR**: HTTP endpoint URL, for example `http://localhost:8080/events`.
- **RAY_DASHBOARD_AGGREGATOR_AGENT_EXPOSABLE_EVENT_TYPES**: Comma-separated list of event types to expose. Set it to `ALL` to expose every event type. Ray 2.54.0 and later support `ALL`.
- **RAY_DASHBOARD_AGGREGATOR_AGENT_PUBLISH_EVENTS_TO_EXTERNAL_HTTP_SERVICE**: Flag that enables or disables publishing to external HTTP services. The default is `True`.

## Creating new event types

To create a new event type, complete the following steps.

### Step 1: Define protobuf message

Create a new `.proto` file in `src/ray/protobuf/public/` that follows the naming convention `events_<name>_event.proto`. For an example, see [events_task_definition_event.proto](https://github.com/ray-project/ray/blob/4ebdc0abe5e5a551625fe7f87053c7e668a6ff74/src/ray/protobuf/public/events_task_definition_event.proto).

Define your event-specific message with the fields you need:

```protobuf
syntax = "proto3";
package ray.rpc.events;

message MyNewEvent {
  // Define your event-specific fields here
  string entity_id = 1;
  // ... other fields
}
```

### Step 2: Add to base event

Update [events_base_event.proto](https://github.com/ray-project/ray/blob/4ebdc0abe5e5a551625fe7f87053c7e668a6ff74/src/ray/protobuf/public/events_base_event.proto) as follows:

1. Add an import for your new proto file.
1. Add a new `EventType` enum value, such as `MY_NEW_EVENT = 11`.
1. Add a new field to the `RayEvent` message, such as `MyNewEvent my_new_event = 18`.

### Step 3: Implement RayEventInterface

Create a C++ class that implements [RayEventInterface](https://github.com/ray-project/ray/blob/4ebdc0abe5e5a551625fe7f87053c7e668a6ff74/src/ray/observability/ray_event_interface.h). The simplest approach is to extend the `RayEvent<T>` template class, as shown in [ray_actor_definition_event.h](https://github.com/ray-project/ray/blob/4ebdc0abe5e5a551625fe7f87053c7e668a6ff74/src/ray/observability/ray_actor_definition_event.h).

Implement the following methods:

- **GetEntityId()**: Return a unique identifier for the entity, such as the task ID plus the attempt, or the actor ID.
- **MergeData()**: Implement merging logic for events with the same entity ID.
  - Definition events typically don't change when merged.
  - Lifecycle events append state transitions.
- **SerializeData()**: Convert the event data to a `RayEvent` protobuf.
- **GetEventType()**: Return the `EventType` enum value for this event.

See [ray_actor_definition_event.cc](https://github.com/ray-project/ray/blob/4ebdc0abe5e5a551625fe7f87053c7e668a6ff74/src/ray/observability/ray_actor_definition_event.cc) for a complete example.

### Step 4: Update exposable event types (if needed)

To expose your event to external HTTP services, add it to [DEFAULT_EXPOSABLE_EVENT_TYPES](https://github.com/ray-project/ray/blob/4ebdc0abe5e5a551625fe7f87053c7e668a6ff74/python/ray/dashboard/modules/aggregator/aggregator_agent.py#L56) in `aggregator_agent.py`. Alternatively, configure it through the `RAY_DASHBOARD_AGGREGATOR_AGENT_EXPOSABLE_EVENT_TYPES` environment variable.

### Step 5: Update RayEventRecorder to publish your new event type

Use [RayEventRecorder::AddEvent()](https://github.com/ray-project/ray/blob/4ebdc0abe5e5a551625fe7f87053c7e668a6ff74/src/ray/observability/ray_event_recorder.cc#L92) to add your new event type to the buffer.

### Step 6: Update AggregatorAgent to publish your new event type

Update [AggregatorAgent](https://github.com/ray-project/ray/blob/4ebdc0abe5e5a551625fe7f87053c7e668a6ff74/python/ray/dashboard/modules/aggregator/aggregator_agent.py#L56) to publish your new event type.
