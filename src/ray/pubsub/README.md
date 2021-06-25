# Pubsub module

The doc is written on June 9th 2021. The implementation can be changed in any
time, and the documentation could be out of date.

## Motivation

Ray built a lightweight generalized pubsub module to fix a problem
https://github.com/ray-project/ray/issues/14762.

Ray has required several different long-polling GRPC requests for some
protocols. For example, imagine the reference counting protocol. The owner
needs to "wait" until the borrower reports that the reference has gone out of
scope. It is used to be done by sending a single long-polling request "per
object". For example, the borrower replies to the owner's
`WaitForRefRemoved(objectID)` RPC when the ref is gone out of scope.

It causes a excessive memory usage when many objects need to be tracked because
GRPC uses big memory when requests are not replied.

The pubsub module is developed to mitigate this problem. It has a publisher
(borrower) and a subscriber (owner). Instead of caching individual RPCs for a
long time, it batches a message and publish back to the subscriber. In this
way, we could reduce the number of connections from O(#objects) to
O(#subscribers). Since the number of subscribers are less than 10,000 in Ray
most of the time, this could drastically reduce the memory usage in this
situation.

## Terminology

- Publisher: A process that publishes messages to subscribers.
- Subscriber: A process that subscribes channels from publishers.
- Channel: Equivalent to topic in Kafka.
- Command: Equivalent to Redis pubsub's command. E.g., Subscribe / Unsubscribe.

## Features

- All Subscribe and Unsubscribe commands are properly ordered and delivered to
  the publisher in FIFO order.
- All publishes messages are delivered to subscribers subscribing the channel
  in the FIFO order.
- Last commands win. The state of the publisher / subscriber is equivalent to
  the last command that's called.
- Publisher failiure detection. The publisher failure is detected by
  subscribers.
- Subscriber failure detection. The subscriber failure is tracked by
  publishers.
- The module is general and can be used in arbitrary two core ray components.

## Limitation

- If messages are published before it is subscribed from the publisher, they
  are lost.
- It doesn't handle the fault tolerance by design because raylet -> core_worker
  (the most common use case) doesn't require it. The fault tolerance needs to
  be implemented in the higher layer.

## Implementation

The pubsub module doesn't have a broker like traditional pubsub systems because
there's no use case. In the pubsub module, all publishers are also brokers. The
performance, especially a throughput is not a requirement when developed, and
the module is not designed for high throughput.

### Basic mechanism

Between the publisher and subscriber, there's only 1 long-polling connection.
The long polling connection is initiated from the subscriber when there are
subscribing entries from the publisher. Whenever publisher publishes messages,
they are batched to the reply of the long polling request in FIFO order.

### Commands

A command is an operation from a subscriber to publisher. For example,
Subscribe or Unsubscribe could be a command. Commands are served by a separate
RPC, which also batches them in the FIFO order. Subscriber keeps sending
commands until they are not queued. There's no backpressure mechanism here.

### Fault detection

Fault detection needed to be implemented in the component-agonistic manner, so
it doesn't use Ray's GCS for that.

Subscriber detects the publisher failures from the long polling request. A
single long polling request is initiated from the subscriber, and it sends them
again and again whenever replied as long as there are subscribing entreis. If
the publisher fails, the long polling request is also failed, so that the
subscriber can detect the failures of publishers. All metadata is cleaned up in
this case.

Publishers always have received long polling request from a subscriber as long
as there are subscribing entries from them. If subscribers are failed, they are
not sending any more long polling requests. Publishers refreshes the long
polling request every 30 seconds to check if the subscriber is still alive. If
the subscriber doesn't initiate a long polling request for more than certain
threshold, subscriber is condiered to be failed and all metadata is cleaned up.
