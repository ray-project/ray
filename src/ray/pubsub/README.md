# Pubsub module

This doc has last been updated on Aug 19, 2025. This doc should be updated
as the implementation changes.

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
- Key/Entity: A specific item you care about in the channel. E.g. in
  the actor channel, you only care about a specific actor id so that's
  the key you subscribe to. Not all channels have keys you can subscribe by.
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
- The module is general and can be used in two arbitrary Ray components.

## Limitation

If messages are published before a subscription, they're lost.

## Implementation

In this pubsub implementation, publishers directly send messages to subscribers.
There are no intermediary brokers. The performance, especially throughput
wasn't a requirement when developed, and therefore the module isn't designed
for high throughput.

### Basic mechanism

#### PubsubCommandBatch
A command is an operation from a subscriber to publisher. Subscribe and
Unsubscribe are the only commands. Commands are served by `PubsubCommandBatch`,
which batches them in the FIFO order. We limit to it one in-flight `PubsubCommandBatchRequest`
at a time to prevent out of order subscribes / unsubscribes. Because of this,
we have to queue up commands and therefore have to batch commands when sending them.

#### PubsubLongPolling
Between the publisher and subscriber, there's only 1 long-polling connection
(only one in-flight request), no matter how many separate channels / keys the
subscriber is subscribed to. The subscriber will always have an in-flight
`PubsubLongPollingRequest` as long as it's subscribed to something. Whenever a
publisher publishes messages to that subscriber, they're batched to the reply
of the long polling request in FIFO order.

### Pubsub Code Flow
Breakdown of the pubsub flow from the subscriber and publisher
Note that this section ignores fault tolerance.

#### Subscriber Actions

1. **On a Subscribe call**
   - Sends a `PubsubCommandBatchRequest` with its own `subscriber_id` and a `SubMessage`
     Command containing `channel_type` and optionally `key_id`
   - Sends a `PubsubLongPollingRequest` with its own `subscriber_id`

2. **Subscribe done**
   - Receives `PubsubCommandBatchReply` and runs a callback if provided on subscribe
   - Sends new commands to publisher if they've been queued up, e.g. another subscribe to
     something else or an unsubscribe to something
   - Only allows one in-flight `PubsubCommandBatchRequest` to ensure command ordering

3. **Message Processing**
   - Receives reply to `PubsubLongPollingRequest` and processes published messages
   - Sends another `PubsubLongPollingRequest` if subscription still exists

4. **Unsubscribe**
   - Sends `PubsubCommandBatchRequest` with `UnsubscribeMessage` when unsubscribing

#### Publisher Actions

1. **Subscribe Handling**
   - Receives `PubsubCommandBatchRequest` and creates a `SubscriberState` for the
     subscriber if it doesn't exist
   - Registers subscription for the given channel + key by setting up a relation between
     an `EntityState` and a `SubscriberState`
   - Note that the publisher maintains a `SubscriptionIndex` for each channel, and each
     `SubscriptionIndex` holds `EntityState` objects for each key. Each `EntityState`
     holds `SubscriberState` pointers to send / queue up messages to send. There's a
     special `EntityState` in every `SubscriptionIndex` for "subscribing to all"

2. **Initial Long Polling Request**
   - Receives `PubsubLongPollingRequest` and creates `SubscriberState` if it doesn't exist.
     Note that the `SubscriberState` might not exist because the initial `PubsubLongPollingRequest`
     could arrive before the associated `PubsubCommandBatchRequest`.
   - Creates a `LongPollConnection` in the `SubscriberState` to store the reply + reply callback
   - Attempts to publish by replying to the request if mailbox already contains messages
   - If mailbox is empty, waits until next relevant publish to reply and send the publish

3. **Subsequent Long Polling**
   - Receives a subsequent `PubsubLongPollingRequest` from the subscriber and checks mailbox
   - Publishes messages if mailbox isn't empty, or waits for relevant publish to reply

4. **Unsubscribe**
   - Receives unsubscribe command and unregisters `SubscriberState` from the appropriate
     `EntityState`
   - Erases the `EntityState` if it no longer contains any `SubscriberState` pointers
   - Periodically cleans up "Dead" `SubscriberState`'s


### Fault detection

Both pubsub RPC's will be retried by the client on transient network failures using the
retryable grpc client used by other RPC's throughout.

Subscribing and unsubscribing are idempotent so the `PubsubCommandBatchRequest` can be resent.
Since we restrict it to one in-flight request, the commands will be ordered even with retries.

The subscriber's `PubsubLongPollingRequest` can also be retried since it comes with a
max_processed_sequence_id. The retry will be sent with the same max_processed_sequence_id
and therefore the publisher will send back the all the messages from max_processed_sequence_id
to max_sequence_id in that subscriber's mailbox. Messages will not be removed from a subscriber's
mailbox until the subscriber sends a request with max_processed_sequence_id > sequence id of message.
Sequence id increments on every publish on a publisher, regardless of channel or entity.

Publishers keep receiving long polling requests from a subscriber as long
as there are subscribing entries from them. If subscribers are "dead", they are
not sending any more long polling requests. Publishers check if there's been active
long polling requests every 30 seconds to check if the subscriber is still alive. If
there's no activity on a LongPollingRequest for subscriber_timeout_ms (300s by default),
we'll flush the request (we'll reply) and wait to see if the subscriber sends another one.
If there hasn't been an active long polling request for over subscriber_timeout_ms, the
subscriber is considered dead and all metadata is cleaned up.
