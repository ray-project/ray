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

If messages are published before a subscription, they're lost.

## Implementation

The pubsub module doesn't have a broker like traditional pubsub systems because
there's no use case. In the pubsub module, all publishers are also brokers. The
performance, especially a throughput was not a requirement when developed, and
the module is not designed for high throughput.

### Basic mechanism

Between the publisher and subscriber, there's only 1 long-polling connection.
The long polling connection is initiated from the subscriber when there are
subscribing entries from the publisher. Whenever publisher publishes messages,
they are batched to the reply of the long polling request in FIFO order.

### Commands

A command is an operation from a subscriber to publisher. Subscribe and
Unsubscribe are the only commands. Commands are served by a separate
RPC, which also batches them in the FIFO order.

### What will actually happen / How it actually works
1. The subscriber sends a PubsubCommandBatchRequest with its own subscriber_id
and a SubMessage Command with a channel_type and possibly a key_id. It also sends
over a PubsubLongPollingRequest with its own subscriber_id.
2. The publisher receives the PubsubCommandBatchRequest, creates a SubscriberState
for the subscriber if it doesn't exist, registers the subscription for the subscriber
for the given channel + key, and sends a reply back. Registering the subscription means
setting up the relation between appropriate EntityState and SubscriberState.
The Publisher has a SubscriptionIndex for each channel and each SubscriptionIndex holds
EntityState's for each key in the channel. Each EntityState holds SubscriberState
pointers so it can insert into its mailbox. There's a special EntityState for
"subscribing to all" in every SubscriptionIndex.
3. The publisher receives the PubsubLongPollingRequest, creates a SubscriberState if
it doesn't exist, creates a "LongPollConnection" in the SubscriberState, and tries
to Publish by responding to the request if there were already things in its SubscriberState
mailbox. Note that 2 and 3 can happen out of order as well.
4. If the mailbox was empty at the time the PubsubLongPollingRequest was received, the
publisher will wait until the next relevant Publish to reply and send the publish over.
5. When the subscriber gets the reply to the PubsubCommandBatchRequest, it just runs
a callback for the command if the subscriber passed one in. It will also send new
commands to the publisher if they'd been queued up. We only allow one in-flight
PubsubCommandBatchRequest to a publisher to ensure ordering of commands.
6. When the subscriber gets the reply to the PubsubLongPollingRequest, it will process
the published messages and then send another PubsubLongPollingRequest if a subscription
still exists.
7. The publisher once again receives the PubsubLongPollingRequest, check the mailbox and
publish it if it's not empty or wait for a relevant publish to publish and reply.
8. When unsubscribing, the subscriber sends another PubsubCommandBatchRequest with an
UnsubscribeMessage.
9. The publisher receives the PubsubCommandBatchRequest and unregisters the SubscriberState from
the appropriate EntityState. If the EntityState doesn't have any more SubscriberState's, it will
be erased. Later on we'll clean up inactive SubscriberState's on an interval.

### Fault detection

Fault detection needed to be implemented in the component-agonistic manner, so
it doesn't use Ray's GCS for that.

Subscriber detects the publisher failures from the long polling request. A
single long polling request is initiated from the subscriber, and it sends them
again and again whenever replied as long as there are subscribing entries. If
the publisher fails, the long polling request is also failed, so that the
subscriber can detect the failures of publishers. All metadata is cleaned up in
this case.

Publishers always have received long polling request from a subscriber as long
as there are subscribing entries from them. If subscribers are failed, they are
not sending any more long polling requests. Publishers refreshes the long
polling request every 30 seconds to check if the subscriber is still alive. If
the subscriber doesn't initiate a long polling request for more than certain
threshold, the subscriber is considered failed and all metadata is cleaned up.
