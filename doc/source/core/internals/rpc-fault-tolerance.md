---
myst:
  html_meta:
    description: "Make Ray Core RPCs fault tolerant with the retryable gRPC client, including long-polling pitfalls and idempotency requirements."
---

(rpc-fault-tolerance)=

# RPC fault tolerance

Every RPC you add to Ray Core should be fault tolerant and use the retryable gRPC client. Ideally, make each RPC idempotent. At a minimum, document any lack of idempotency, and make sure the client can take retries into account. To see what idempotency means, consider a function that writes "hello" to a file. On retry, it writes "hello" again, and the file ends up with "hellohello" in it. That function isn't idempotent. One way to make it idempotent is to check the file contents before writing "hello" again. Then the observable state after multiple identical calls is the same as after a single call.

This guide walks through a case study of an RPC that wasn't fault tolerant or idempotent, and how the fix works. It also covers what to look for when you add an RPC and which testing methods verify fault tolerance.

## Case study: RequestWorkerLease

### Problem

Before the fix described here, `RequestWorkerLease` couldn't be retryable because its handler in the raylet wasn't idempotent.

Once the raylet granted a lease, the lease stayed occupied until `ReturnWorker` was called. Until then, the worker and its resources never returned to the pool of available workers and resources. The raylet assumed that the original RPC and its retry were both fresh lease requests, and it couldn't deduplicate them.

For example, consider the following sequence of operations:

1. Request a new worker lease (Owner → Raylet) through `RequestWorkerLease`.
1. The response is lost (Raylet → Owner).
1. Retry `RequestWorkerLease` for the lease (Owner → Raylet).
1. The raylet grants two sets of resources and workers, one for the original request and one for the retry.

On a retry, the raylet should detect that the lease request is a retry and forward the address of the already leased worker to the owner, so it doesn't grant a second lease.

### Solution

To implement idempotency, [PR #55469](https://github.com/ray-project/ray/pull/55469) added a unique identifier called `LeaseID`, which makes it possible to deduplicate incoming lease requests. A `leased_workers` map tracks each granted lease by mapping lease IDs to workers. If an incoming lease request is already in the `leased_workers` map, the system recognizes the request as a retry and responds with the address of the already leased worker.

### Hidden problem: Long-polling RPCs

Transient network errors can happen at any time. Most RPCs finish in one I/O context execution, so guarding against a failed request or a failed response is sufficient. A few RPCs are long-polling, though. After the `HandleX` function executes, a long-polling RPC doesn't respond to the client immediately. Instead, it depends on a later state change to trigger the response to the client.

`RequestWorkerLease` was one of these RPCs. The raylet doesn't grant a lease until it pulls all the lease's arguments, so it can't respond to the client until pulling finishes. What happens if the client disconnects while the server logic executes on the raylet, and then sends a retry? The `leased_workers` map tracks only granted leases, not leases still in the process of being granted. As a result, the system couldn't deduplicate lease request retries while the server logic was executing, which [triggered a RAY_CHECK](https://github.com/ray-project/ray/blob/66c08b47a195bcfac6878a234dc804142e488fc2/src/ray/raylet/lease_dependency_manager.cc#L222) in the `lease_dependency_manager`.

Specifically, consider this sequence of operations:

1. Request a new worker lease (Owner → Raylet) through `RequestWorkerLease`.
1. The raylet is pulling the lease arguments asynchronously.
1. Retry `RequestWorkerLease` for the lease (Owner → Raylet).
1. The lease hasn't been granted yet, so it passes the idempotency check and the raylet fails to deduplicate the lease request.
1. The raylet tries to pull arguments for the same lease again, which hits a `RAY_CHECK`.

The final fix accounts for server logic that could still be executing, and tracks the lease through each phase of lease granting. At any phase, the system should be able to deduplicate requests.

For any long-polling RPC, be **particularly careful** about idempotency, because the client's retry doesn't necessarily wait for the server to send the response.

## Retryable gRPC client

The retryable gRPC client changed as part of the RPC fault-tolerance project. This section describes how it works and some pitfalls to watch for.

For an introduction, see the comment in [retryable_grpc_client.h](https://github.com/ray-project/ray/blob/885e34f4029f8956a0440f3cdfc89c9fe8f3d395/src/ray/rpc/retryable_grpc_client.h#L67).

### How it works

The retryable gRPC client works as follows:

- RPCs go through the retryable gRPC client.
- If the client encounters a [gRPC transient network error](https://github.com/ray-project/ray/blob/78082d65fa7081172d2848ced56d68cc612f8fd1/src/ray/common/grpc_util.h#L130), it pushes the callback into a queue.
- The client runs several checks periodically:

  - **Cheap gRPC channel state check**: This check inspects the state of the [gRPC channel](https://github.com/ray-project/ray/blob/885e34f4029f8956a0440f3cdfc89c9fe8f3d395/src/ray/rpc/retryable_grpc_client.cc#L74) to see whether the system can start sending messages again. It runs every second by default, and you can configure the interval with [check_channel_status_interval_milliseconds](https://github.com/ray-project/ray/blob/9b217e9ad01763e0b78c9161a4ebdd512289a748/src/ray/common/ray_config_def.h#L449).

  - **Potentially expensive GCS node status check**: If the exponential backoff period has passed and the channel is still down, the system calls [server_unavailable_timeout_callback_](https://github.com/ray-project/ray/blob/885e34f4029f8956a0440f3cdfc89c9fe8f3d395/src/ray/rpc/retryable_grpc_client.cc#L84). The client pool classes, [raylet_client_pool](https://github.com/ray-project/ray/blob/9b217e9ad01763e0b78c9161a4ebdd512289a748/src/ray/raylet_rpc_client/raylet_client_pool.cc#L24) and [core_worker_client_pool](https://github.com/ray-project/ray/blob/9b217e9ad01763e0b78c9161a4ebdd512289a748/src/ray/core_worker_rpc_client/core_worker_client_pool.cc#L28), set this callback. The callback checks whether the client is subscribed to node status updates, and then checks the local subscriber cache for a node death notification from the GCS. If the client isn't subscribed, or if the cache has no status for the node, the callback makes an RPC to the GCS. For the GCS client, the `server_unavailable_timeout_callback_` [kills the process when called](https://github.com/ray-project/ray/blob/888083bedf31458fb0fb33bf5613fb80f8fc0a6a/src/ray/gcs_rpc_client/rpc_client.h#L201). That happens after `gcs_rpc_server_reconnect_timeout_s` seconds, which defaults to 60.

  - **Per-RPC timeout check**: There's a [timeout check](https://github.com/ray-project/ray/blob/9b217e9ad01763e0b78c9161a4ebdd512289a748/src/ray/rpc/retryable_grpc_client.cc#L57) that you can customize per RPC, but it's functionally disabled because it's [always set to -1](https://github.com/ray-project/ray/blob/9b217e9ad01763e0b78c9161a4ebdd512289a748/src/ray/core_worker_rpc_client/core_worker_client.h#L72) for each RPC, which means an infinite timeout.
- Each additional failed RPC [increases the exponential backoff period](https://github.com/ray-project/ray/blob/9b217e9ad01763e0b78c9161a4ebdd512289a748/src/ray/rpc/retryable_grpc_client.cc#L95), regardless of the type of RPC that fails. The backoff period caps at a maximum that you can customize for the core worker and raylet clients with the `core_worker_rpc_server_reconnect_timeout_max_s` or `raylet_rpc_server_reconnect_timeout_max_s` config option. As noted earlier, the GCS client doesn't have a maximum backoff period.
- Once the channel check succeeds, the client [resets the exponential backoff period and retries all RPCs in the queue](https://github.com/ray-project/ray/blob/9b217e9ad01763e0b78c9161a4ebdd512289a748/src/ray/rpc/retryable_grpc_client.cc#L117).
- If the system receives a node death notification, either through its subscription or by querying the GCS directly, it destroys the RPC client. Destroying the client posts each callback to the I/O context with a [gRPC Disconnected error](https://github.com/ray-project/ray/blob/75f8562759d4a5ef84163bb68ae9f7401b85728f/src/ray/rpc/retryable_grpc_client.cc#L32).

### Important considerations

Keep the following points in mind:

- **Per-client queuing**: Each retryable gRPC client is unique to a client, not to a type of RPC. A core worker client is identified by its `WorkerID`, and a raylet client by its `NodeID`. Suppose you submit RPC A, which fails because of a transient network error, and then submit RPC B to the same client, which also fails because of a transient network error. The queue then holds two items, RPC A followed by RPC B. Queues exist per client, not per RPC.

- **Client-level timeouts**: Each timeout needs to wait for the previous timeout to complete. If you submit RPC A and RPC B in short succession, RPC A waits 1 second in total, and RPC B waits 1 + 2 = 3 seconds in total. The type of RPC doesn't matter, and the client treats every RPC the same. The reasoning is that transient network errors aren't specific to an RPC. If RPC A sees a network failure, you can assume that RPC B, if sent to the same client, experiences the same failure. As a result, the time an RPC waits is the sum of its own timeout and the timeouts of all the previous RPCs in the queue.

- **Destructor behavior**: The `RetryableGrpcClient` destructor fails all pending RPCs by posting their I/O contexts. Ideally, these callbacks should never modify state held by client classes such as `RayletClient`. If a callback must modify that state, it must check that the client is still alive, for example with a weak pointer. [PR #58744](https://github.com/ray-project/ray/pull/58744) shows an example. Application code should also account for the [Disconnected error](https://github.com/ray-project/ray/blob/75f8562759d4a5ef84163bb68ae9f7401b85728f/src/ray/rpc/retryable_grpc_client.cc#L32).

## Testing RPC fault tolerance

Ray Core has three layers of testing for RPC fault tolerance and idempotency.

### C++ unit tests

For each RPC, write some form of C++ idempotency test that calls the `HandleX` server function twice and checks that it produces the same result each time. The test should take into account different state changes between the `HandleX` calls. For example, a C++ unit test for `RequestWorkerLease` models a retry that arrives while the initial lease request is stuck in the argument-pulling stage.

### Python integration tests

Ideally, add a Python integration test for each RPC when it's straightforward to write. Some RPCs are hard to test fully deterministically through Python APIs, and for those, sufficient C++ unit testing can act as a good proxy. That makes a Python integration test more of a nice-to-have. Integration tests also act as examples of how a user could run into idempotency issues.

The main testing mechanism uses the `RAY_testing_rpc_failure` config option. Use it to do any of the following:

- Trigger the RPC callback immediately with a gRPC error without sending the RPC. This simulates a request failure.
- Trigger the RPC callback with a gRPC error once the response arrives from the server. This simulates a response failure.
- Trigger the RPC callback immediately with a gRPC error, but send the RPC to the server as well. This simulates an in-flight failure. For long-polling RPCs, the retry should ideally hit the server while it's executing the server code.

For details, see the comment in the Ray config file, [ray_config_def.h](https://github.com/ray-project/ray/blob/a24e625f409a5c638414e5d104fd265547e4d1b4/src/ray/common/ray_config_def.h#L860).

### Chaos network release tests

#### IP table blackout

The IP table blackout approach connects to each node over SSH and blacks out the IP tables for 5 seconds to simulate transient network errors. The IP table script runs in the background while the test script executes, causing a network blackout every 60 seconds.

[PR #58868](https://github.com/ray-project/ray/pull/58868) added the IP table blackout approach to all existing chaos release tests for Ray Core.

:::{note}
Amazon FIS was considered first. However, it has a 60-second minimum, which caused node death because of configuration settings and was hard to debug. The IP table approach was simpler and more flexible to use.
:::
