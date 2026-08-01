.. _rpc-fault-tolerance:

RPC Fault Tolerance
===================

All RPCs added to Ray Core should be fault tolerant and use the retryable gRPC client. 
Ideally, they should be idempotent, or at the very least, the lack of idempotency should be 
documented and the client must be able to take retries into account. If you aren't familiar 
with what idempotency is, consider a function that writes "hello" to a file. On retry,
it writes "hello" again, resulting in "hellohello". This isn't idempotent. To make it
idempotent, you could check the file contents before writing "hello" again, ensuring that the 
observable state after multiple identical function calls is the same as after a single call.

This guide walks you through a case study of a RPC that wasn't fault tolerant or idempotent, 
and how it was fixed. By the end of this guide, you should understand what to look for 
when adding new RPCs and which testing methods to use to verify fault tolerance.

Case study: RequestWorkerLease
-------------------------------

Problem
~~~~~~~

Prior to the fix described here, ``RequestWorkerLease`` could not be made retryable because 
its handler in the Raylet was not idempotent.

This was because once leases were granted, they were considered occupied until ``ReturnWorker``
was called. Until this RPC was called, the worker and its resources were never returned to the pool 
of available workers and resources. The raylet assumed that the original RPC and its retry were 
both fresh lease requests and couldn't deduplicate them. 

For example, consider the following sequence of operations:

1. Request a new worker lease (Owner → Raylet) through ``RequestWorkerLease``.
2. Response is lost (Raylet → Owner).
3. Retry ``RequestWorkerLease`` for lease (Owner → Raylet).
4. Two sets of resources and workers are now granted, one for the original AND retry.

On the retry, the raylet should detect that the lease request is a retry and forward the 
already leased worker address to the owner so a second lease isn't granted.

Solution
~~~~~~~~

To implement idempotency, a unique identifier called ``LeaseID`` was added in 
`PR #55469 <https://github.com/ray-project/ray/pull/55469>`_, which allowed for the deduplication 
of incoming lease requests. Once leases are granted, they're tracked in a ``leased_workers`` map 
which maps lease IDs to workers. If the new lease request is already present in the 
``leased_workers`` map, the system knows this lease request is a retry and responds with the 
already leased worker address.

Hidden problem: long-polling RPCs
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Network transient errors can happen at any time. For most RPCs, they finish in one I/O context 
execution, so guarding against whether the request or response failed is sufficient. 
However, there are a few RPCs that are long-polling, meaning that once the ``HandleX`` function 
executes, it won't immediately respond to the client but will rather depend on some state change 
in the future to trigger the response back to the client.

This was the case for ``RequestWorkerLease``. Leases aren't granted until all the args are pulled, 
so the system can't respond to the client until the pulling process is over. What happens if the 
client disconnects while the server logic executes on the raylet side and sends a retry? The 
``leased_workers`` map only tracks leases that are granted, not in the process of granting. 
This caused a 
`RAY_CHECK to be triggered <https://github.com/ray-project/ray/blob/66c08b47a195bcfac6878a234dc804142e488fc2/src/ray/raylet/lease_dependency_manager.cc#L222>`_ 
in the ``lease_dependency_manager`` because the system wasn't able to deduplicate lease 
request retries while the server logic was executing.

Specifically, consider this sequence of operations:

1. Request a new worker lease (Owner → Raylet) through ``RequestWorkerLease``.
2. The raylet is pulling the lease args asynchronously for the lease.
3. Retry ``RequestWorkerLease`` for the lease (Owner → Raylet).
4. The lease hasn't been granted yet, so it passes the idempotency check and the raylet 
   fails to deduplicate the lease request.
5. ``RAY_CHECK`` hit since the raylet tries to pull args for the same lease again.

The final fix was to take into account that the server logic could still be executing and track 
the lease as it goes through the phases of lease granting. At any phase, the system should be 
able to deduplicate requests.

For any long-polling RPC, you should be **particularly careful** about idempotency because the 
client's retry won't necessarily wait for the response to be sent.

Retryable gRPC client
---------------------

The retryable gRPC client was updated during the RPC fault tolerance project. This section 
describes how it works and some gotchas to watch out for.

For a basic introduction, read the 
`retryable_grpc_client.h comment <https://github.com/ray-project/ray/blob/885e34f4029f8956a0440f3cdfc89c9fe8f3d395/src/ray/rpc/retryable_grpc_client.h#L67>`_.

How it works
~~~~~~~~~~~~

The retryable gRPC client works as follows:

- RPCs are sent using the retryable gRPC client.
- If the client encounters a 
  `gRPC transient network error <https://github.com/ray-project/ray/blob/78082d65fa7081172d2848ced56d68cc612f8fd1/src/ray/common/grpc_util.h#L130>`_, 
  it pushes the callback into a queue.
- Several checks are done on a periodic basis:

  - **Cheap gRPC channel state check**: This checks the state of the 
    `gRPC channel <https://github.com/ray-project/ray/blob/885e34f4029f8956a0440f3cdfc89c9fe8f3d395/src/ray/rpc/retryable_grpc_client.cc#L74>`_ 
    to see whether the system can start sending messages again. This check happens every 
    second by default, but is configurable through 
    `check_channel_status_interval_milliseconds <https://github.com/ray-project/ray/blob/9b217e9ad01763e0b78c9161a4ebdd512289a748/src/ray/common/ray_config_def.h#L449>`_.
    
  - **Potentially expensive GCS node status check**: If the exponential backoff period has 
    passed and the channel is still down, the system calls 
    `server_unavailable_timeout_callback_ <https://github.com/ray-project/ray/blob/885e34f4029f8956a0440f3cdfc89c9fe8f3d395/src/ray/rpc/retryable_grpc_client.cc#L84>`_. 
    This callback is set in the client pool classes 
    (`raylet_client_pool <https://github.com/ray-project/ray/blob/9b217e9ad01763e0b78c9161a4ebdd512289a748/src/ray/raylet_rpc_client/raylet_client_pool.cc#L24>`_, 
    `core_worker_client_pool <https://github.com/ray-project/ray/blob/9b217e9ad01763e0b78c9161a4ebdd512289a748/src/ray/core_worker_rpc_client/core_worker_client_pool.cc#L28>`_). 
    It checks if the client is subscribed for node status updates, and then checks the local 
    subscriber cache to see whether a node death notification from the GCS has been received. 
    If the client isn't subscribed or if there's no status for the node in the cache, it 
    makes a RPC to the GCS. Note that for the GCS client, the ``server_unavailable_timeout_callback_`` 
    `kills the process once called <https://github.com/ray-project/ray/blob/888083bedf31458fb0fb33bf5613fb80f8fc0a6a/src/ray/gcs_rpc_client/rpc_client.h#L201>`_. 
    This happens after ``gcs_rpc_server_reconnect_timeout_s`` seconds (60 by default).
    
  - **Per-RPC timeout check**: There's a 
    `timeout check <https://github.com/ray-project/ray/blob/9b217e9ad01763e0b78c9161a4ebdd512289a748/src/ray/rpc/retryable_grpc_client.cc#L57>`_ 
    that's customizable per RPC, but it's functionally disabled because it's 
    `always set to -1 <https://github.com/ray-project/ray/blob/9b217e9ad01763e0b78c9161a4ebdd512289a748/src/ray/core_worker_rpc_client/core_worker_client.h#L72>`_ 
    (infinity) for each RPC.
- With each additional failed RPC, the
  `exponential backoff period is increased <https://github.com/ray-project/ray/blob/9b217e9ad01763e0b78c9161a4ebdd512289a748/src/ray/rpc/retryable_grpc_client.cc#L95>`_,
  agnostic of the type of RPC that fails. The backoff period caps out to a max that you can 
  customize for the core worker and raylet clients using either the 
  ``core_worker_rpc_server_reconnect_timeout_max_s`` or ``raylet_rpc_server_reconnect_timeout_max_s`` 
  config options. The GCS client doesn't have a max backoff period as noted above.
- Once the channel check succeeds, the
  `exponential backoff period is reset and all RPCs in the queue are retried <https://github.com/ray-project/ray/blob/9b217e9ad01763e0b78c9161a4ebdd512289a748/src/ray/rpc/retryable_grpc_client.cc#L117>`_.
- If the system successfully receives a node death notification (either through subscription or
  querying the GCS directly), it destroys the RPC client, which posts each callback to the I/O
  context with a
  `gRPC Disconnected error <https://github.com/ray-project/ray/blob/75f8562759d4a5ef84163bb68ae9f7401b85728f/src/ray/rpc/retryable_grpc_client.cc#L32>`_.

Important considerations
~~~~~~~~~~~~~~~~~~~~~~~~

A few important points to keep in mind:

- **Per-client queuing**: Each retryable gRPC client is unique to the client (``WorkerID`` for 
  core worker clients, ``NodeID`` for raylet clients), not to the type of RPC. If you first 
  submit RPC A that fails due to a transient network error, then RPC B to the same client that 
  fails due to a transient network error, the queue will have two items: RPC A then RPC B. 
  There isn't a separate queue on an RPC basis, but on a client basis.

- **Client-level timeouts**: Each timeout needs to wait for the previous timeout to complete. 
  If both RPC A and RPC B are submitted in short succession, then RPC A will wait in total 
  for 1 second, and RPC B will wait in total for 1 + 2 = 3 seconds. Different RPCs don't 
  matter and are treated the same. The reasoning is that transient network errors aren't RPC 
  specific. If RPC A sees a network failure, you can assume that RPC B, if sent to the same 
  client, will experience the same failure. Hence, the time that an RPC waits is the sum of 
  the timeouts of all the previous RPCs in the queue and its own timeout.

- **Destructor behavior**: In the destructor for ``RetryableGrpcClient``, the system fails all
  pending RPCs by posting their I/O contexts. These callbacks should ideally never modify state
  held by the client classes such as ``RayletClient``. If absolutely necessary, they must check
  if the client is still alive somehow, such as using a weak pointer. An example of this is in
  `PR #58744 <https://github.com/ray-project/ray/pull/58744>`_. The application code should
  also take into account the
  `Disconnected error <https://github.com/ray-project/ray/blob/75f8562759d4a5ef84163bb68ae9f7401b85728f/src/ray/rpc/retryable_grpc_client.cc#L32>`_.

Testing RPC fault tolerance
----------------------------

Ray Core has three layers of testing for RPC fault tolerance and idempotency.

C++ unit tests
~~~~~~~~~~~~~~

For each RPC, there should be some form of C++ idempotency test that calls the ``HandleX`` server 
function twice and checks that the same result is outputted each time. Different state changes 
between the ``HandleX`` server function calls should be taken into account. For example, in 
``RequestWorkerLease``, a C++ unit test was written to model the situation where the retry comes 
while the initial lease request is stuck in the args pulling stage.

Python integration tests
~~~~~~~~~~~~~~~~~~~~~~~~

For each RPC, there should ideally be a Python integration test if it's straightforward. For some 
RPCs, it's challenging to test them fully deterministically using Python APIs, so having 
sufficient C++ unit testing can act as a good proxy. Hence, it's more of a nice-to-have, as 
integration tests also act as examples of how a user could run into idempotency issues.

The main testing mechanism uses the ``RAY_testing_rpc_failure`` config option, which allows 
you to:

- Trigger the RPC callback immediately with a gRPC error without sending the RPC
  (simulating a request failure).
- Trigger the RPC callback with a gRPC error once the response arrives from the server 
  (simulating a response failure).
- Trigger the RPC callback immediately with a gRPC error but send the RPC to the server as well 
  (simulating an in-flight failure, where the retry should ideally hit the server while it's 
  executing the server code for long-polling RPCs).

For more details, see the comment in the Ray config file at 
`ray_config_def.h <https://github.com/ray-project/ray/blob/a24e625f409a5c638414e5d104fd265547e4d1b4/src/ray/common/ray_config_def.h#L860>`_.

Chaos network release tests
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

IP table blackout
^^^^^^^^^^^^^^^^^

The IP table blackout approach involves SSH-ing into each node and blacking out the IP tables
for a small amount of time (5 seconds) to simulate transient network errors. The IP table script
runs in the background, periodically (60 seconds) causing network blackouts while the test script
executes.

For core release tests, we've added the IP table blackout approach to all existing chaos release
tests in `PR #58868 <https://github.com/ray-project/ray/pull/58868>`_.

.. note::
   Initially, Amazon FIS was considered. However, it has a 60-second minimum which caused node 
   death due to configuration settings which was hard to debug, so the IP table approach was 
   simpler and more flexible to use.
