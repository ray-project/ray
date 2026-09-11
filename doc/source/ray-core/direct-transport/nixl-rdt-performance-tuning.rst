.. _nixl-rdt-performance-tuning:

*****************************************
Performance tuning the NIXL RDT backend
*****************************************

Ray Direct Transport (RDT) layers a number of optimizations and helper APIs on top of `NVIDIA NIXL <https://github.com/ai-dynamo/nixl>`__ to reduce the per-transfer and memory registration overheads of RDMA.
This page explains what those optimizations are, when they help, and how to opt in to the helper APIs for the workloads that benefit the most.

If you haven't used RDT with NIXL before, start with the :ref:`NIXL walkthrough <direct-transport>` first.

Memory registration
===================

NIXL builds on RDMA one-sided protocols, which require memory regions to be registered with the NIC before they can participate in a transfer.
Registration is expensive: it issues a syscall per region and pins the pages so they can't be evicted.
RDT provides two complementary optimizations to keep this cost off the hot path.

PyTorch storage block registration
----------------------------------

PyTorch uses its own memory allocator, so when you allocate a large 2D tensor, for example, the rows usually share a single contiguous storage block.
Without this optimization, NIXL would register each row view individually, which can be very slow when you have many views.

RDT instead registers the underlying PyTorch storage block once, regardless of how many tensor views point into it.
The following snippet contrasts the two approaches:

.. code-block:: python

    # Assume view_0 ... view_n share the same PyTorch storage block.
    tensors = [view_0, view_1, view_2, ...]

    # Old behavior: n registrations, one per view.
    for tensor in tensors:
        nixl_agent.register_memory([tensor])

    # New behavior: one registration for the shared storage block.
    nixl_agent.register_memory(
        [
            (
                view_0.untyped_storage().data_ptr(),
                view_0.untyped_storage().nbytes(),
                ...
            )
        ]
    )

The following table shows the registration and deregistration time for a 2D tensor with ``X`` rows of size ``Y`` each,
measured against the underlying storage block:

.. list-table::
   :header-rows: 1
   :widths: 25 25 25

   * - Tensor shape (X, Y)
     - Registration time (ms)
     - Deregistration time (ms)
   * - X = 1, Y = 2 GB
     - 10.570
     - 0.015
   * - X = 1000, Y = 200 KB
     - 857.315
     - 220.120
   * - X = 1000, Y = 2 MB
     - 1361.092
     - 262.175
   * - X = 10000, Y = 200 KB
     - 10449.209
     - 3113.972

Registration time scales roughly linearly with the number of tensor views.
Larger memory regions also increase registration time, but much less aggressively than the number of regions does.
Also note that the first registration on a fresh NIXL agent typically takes 5-10x longer than subsequent registrations on the same agent,
so warm up the agent ahead of time if you're sensitive to first-call latency.

Control when memory is registered with ``register_nixl_memory``
---------------------------------------------------------------

By default, the lifetime of a NIXL memory registration is tied to the lifetime of the :class:`ObjectRef <ray.ObjectRef>` that points to the data.
The registration happens when the ref is created and the memory is deregistered when the ref goes out of scope.

On the sender side, registration happens either when you call :func:`ray.put` with NIXL or when a remote task returns a tensor through NIXL:

.. code-block:: python

    @ray.remote(tensor_transport="nixl")
    def produce():
        # Serializing the return value registers its memory with NIXL.
        return torch.zeros(1024, 1024, device="cuda")

    # ray.put registers the tensor immediately.
    ref = ray.put(tensor, _tensor_transport="nixl")

On the receiver side, registration happens either when an ``ObjectRef`` is passed by value as a task argument,
or when you call :func:`ray.get` on a ref that you've been passed by reference:

.. code-block:: python

    @ray.remote
    def consume_by_value(rdt_ref):
        # The system pre-allocates registered tensors
        # before the NIXL transfer fires for rdt_ref.
        # This happens before consume_by_value starts executing.
        ...

    @ray.remote
    def consume_by_reference(rdt_list):
        # ray.get triggers memory registration for RDT object refs.
        # This happens after consume_by_reference starts executing.
        tensor = ray.get(rdt_list[0])

This default keeps the API consistent with regular Ray object lifetimes, but it has two drawbacks:

* Memory registration can happen on the hot path of your script every time you pass refs around.
* You have to keep the ``ObjectRef`` alive yourself to avoid paying for re-registration on the next transfer.

To keep registrations off the hot path, use :func:`ray.experimental.register_nixl_memory` to pre-register a tensor.
Ray keeps the registration alive until you call :func:`ray.experimental.deregister_nixl_memory` on the same tensor.
Use this API when you know the tensors you want to transfer up front and their memory location doesn't change.
A common pattern is registering the weight buffers used for weight syncs between a trainer and an inference engine:

.. code-block:: python

    from ray.experimental import register_nixl_memory, deregister_nixl_memory

    @ray.remote(num_gpus=1, enable_tensor_transport=True)
    class Trainer:
        def __init__(self):
            self.weights = torch.randn(1024 * 1024, device="cuda")
            register_nixl_memory(self.weights)

        def __del__(self):
            deregister_nixl_memory(self.weights)

Avoid intermediate copies with ``set_target_for_ref``
-----------------------------------------------------

By default, :func:`ray.get` materializes the result into a fresh tensor.
In the trainer example above, an ``update_weights`` method written the obvious way ends up allocating a throwaway buffer on every call:

.. code-block:: python

    def update_weights(self, weights_ref):
        # Allocates a new tensor and registers it with NIXL,
        # even though self.weights is already registered.
        new_weights = ray.get(weights_ref[0])
        self.weights = new_weights

That intermediate ``new_weights`` tensor wastes memory,
and with NIXL it's even more expensive because :func:`ray.get` registers the new tensor with NIXL before the transfer.

:func:`ray.experimental.set_target_for_ref` tells Ray to receive directly into a tensor that you already own
(and, ideally, that you've already registered):

.. code-block:: python

    from ray.experimental import set_target_for_ref

    def update_weights(self, weights_ref):
        set_target_for_ref(weights_ref[0], [self.weights])
        ray.get(weights_ref[0])

The main limitation is that ``set_target_for_ref`` only works in the ``ray.put`` / borrowing pattern shown above.
It can't be used when the ref is passed as a task argument because Ray would already have created a placeholder tensor during argument deserialization before ``update_weights`` runs.

For best results, combine ``set_target_for_ref`` with ``register_nixl_memory`` to prevent unnecessary memory registrations across calls.

NIXL agent reuse
================

The NIXL agent on the receiver side isn't stateless.
It maintains a registry of remote agents (one per sender) and tracks every memory descriptor those senders have registered.
Each transfer goes through descriptor validation (``rkey``, memory regions, and so on) against this registry.

The original implementation created and destroyed the remote agent entry on every receive.
For senders with many distinct memory registrations, the per-transfer cost of this was significant.

To avoid that overhead, Ray version-tracks each remote agent in the receiver's registry:

* When the sender adds a new memory registration, Ray incrementally updates the remote agent's descriptor list without bumping the version.
* When the sender deregisters memory, Ray bumps the version and rebuilds the remote agent, because NIXL doesn't support incremental deregistration.

You can tune the cache size with the ``RAY_NIXL_REMOTE_AGENT_CACHE_MAXSIZE`` environment variable.
Set it to ``0`` to disable agent reuse entirely (mostly useful for benchmarking).
When the cache is full, Ray evicts the least recently used remote agent.

The following table shows the cost of adding and removing a remote agent on the receiver side,
for a sender that has registered a 2D tensor with ``X`` rows of size ``Y`` each, with and without agent reuse:

.. list-table::
   :header-rows: 1
   :widths: 20 15 20 20

   * - Tensor shape (X, Y)
     - Agent reuse
     - Add remote agent (ms)
     - Remove remote agent (ms)
   * - X = 1, Y = 2 GB
     - false
     - 0.457
     - 0.020
   * - X = 1, Y = 2 GB
     - true
     - 0.001
     - 0.000
   * - X = 1000, Y = 2 MB
     - false
     - 1.469
     - 0.312
   * - X = 1000, Y = 2 MB
     - true
     - 0.279
     - 0.000
   * - X = 10000, Y = 200 KB
     - false
     - 15.658
     - 3.743
   * - X = 10000, Y = 200 KB
     - true
     - 3.128
     - 0.000

The cost of initializing and tearing down a remote agent scales with the number of memory descriptors the sender has registered.
Combining agent reuse with the storage block registration optimization collapses the descriptor count,
which together bring all of the scenarios in the table down to roughly the cost of the ``X = 1000, Y = 2 MB`` row with ``agent_reuse = true``.

Summary
=======

For workloads that re-use the same tensors across many transfers, such as RL weight syncs:

* Re-use the same tensors and call :func:`ray.experimental.register_nixl_memory` once at startup, so memory registration stays off the hot path.
* Pre-allocate receive buffers and bind them with :func:`ray.experimental.set_target_for_ref` to skip the intermediate copy on :func:`ray.get`.
* Pass whole tensors, or views into a shared storage block, rather than many small standalone tensors, so RDT registers a single underlying storage block instead of many descriptors.