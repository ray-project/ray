.. _custom-tensor-transport:

*************************************************
Implementing a custom tensor transport (Advanced)
*************************************************

Ray Direct Transport (RDT) allows you to register custom tensor transports at runtime.
This page explains how to implement a custom tensor transport by implementing the :class:`TensorTransportManager <ray.experimental.TensorTransportManager>` abstract interface.

Overview
========

To create a custom tensor transport:

1. Implement the abstract interface :class:`ray.experimental.TensorTransportManager <ray.experimental.TensorTransportManager>`.
2. Define custom metadata classes by extending :class:`TensorTransportMetadata <ray.experimental.TensorTransportMetadata>` and :class:`CommunicatorMetadata <ray.experimental.CommunicatorMetadata>`.
3. Register your transport using :func:`ray.experimental.register_tensor_transport <ray.experimental.register_tensor_transport>`.

When Ray needs to transfer a tensor between actors using your transport, it calls specific methods on your ``TensorTransportManager`` implementation at different stages of the transfer lifecycle.


Implementing TensorTransportManager
===================================

The :class:`TensorTransportManager <ray.experimental.TensorTransportManager>` abstract class defines the interface for custom tensor transports. You must implement all abstract methods.

The following diagram shows when each method is called during a tensor transfer:

.. code-block:: text

   Source Actor                    Owner Process                 Destination Actor
   ============                    =============                 =================
        |                               |                               |
   1. Task returns tensor               |                               |
        |                               |                               |
   2. extract_tensor_transport_metadata |                               |
        |                               |                               |
        | ---- transport_metadata ----> |                               |
        |                               |                               |
        |                     3. get_communicator_metadata              |
        |                               |                               |
        | <---- comm metadata --------- | ---- comm metadata -------->  |
        |                               |                               |
   4. send_multiple_tensors             |          5. recv_multiple_tensors
        | ------------ tensors ---------------------------------------> |
        |                               |                               |
        |                         (transfer complete)                   |
        |                               |                               |
   6. garbage_collect                   |                               |
   (when ref goes out of scope)         |                               |


Note that Ray will not call `send_multiple_tensors` for one-sided transports.
The following diagram shows where each method is called in the ray.put / ray.get case supported by one-sided transports.

.. code-block:: text

   Source Actor                                                  Destination Actor
   ============                                                  =================
        |                                                               |
   1. User ray.put's tensor                                             |
        |                                                               |
   2. extract_tensor_transport_metadata                                 |
        |                                                               |
        |                                                               |
   3. User passes ref to another actor                                  |
        | ---- transport_metadata ---------------------------------->   |
        |                                                               |
        |                                                               |
        |                                                               |
        |                                                               |
        |                                          4. User ray.get's on object ref
                                                         get_communicator_metadata
        |                                                    recv_multiple_tensors
        | ------------ tensors --------- -----------------------------> |
        |                                                               |
        |                         (transfer complete)                   |
        |                                                               |
   5. garbage_collect                                                   |
   (when ref goes out of scope)                                         |


The API reference page for :class:`TensorTransportManager <ray.experimental.TensorTransportManager>` has more details what each method does how to implement them.
Below is an example implementation of a custom tensor transport.

Example Shared Memory Tensor Transport Manager
===============================================



Registering your transport
==========================

After implementing your transport, register it using :func:`ray.experimental.register_tensor_transport <ray.experimental.register_tensor_transport>`:

.. code-block:: python

   from ray.experimental import register_tensor_transport

   register_tensor_transport(
       "MY_TRANSPORT",    # Transport name
       ["cuda", "cpu"],   # List of supported device types
       MyCustomTransport, # Your TensorTransportManager class
   )

You can then use your transport in actor methods:

.. code-block:: python

    @ray.remote
    class MyActor:
        @ray.method(tensor_transport="MY_TRANSPORT")
        def create_tensor(self):
            return torch.randn(1000, 1000)

        def sum_tensor(self, tensor: torch.Tensor):
            return torch.sum(tensor).item()

    actors = [MyActor.remote() for _ in range(2)]
    ref = actors[0].create_tensor.remote()
    result = actors[1].sum_tensor.remote(ref)
    ray.get(result)


Limitations
===========

Custom tensor transports have the following limitations:

- **Actor restarts aren't supported.** Your actor doesn't have access to the custom transport after a restart.

- **Register transports before actor creation.** If you register a transport after creating an actor, that actor can't use the new transport.

- **Out-of-order actors** If you have an out-of-order actor (such as an async actor) and the process where you submit the actor task is different from where you created the actor, Ray can't guarantee it has registered your custom transport on the actor at task execution time.

- **Actor creation and task submission from different processes** If the process where you submit an actor task is different from where you created the actor, Ray can't guarantee it has registered your custom transport on the actor at task execution time.

For general RDT limitations, see :ref:`limitations <limitations>`.

Also feel free to reach out through GitHub issues or the OSS Ray Slack to ask any questions.
