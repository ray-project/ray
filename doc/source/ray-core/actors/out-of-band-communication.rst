Out-of-band Communication
=========================

Typically, Ray actor communication is done through actor method calls and data is shared through the distributed object store.
However, in some use cases out-of-band communication can be useful.

Wrapping Library Processes
--------------------------
Many libraries already have mature, high-performance internal communication stacks and
they leverage Ray as a language-integrated actor scheduler.
The actual communication between actors is mostly done out-of-band using existing communication stacks.
For example, Horovod-on-Ray uses NCCL or MPI-based collective communications, and RayDP uses Spark's internal RPC and object manager.
See `Ray Distributed Library Patterns <https://www.anyscale.com/blog/ray-distributed-library-patterns>`_ for more details.

Ray Collective
--------------
Ray's collective communication library (\ ``ray.util.collective``\ ) allows efficient out-of-band collective and point-to-point communication between distributed CPUs or GPUs.
See :ref:`Ray Collective <ray-collective>` for more details.

HTTP Server
-----------
You can start a http server inside the actor and expose http endpoints to clients
so users outside of the ray cluster can communicate with the actor.

.. tab-set::

    .. tab-item:: Python

        .. literalinclude:: ../doc_code/actor-http-server.py

Similarly, you can expose other types of servers as well (e.g., gRPC servers).

Limitations
-----------

When using out-of-band communication with Ray actors, keep in mind that Ray does not manage the calls between actors. This means that functionality like distributed reference counting will not work with out-of-band communication, so you should take care not to pass object references in this way.
