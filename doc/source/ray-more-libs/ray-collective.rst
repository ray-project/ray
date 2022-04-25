.. _ray-collective:

..
  This part of the docs is generated from the ray.util.collective readme using m2r
  To update:
  - run `m2r RAY_ROOT/python/ray/util/collective/README.md`
  - copy the contents of README.rst here
  - Be sure not to delete the API reference section in the bottom of this file.


Ray Collective Communication Lib
================================

The Ray collective communication library (\ ``ray.util.collectve``\ ) offers a set of native collective primitives for 
communication between distributed CPUs or GPUs.

Ray collective communication library


* enables 10x more efficient out-of-band collective communication between Ray actor and task processes,
* operates on both distributed CPUs and GPUs,
* uses NCCL and GLOO as the optional high-performance communication backends,
* is suitable for distributed ML programs on Ray.

Collective Primitives Support Matrix
------------------------------------

See below the current support matrix for all collective calls with different backends. 

.. list-table::
   :header-rows: 1

   * - Backend
     - `gloo <https://github.com/ray-project/pygloo>`_
     - 
     - `nccl <https://docs.nvidia.com/deeplearning/nccl/user-guide/docs/index.html>`_
     - 
   * - Device
     - CPU
     - GPU
     - CPU
     - GPU
   * - send
     - ✔
     - ✘
     - ✘
     - ✔
   * - recv
     - ✔
     - ✘
     - ✘
     - ✔
   * - broadcast
     - ✔
     - ✘
     - ✘
     - ✔
   * - allreduce
     - ✔
     - ✘
     - ✘
     - ✔
   * - reduce
     - ✔
     - ✘
     - ✘
     - ✔
   * - allgather
     - ✔
     - ✘
     - ✘
     - ✔
   * - gather
     - ✘
     - ✘
     - ✘
     - ✘
   * - scatter
     - ✘
     - ✘
     - ✘
     - ✘
   * - reduce_scatter
     - ✔
     - ✘
     - ✘
     - ✔
   * - all-to-all
     - ✘
     - ✘
     - ✘
     - ✘
   * - barrier
     - ✔
     - ✘
     - ✘
     - ✔


Supported Tensor Types
----------------------


* ``torch.Tensor``
* ``numpy.ndarray``
* ``cupy.ndarray``

Usage
-----

Installation and Importing
^^^^^^^^^^^^^^^^^^^^^^^^^^

Ray collective library is bundled with the released Ray wheel. Besides Ray, users need to install either `pygloo <https://github.com/ray-project/pygloo>`_ 
or `cupy <https://docs.cupy.dev/en/stable/install.html>`_ in order to use collective communication with the GLOO and NCCL backend, respectively.

.. code-block:: python

   pip install pygloo
   pip install cupy-cudaxxx # replace xxx with the right cuda version in your environment

To use these APIs, import the collective package in your actor/task or driver code via:

.. code-block:: python

   import ray.util.collective as col

Initialization
^^^^^^^^^^^^^^

Collective functions operate on collective groups. 
A collective group contains a number of processes (in Ray, they are usually Ray-managed actors or tasks) that will together enter the collective function calls. 
Before making collective calls, users need to declare a set of actors/tasks, statically, as a collective group. 

Below is an example code snippet that uses the two APIs ``init_collective_group()`` and ``declare_collective_group()`` to initialize collective groups among a few 
remote actors. Refer to `APIs <#api-reference>`_ for the detailed descriptions of the two APIs.

.. code-block:: python

   import ray
   import ray.util.collective as collective

   import cupy as cp


   @ray.remote(num_gpus=1)
   class Worker:
      def __init__(self):
          self.send = cp.ones((4, ), dtype=cp.float32)
          self.recv = cp.zeros((4, ), dtype=cp.float32)

      def setup(self, world_size, rank):
          collective.init_collective_group(world_size, rank, "nccl", "default")
          return True

      def compute(self):
          collective.allreduce(self.send, "default")
          return self.send

      def destroy(self):
          collective.destroy_group()

   # imperative
   num_workers = 2
   workers = []
   init_rets = []
   for i in range(num_workers):
      w = Worker.remote()
      workers.append(w)
      init_rets.append(w.setup.remote(num_workers, i))
   _ = ray.get(init_rets)
   results = ray.get([w.compute.remote() for w in workers])


   # declarative
   for i in range(num_workers):
      w = Worker.remote()
      workers.append(w)
   _options = {
      "group_name": "177",
      "world_size": 2,
      "ranks": [0, 1],
      "backend": "nccl"
   }
   collective.declare_collective_group(workers, **_options)
   results = ray.get([w.compute.remote() for w in workers])

Note that for the same set of actors/task processes, multiple collective groups can be constructed, with ``group_name`` as their unique identifier. 
This enables to specify complex communication patterns between different (sub)set of processes.

Collective Communication
^^^^^^^^^^^^^^^^^^^^^^^^

Check `the support matrix <#collective-primitives-support-matrix>`_ for the current status of supported collective calls and backends.

Note that the current set of collective communication API are imperative, and exhibit the following behaviours:


* All the collective APIs are synchronous blocking calls
* Since each API only specifies a part of the collective communication, the API is expected to be called by each participating process of the (pre-declared) collective group. 
  Upon all the processes have made the call and rendezvous with each other, the collective communication happens and proceeds.
* The APIs are imperative and the communication happends out-of-band --- they need to be used inside the collective process (actor/task) code.

An example of using ``ray.util.collective.allreduce`` is below:

.. code-block:: python

   import ray
   import cupy
   import ray.util.collective as col


   @ray.remote(num_gpus=1)
   class Worker:
       def __init__(self):
           self.buffer = cupy.ones((10,), dtype=cupy.float32)

       def compute(self):
           col.allreduce(self.buffer, "default")
           return self.buffer

   # Create two actors A and B and create a collective group following the previous example...
   A = Worker.remote()
   B = Worker.remote()
   # Invoke allreduce remotely
   ray.get([A.compute.remote(), B.compute.remote()])

Point-to-point Communication
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

``ray.util.collective`` also supports P2P send/recv communication between processes. 

The send/recv exhibits the same behavior with the collective functions: 
they are synchronous blocking calls -- a pair of send and recv must be called together on paired processes in order to specify the entire communication, 
and must successfully rendezvous with each other to proceed. See the code example below:

.. code-block:: python

   import ray
   import cupy
   import ray.util.collective as col


   @ray.remote(num_gpus=1)
   class Worker:
       def __init__(self):
           self.buffer = cupy.ones((10,), dtype=cupy.float32)

       def get_buffer(self):
           return self.buffer

       def do_send(self, target_rank=0):
           # this call is blocking
           col.send(target_rank)

       def do_recv(self, src_rank=0):
           # this call is blocking
           col.recv(src_rank)

       def do_allreduce(self):
           # this call is blocking as well
           col.allreduce(self.buffer)
           return self.buffer

   # Create two actors
   A = Worker.remote()
   B = Worker.remote()

   # Put A and B in a collective group
   col.declare_collective_group([A, B], options={rank=[0, 1], ...})

   # let A to send a message to B; a send/recv has to be specified once at each worker
   ray.get([A.do_send.remote(target_rank=1), B.do_recv.remote(src_rank=0)])

   # An anti-pattern: the following code will hang, because it does instantiate the recv side call
   ray.get([A.do_send.remote(target_rank=1)])

Single-GPU and Multi-GPU Collective Primitives
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

In many cluster setups, a machine usually has more than 1 GPU; 
effectively leveraging the GPU-GPU bandwidth, such as `NVLINK <https://www.nvidia.com/en-us/design-visualization/nvlink-bridges/>`_\ , 
can significantly improve communication performance.

``ray.util.collective`` supports multi-GPU collective calls, in which case, a process (actor/tasks) manages more than 1 GPU (e.g., via ``ray.remote(num_gpus=4)``\ ). 
Using these multi-GPU collective functions are normally more performance-advantageous than using single-GPU collective API 
and spawning the number of processes equal to the number of GPUs.
See the API references for the signatures of multi-GPU collective APIs.

Also of note that all multi-GPU APIs are with the following restrictions:


* Only NCCL backend is supported.
* Collective processes that make multi-GPU collective or P2P calls need to own the same number of GPU devices.
* The input to multi-GPU collective functions are normally a list of tensors, each located on a different GPU device owned by the caller process.

An example code utilizing the multi-GPU collective APIs is provided below:

.. code-block:: python

   import ray
   import ray.util.collective as collective

   import cupy as cp
   from cupy.cuda import Device


   @ray.remote(num_gpus=2)
   class Worker:
      def __init__(self):
          with Device(0):
              self.send1 = cp.ones((4, ), dtype=cp.float32)
          with Device(1):
              self.send2 = cp.ones((4, ), dtype=cp.float32) * 2
          with Device(0):
              self.recv1 = cp.ones((4, ), dtype=cp.float32)
          with Device(1):
              self.recv2 = cp.ones((4, ), dtype=cp.float32) * 2

      def setup(self, world_size, rank):
          collective.init_collective_group(world_size, rank, "nccl", "177")
          return True

      def allreduce_call(self):
          collective.allreduce_multigpu([self.send1, self.send2], "177")
          return [self.send1, self.send2]

      def p2p_call(self):
          if self.rank == 0:
             collective.send_multigpu(self.send1 * 2, 1, 1, "8")
          else:
             collective.recv_multigpu(self.recv2, 0, 0, "8")
          return self.recv2

   # Note that the world size is 2 but there are 4 GPUs.
   num_workers = 2
   workers = []
   init_rets = []
   for i in range(num_workers):
      w = Worker.remote()
      workers.append(w)
      init_rets.append(w.setup.remote(num_workers, i))
   a = ray.get(init_rets)
   results = ray.get([w.allreduce_call.remote() for w in workers])
   results = ray.get([w.p2p_call.remote() for w in workers])

More Resources
--------------

The following links provide helpful resources on how to efficiently leverage the ``ray.util.collective`` library.


* `More running examples <https://github.com/ray-project/ray/tree/master/python/ray/util/collective/examples>`_ under ``ray.util.collective.examples``.
* `Scaling up the Spacy Name Entity Recognition (NER) pipeline <https://github.com/explosion/spacy-ray>`_ using Ray collective library.
* `Implementing the AllReduce strategy <https://github.com/ray-project/distml/blob/master/distml/strategy/allreduce_strategy.py>`_ for data-parallel distributed ML training.

API References
--------------

.. automodule:: ray.util.collective.collective
    :members:
