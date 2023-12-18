.. _ray-mpi-guide:

Run MPI cluster on Ray (Experimental)
=====================================

.. note::

    This feature is experimental and the API may change in future releases.

`MPI <https://www.open-mpi.org/>`_ is a popular framework for distributed
computation. Ray clusters support the running of MPI programs. This guide
describes how to run MPI programs on Ray clusters.

Requirements
------------

`mpi4py <https://mpi4py.readthedocs.io/en/stable/>`_ is required to run MPI
programs on Ray clusters. See the documentation for installation instructions.

Get Started
-----------

A typical MPI program consists of a master process and multiple worker
processes. Inside Ray, the master process is a Ray remote function or actor and
the worker processes are normal processes. This configuration means the workers can't use
Ray APIs, but they can use MPI APIs. The master process can use Ray APIs and MPI
APIs at the same time.

The support is implemented as a runtime environment plugin. So it has to be
specify it in the function or actor decorator. For example:

    .. testcode::
      from ray.runtime_env import mpi_init
      @ray.remote(
        runtime_env={
            "mpi": {
                "args": ["-n", "4"],
                "worker_entry": "test_mpi.entry",
            },
        }
      )
      def foo():
        mpi_init()
        pass

The runtime environment plugin has two arguments: `args` and `worker_entry`. The
first one is the arguments passed to `mpirun` and the second one is the entry
function of the worker. The entry function is a normal Python function and it
must be accessible from the worker. In the above example, `foo` is the entry
function for the master process (either Ray actor or task) which has rank 0. The
functions in the working directory or it's in the python library is considered
as accessible. The Ray functions or actors are guarenteed to run with rank 0.

Call *`ray.runtime_env.mpi_init`* in the main entry. Otherwise, the
program hangs. This function ensures that the environment setup is correct
on both the master process and the worker processes.

.. note::
  
      You must call `ray.runtime_env.mpi_init` in the master process before any MPI operation. 
      Otherwise, the program hangs.


The Ray MPI plugin also supports actors. This code adds the function for an
actor to call MPI, so it can utilize MPI to run some distributed computation.

    .. testcode::
      def entry(val=None):
        from mpi4py import MPI
        import os

        mpi_init()
        comm = MPI.COMM_WORLD
        rank = comm.Get_rank()
        if rank == 0:
          return comm.Bcast([val])
        else:
          while True:
            data = comm.Bcast(None)
            print(data)
        

      from ray.runtime_env import mpi_init
      @ray.remote(
        runtime_env={
            "mpi": {
                "args": ["-n", "4"],
                "worker_entry": "test_mpi.entry",
            },
        },
        num_gpus=2,
      )
      class Foo:
        def __init__(self):
          # Call mpi_init in the beginning of the task or actor. Otherwise, it can hang
          mpi_init()
          pass

        def bar(self, val):
          return entry(val)

      foo = Foo.remote()
      # run the function as normal ray remote function.
      ray.get(foo.bar.remote())

Actors usually are used to host a service in Ray cluster running with MPI. For
example, it could be a long-living service and accept requests from the queue
and use MPI to finish the tasks. While for Ray functions, it's more like the
traditional MPI workers, doing computation distributedly.


The MPI plugin also supports GPUs. You can specify the number of GPUs in the
actor decorator as above. If you don't set it, the program sees all GPUs of the
nodes. All MPI workers will have the same device visibility flags as the ray
workers. For example, with CUDA, they'll share the same value of
`CUDA_VISIBILE_DEVICES` which is setup by Ray in the runtime.

You can call remote functions like `bar` in the same way you call normal Ray remote functions.
Inside the bar, it can call MPI APIs to do distributed computation. You
must carefully manage the coordination with the workers to avoid
deadlock.
