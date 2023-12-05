.. _ray-mpi-guide:

Run MPI cluster on Ray (Experimental)
=====================================

.. note::

    This feature is experimental and the API may change in future releases.

`MPI <https://www.open-mpi.org/>`_ is a popular framework for distributed
computation. MPI programs are supported to run on Ray clusters. This guide
describes how to run MPI programs on Ray clusters.

Requirements
------------

`mpi4py <https://mpi4py.readthedocs.io/en/stable/>`_ is required to run MPI
programs on Ray clusters. Check the documentation for installation instructions.

Get Started
-----------

A typical MPI program consists of a master process and multiple worker
processes. Inside Ray, the master process is a Ray remote function or actor and
the worker processes are just normal processes. This means the workers can't use
Ray APIs, but they can use MPI APIs. The master process can use Ray APIs and MPI
APIs at the same time.

The support is implemented as a runtime environment plugin. So it has to be
specified in the function or actor decorator. For example:

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
function for master process which has rank 0.

`ray.runtime_env.mpi_init` has to be called in the main entry. Otherwise, the
program will hang. This function is to ensure the environment setup is correctly
on both the master process and the worker processes.

.. note::
  
      `ray.runtime_env.mpi_init` is required to be called in the master process before any MPI operations. 
      Otherwise, the program will hang.


Actors are also supported. This adds the function for the actor to call MPI, so
it can utilize MPI to run some distributed computation.

    .. testcode::
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
          mpi_init()
          pass

        def bar(self):
          pass


GPUs are also supported. The number of GPUs can be specified in the actor
decorator as above. If it's not set, the program will see all the GPUs of the
nodes.

Remote function like `bar` can be called just like normal Ray remote functions.
Inside the bar, it can call MPI APIs to do distributed computation. The
coordination with the workers have to be managed by the user carefully to avoid
deadlock.