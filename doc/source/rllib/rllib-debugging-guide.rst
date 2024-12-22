.. include:: /_includes/rllib/we_are_hiring.rst

.. include:: /_includes/rllib/new_api_stack.rst

.. _rllib-debugging-guide:

Debugging RLlib Experiments
===========================

Using your IDE's debugger
-------------------------

The fastest way to find and fix bugs in RLlib and your custom code is to use a locally installed IDE,
such as `PyCharm <https://www.jetbrains.com/pycharm/>`__ or `VS Code <https://code.visualstudio.com/>`__.

We strongly recommend to install either one of these software first, before you start your journey into developing
with RLlib. Even though, Ray and RLlib are distributed and best unfold all of their potential in production on large,
multi-node clusters, it's often helpful to start running your programs locally on your laptop or desktop machine and see,
whether - roughly - it works as intended. Even if your local setup doesn't have some compute resources that are absolutely
crucial for the actual training rund to succeed (and not take forever), most bugs already surface in the simplest of
setups, for example in a single, local process running on the CPU.

To change your config, such that your RLlib program runs in such local setup, you should - before anything else -try
the following settings.



Finding memory leaks in EnvRunner actors
----------------------------------------

Keeping the memory usage of long running workers stable can be challenging. The ``MemoryTrackingCallbacks`` class can be used to track memory usage of workers.

.. autoclass:: ray.rllib.algorithms.callbacks.MemoryTrackingCallbacks

The objects with the top 20 memory usage in the workers are added as custom metrics. These can then be monitored using tensorboard or other metrics integrations like Weights & Biases:

.. image:: images/MemoryTrackingCallbacks.png


Troubleshooting
---------------

If you encounter errors like
`blas_thread_init: pthread_create: Resource temporarily unavailable` when using many workers,
try setting ``OMP_NUM_THREADS=1``. Similarly, check configured system limits with
`ulimit -a` for other resource limit errors.

For debugging unexpected hangs or performance problems, you can run ``ray stack`` to dump
the stack traces of all Ray workers on the current node, ``ray timeline`` to dump
a timeline visualization of tasks to a file, and ``ray memory`` to list all object
references in the cluster.


Episode traces
--------------

You can use the `data output API <rllib-offline.html>`__ to save episode traces
for debugging. For example, the following command will run PPO while saving episode
traces to ``/tmp/debug``.

.. code-block:: bash

    cd rllib/tuned_examples/ppo
    python cartpole_ppo.py --output /tmp/debug

    # episode traces will be saved in /tmp/debug, for example
    output-2019-02-23_12-02-03_worker-2_0.json
    output-2019-02-23_12-02-04_worker-1_0.json


Log verbosity
-------------

You can control the log level via the ``"log_level"`` flag. Valid values are "DEBUG",
"INFO", "WARN" (default), and "ERROR". This can be used to increase or decrease the
verbosity of internal logging.
For example:

.. code-block:: bash

    cd rllib/tuned_examples/ppo

    python atari_ppo.py --env ALE/Pong-v5 --log-level INFO
    python atari_ppo.py --env ALE/Pong-v5 --log-level DEBUG

The default log level is ``WARN``, but you should use at least the ``INFO`` level logging
for development.


Stack traces
------------

You can use the ``ray stack`` command to dump the stack traces of all the
Python workers on a single node. This can be useful for debugging unexpected
hangs or performance issues.


Next steps
----------

- To check how your application is doing, you can use the :ref:`Ray dashboard <observability-getting-started>`.
