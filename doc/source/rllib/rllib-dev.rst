.. include:: /_includes/rllib/we_are_hiring.rst

.. include:: /_includes/rllib/new_api_stack.rst

How To Contribute to RLlib
==========================

Development Install
-------------------

You can develop RLlib locally without needing to compile Ray by using the `setup-dev.py <https://github.com/ray-project/ray/blob/master/python/ray/setup-dev.py>`__ script.
This sets up symlinks between the ``ray/rllib`` dir in your local git clone and the respective directory bundled with the pip-installed ``ray`` package. This way, every change you make in the source files in your local git clone will immediately be reflected in your installed ``ray`` as well.
However if you have installed ray from source using `these instructions <https://docs.ray.io/en/master/ray-overview/installation.html>`__ then don't use this, as these steps should have already created this symlink.
When using this script, make sure that your git branch is in sync with the installed Ray binaries (i.e., you are up-to-date on `master <https://github.com/ray-project/ray>`__ and have the latest `wheel <https://docs.ray.io/en/master/installation.html>`__ installed.)

.. code-block:: bash

    # Clone your fork onto your local machine, e.g.:
    git clone https://github.com/[your username]/ray.git
    cd ray
    # Only enter 'Y' at the first question on linking RLlib.
    # This leads to the most stable behavior and you won't have to re-install ray as often.
    # If you anticipate making changes to e.g. tune quite often, consider also symlinking ray tune here
    # (say 'Y' when asked by the script about creating the tune symlink).
    python python/ray/setup-dev.py

API Stability
-------------

Objects and methods annotated with ``@PublicAPI``, ``@DeveloperAPI``, or ``@ExperimentalAPI``
have the following API compatibility guarantees:

.. autofunction:: ray.rllib.utils.annotations.PublicAPI
    :noindex:

.. autofunction:: ray.rllib.utils.annotations.DeveloperAPI
    :noindex:

.. autofunction:: ray.rllib.utils.annotations.ExperimentalAPI
    :noindex:

Features
--------

Feature development, discussion, and upcoming priorities are tracked on the `GitHub issues page <https://github.com/ray-project/ray/issues>`__ (note that this may not include all development efforts).

Benchmarks
----------

A number of training run results are available in the `rl-experiments repo <https://github.com/ray-project/rl-experiments>`__, and there is also a list of working hyperparameter configurations in `tuned_examples <https://github.com/ray-project/ray/tree/master/rllib/tuned_examples>`__, sorted by algorithm. Benchmark results are extremely valuable to the community, so if you happen to have results that may be of interest, consider making a pull request to either repo.

Contributing Algorithms
-----------------------

These are the guidelines for merging new algorithms (`rllib/algorithms <https://github.com/ray-project/ray/tree/master/rllib/algorithms>`__) into RLlib:

* Contributed algorithms:
    - must subclass Algorithm and implement the ``training_step()`` method
    - must include a lightweight test to ensure the algorithm runs
    - should include tuned hyperparameter examples and documentation
    - should offer functionality not present in existing algorithms

* Fully integrated algorithms have the following additional requirements:
    - must fully implement the Algorithm API
    - must offer substantial new functionality not possible to add to other algorithms
    - should support custom models and preprocessors
    - should use RLlib abstractions and support distributed execution

Both integrated and contributed algorithms ship with the ``ray`` PyPI package, and are tested as part of Ray's automated tests. The main difference between contributed and fully integrated algorithms is that the latter will be maintained by the Ray team to a much greater extent with respect to bugs and integration with RLlib features.


Debugging your Algorithms
-------------------------

Finding Memory Leaks In Workers
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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

TensorFlow 2.x
~~~~~~~~~~~~~~

It is now recommended to use framework=tf2 and eager_tracing=True (in case you are developing with TensorFlow)
for maximum performance and support.
We will however continue to support framework=tf (static-graph) for the foreseeable future.

For debugging purposes, you should use ``framework=tf2`` with ``eager_tracing=False``.
All ``tf.Tensor`` values will then be visible and printable when executing your code.
However, some slowdown is to be expected in this config mode.

Older TensorFlow versions
~~~~~~~~~~~~~~~~~~~~~~~~~

RLlib supports both TensorFlow 2.x as well as ``tf.compat.v1`` modes.
Always use the ``ray.rllib.utils.framework.try_import_tf()`` utility function to import tensorflow.
It returns three values:

*  ``tf1``: The ``tf.compat.v1`` module or the installed tf1.x package (if the version is < 2.0).
*  ``tf``: The installed tensorflow module as-is.
*  ``tfv``: A version integer, whose value is either 1 or 2.
