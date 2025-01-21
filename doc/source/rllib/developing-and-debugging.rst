
.. include:: /_includes/rllib/we_are_hiring.rst

.. _rllib-developing-and-debugging-docs:

Developing with RLlib and debugging
===================================

.. include:: /_includes/rllib/new_api_stack.rst

This page teaches you how to set up Ray RLlib for development. It walks you through cloning your own github fork,
installing Ray, setting up RLlib so you can modify and customize your own code, creating a pull request (PR),
debugging RLlib code, and clearing and re-installing Ray and RLlib in case of problems with your installation or setup.


Forking the git repository
--------------------------

First, create your own fork using your github account.

.. figure:: images/developing/git_fork.png
    :align: left

    Click on the "fork" button to create your own Ray repository fork under your git username.
    You have to create a git account first in order to do so.

Download your fork to your local computer and change to the new ``ray`` directory.

.. code-block:: bash

    git clone https://github.com/[your git username]/ray
    cd ray

The RLlib team recommends using a python environment management tool, like Anaconda to be
able to switch between different python and package versions. Take a moment to make sure you are using such a tool
and have activate the correct environment, including the python version you would like to develop in.


Installing RLlib
----------------

Next, pip install `Ray <https://docs.ray.io/en/latest/>`__, `PyTorch <https://pytorch.org>`__,
and a few `Farama <https://gymnasium.farama.org>`__ dependencies to be able to run most of RLlib's examples and benchmarks:

.. code-block:: bash

    pip install "ray[rllib]" torch "gymnasium[atari,accept-rom-license,mujoco]"

The preceding commands install the latest release of Ray on your system. To further advance to the current master branch version,
copy the correct `installation link from here <https://docs.ray.io/en/master/ray-overview/installation.html#daily-releases-nightlies>`__,
depending on your system and python version and run:

.. code-block:: bash

    pip install -U [copied link from https://docs.ray.io/en/master/ray-overview/installation.html#daily-releases-nightlies]

Testing your installation
~~~~~~~~~~~~~~~~~~~~~~~~~

Run a quick check to find out whether the above installations were successful:

.. code-block:: bash

    $ python

    >>> from ray.rllib.algorithms.ppo import PPOConfig
    >>> ppo = PPOConfig().environment("CartPole-v1").build()
    >>> ppo.train()




Keeping your master-branch installation up to date
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You should pull from the master branch from time to time
In case you observe
.. todo (sven): Add here, how to setup RLlib to link into your pip installed ray and how to nuke the setup in case after a `git pull` weird errors unrelated to RLlib show up, b/c of other
libraries' updates causing problems.
- Make sure you have everything committed and pushed. Local branch should be free of any uncommitted changes.
- pip uninstall -y ray
- rm -rf [Anaconda ray directory] (if it's still there for some reason after the uninstall)
- pip install -U [pick right wheel from https://docs.ray.io/en/latest/ray-overview/installation.html#daily-releases-nightlies]
- git stash
- python python/ray/setup-dev.py (<- only say yes to RLlib, then get your CTRL+C out of there! :slightly_smiling_face: )




Setting up RLlib for development
--------------------------------

You can develop RLlib locally without needing to compile Ray by using the `setup-dev.py script <https://github.com/ray-project/ray/blob/master/python/ray/setup-dev.py>`__.
This sets up symlinks between the ``ray/rllib`` dir in your local git clone and the respective directory bundled with the pip-installed ``ray`` package.
This way, every change you make in the source files in your local git clone will immediately be reflected in your installed ``ray`` as well.

However if you have installed ray from source using `these instructions <https://docs.ray.io/en/master/ray-overview/installation.html>`__ then don't use this,
as these steps should have already created the necessary symlinks.

When using the `setup-dev.py script <https://github.com/ray-project/ray/blob/master/python/ray/setup-dev.py>`__,
make sure that your git branch is in sync with the installed Ray binaries, meaning you are up-to-date on `master <https://github.com/ray-project/ray>`__
and have the latest `wheel <https://docs.ray.io/en/master/ray-overview/installation.html#daily-releases-nightlies>`__ installed.

.. code-block:: bash

    # Clone your fork onto your local machine, e.g.:
    git clone https://github.com/[your username]/ray.git
    cd ray

    # Only enter 'Y' at the first question on linking RLlib.
    # This leads to the most stable behavior and you won't have to re-install Ray as often.
    # If you anticipate making changes to e.g. Tune or Train quite often, consider also symlinking Ray Tune or Train here
    # (say 'Y' when asked by the script about creating the Tune or Train symlinks).
    python python/ray/setup-dev.py



Modifying your own RLlib branch
-------------------------------



Contributing to RLlib and creating a pull request
-------------------------------------------------




Contributing Fixes and Enhancements
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Feel free to file new RLlib-related PRs through `Ray's github repo <https://github.com/ray-project/ray/pulls>`__.
The RLlib team is very grateful for any external help they can get from the open-source community. If you are unsure about how to structure your
bug-fix or enhancement-PRs, create a small PR first, then ask us questions within its conversation section.
`See here for an example of a good first community PR <https://github.com/ray-project/ray/pull/46317>`__.

Contributing Algorithms
~~~~~~~~~~~~~~~~~~~~~~~

These are the guidelines for merging new algorithms into RLlib.
We distinguish between two levels of contributions: As an `example script <https://github.com/ray-project/ray/tree/master/rllib/examples>`__
(possibly with additional classes in other files)
or as a fully-integrated RLlib Algorithm in `rllib/algorithms <https://github.com/ray-project/ray/tree/master/rllib/algorithms>`__.

* Example Algorithms:
    - must subclass Algorithm and implement the ``training_step()`` method
    - must include the main example script, in which the algo is demoed, in a CI test, which proves that the algo is learning a certain task.
    - should offer functionality not present in existing algorithms

* Fully integrated Algorithms have the following additional requirements:
    - must offer substantial new functionality not possible to add to other algorithms
    - should support custom RLModules
    - should use RLlib abstractions and support distributed execution
    - should include at least one `tuned hyperparameter example <https://github.com/ray-project/ray/tree/master/rllib/tuned_examples>`__, testing of which is part of the CI

Both integrated and contributed algorithms ship with the ``ray`` PyPI package, and are tested as part of Ray's automated tests.

New Features
~~~~~~~~~~~~

New feature developments, discussions, and upcoming priorities are tracked on the `GitHub issues page <https://github.com/ray-project/ray/issues>`__
(note that this may not include all development efforts).


API Stability
=============


API Decorators in the Codebase
------------------------------

Objects and methods annotated with ``@PublicAPI`` (new API stack),
``@DeveloperAPI`` (new API stack), or ``@OldAPIStack`` (old API stack)
have the following API compatibility guarantees:

.. autofunction:: ray.util.annotations.PublicAPI
    :noindex:

.. autofunction:: ray.util.annotations.DeveloperAPI
    :noindex:

.. autofunction:: ray.rllib.utils.annotations.OldAPIStack
    :noindex:


Benchmarks
==========

A number of training run results are available in the `rl-experiments repo <https://github.com/ray-project/rl-experiments>`__,
and there is also a list of working hyperparameter configurations in `tuned_examples <https://github.com/ray-project/ray/tree/master/rllib/tuned_examples>`__, sorted by algorithm.
Benchmark results are extremely valuable to the community, so if you happen to have results that may be of interest, consider making a pull request to either repo.




Debugging RLlib
---------------

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
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Keeping the memory usage of long running workers stable can be challenging. The ``MemoryTrackingCallbacks`` class can be used to track memory usage of workers.

.. autoclass:: ray.rllib.algorithms.callbacks.MemoryTrackingCallbacks

The objects with the top 20 memory usage in the workers are added as custom metrics. These can then be monitored using tensorboard or other metrics integrations like Weights & Biases:

.. image:: images/MemoryTrackingCallbacks.png


Troubleshooting
~~~~~~~~~~~~~~~

If you encounter errors like
`blas_thread_init: pthread_create: Resource temporarily unavailable` when using many workers,
try setting ``OMP_NUM_THREADS=1``. Similarly, check configured system limits with
`ulimit -a` for other resource limit errors.

For debugging unexpected hangs or performance problems, you can run ``ray stack`` to dump
the stack traces of all Ray workers on the current node, ``ray timeline`` to dump
a timeline visualization of tasks to a file, and ``ray memory`` to list all object
references in the cluster.


Episode traces
~~~~~~~~~~~~~~

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
~~~~~~~~~~~~~

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
~~~~~~~~~~~~

You can use the ``ray stack`` command to dump the stack traces of all the
Python workers on a single node. This can be useful for debugging unexpected
hangs or performance issues.


Next steps
----------

- To check how your application is doing, you can use the :ref:`Ray dashboard <observability-getting-started>`.
