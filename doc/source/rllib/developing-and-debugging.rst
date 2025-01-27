
.. include:: /_includes/rllib/we_are_hiring.rst

.. _rllib-developing-and-debugging-docs:

Developing with RLlib and debugging
===================================

.. include:: /_includes/rllib/new_api_stack.rst

This page teaches you how to set up Ray RLlib for development. It walks you through cloning your own GitHub fork,
installing Ray, setting up RLlib so you can modify and customize your own code, creating a pull request (PR),
debugging RLlib code, and clearing and re-installing Ray and RLlib in case of problems with your installation or setup.

Forking the git repository
--------------------------

First, create your own repository fork using your GitHub account.

.. figure:: images/developing/git_fork.png
    :align: left

    Click on the "fork" button to create your own Ray repository fork under your git username.
    To do so, you have to create a git account first.

Then, download the forked files to your local computer and change into the automatically created ``ray`` directory.

.. code-block:: bash

    git clone https://github.com/<your git username>/ray
    cd ray

The RLlib team recommends using a python environment management tool, like `Anaconda <https://www.anaconda.com/download>`__ to easily
switch between different python and package versions. Take a moment to make sure you are using such a tool
and have activate the correct development environment, including your preferred python version.

Pip installing Ray and RLlib
----------------------------

Next, pip install `Ray <https://docs.ray.io/en/latest/>`__, `PyTorch <https://pytorch.org>`__,
and a few `Farama <https://gymnasium.farama.org>`__ dependencies to be able to run most of RLlib's examples and benchmarks:

.. code-block:: bash

    pip install "ray[rllib]" torch "gymnasium[atari,accept-rom-license,mujoco]"

The preceding commands install the latest stable release of Ray on your system. To further advance to Ray's master branch,
copy the correct `installation link from here <https://docs.ray.io/en/master/ray-overview/installation.html#daily-releases-nightlies>`__,
depending on your system and python version and run the following commands:

.. code-block:: bash

    pip install -U <copied link from https://docs.ray.io/en/master/ray-overview/installation.html#daily-releases-nightlies>

.. note::
    You now have installed Ray through ``pip``, however, the installed package has no connection whatsoever with the
    downloaded source files from your github fork. To link them into one installation, in which you can edit source files
    and immediately see the effects of your changes for development and debugging purposes,
    :ref:`see the next paragraph here<rllib-developing-and-debugging-setup-dev>`.

.. _rllib-developing-and-debugging-setup-dev:

Setting up RLlib for development
--------------------------------

You can develop RLlib and edit its source files locally without compiling Ray through using the
`setup-dev.py script <https://github.com/ray-project/ray/blob/master/python/ray/setup-dev.py>`__:

.. code-block:: bash

    $ python python/ray/setup-dev.py
    NOTE: Use '-y' to override all python files without confirmation.
    This will replace:
    /Users/sven/anaconda3/envs/ray2/lib/python3.11/site-packages/ray/rllib
    with a symlink to:
    /Users/sven/Dropbox/Projects/ray/rllib [Y/n]:

Enter `Y` on the prompt, then abort the script through pressing ``CTRL + C`` repeatedly until you return to the command prompt.

This sets up symbolic links between the ``ray/rllib`` dir of your local git clone and the respective directory bundled with the pip-installed ``ray`` package.
This way, every change you make in the source files in your local git clone is immediately reflected in your installed ``ray`` as well.

.. note::
    If you have installed ray from source using `these instructions here <https://docs.ray.io/en/master/ray-overview/installation.html>`__,
    don't run the ``setup-dev.py`` script, because this should have already created the necessary symbolic links.

Testing your installation
~~~~~~~~~~~~~~~~~~~~~~~~~

Run a quick check on whether the preceding pip-installation and git repository symbolic linking  was successful:

.. code-block:: bash

    $ python

    >>> from ray.rllib.algorithms.ppo import PPOConfig
    >>> ppo = PPOConfig().environment("CartPole-v1").build()
    >>> ppo.train()

The call to ``train()`` should result in a large print out of training results in your console.

Keeping your master-branch and installation up to date
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When using the `setup-dev.py script <https://github.com/ray-project/ray/blob/master/python/ray/setup-dev.py>`__,
make sure that your git branch is in sync with the installed Ray binaries, meaning you are up-to-date on `master <https://github.com/ray-project/ray>`__
and have the latest `wheel <https://docs.ray.io/en/master/ray-overview/installation.html#daily-releases-nightlies>`__ installed.

The Ray team recommends to set up your own Ray fork on your local computer as ``origin`` and the Ray team's main repository as ``upstream``:

.. code-block:: bash

    git remote add origin https://github.com/[your git username]/ray
    git remote add upstream https://github.com/ray-project/ray
    git fetch

From time to time you should pull from the Ray team's `master branch <https://github.com/ray-project/ray>`__, no matter, which
PR or branch you are currently developing in. The Ray repository moves very fast and the team may merge several dozen changes into it every day:

Run the following whenever you want to continue developing on your current branch or PR:

.. code-block:: bash

    git pull upstream master


Cleaning up and reinstalling your setup
+++++++++++++++++++++++++++++++++++++++

In case you observe strange error messages that are coming from parts of the code you haven't altered, you may have to clean up and reinstall
Ray in your environment. These errors might come from Ray libraries, other than RLlib, that the Ray team has recently changed and that are now
conflicting with either the pip-installed Ray or with RLlib's source code.

.. warning::
    Before you perform the following steps, make sure you have everything committed and pushed to git. Your local branch should be free
    of any uncommitted changes.

.. code-block:: bash

    pip uninstall -y ray
    rm -rf <your pip-installed ray directory>  # only, if it's still there for some reason after the uninstall
    pip install -U <pick right wheel from https://docs.ray.io/en/master/ray-overview/installation.html#daily-releases-nightlies>
    git stash
    python python/ray/setup-dev.py  # <- only say `Y` to linking RLlib, then press CTRL+C to abort the script


Contributing to RLlib and creating a pull request
-------------------------------------------------

The Ray team is grateful for any external help we can get from the open source community in maintaining and developing RLlib.
Feel free to file new RLlib-related PRs through `Ray's github repo <https://github.com/ray-project/ray/pulls>`__.

To create a pull request, branch out from ``master``, give your new branch a meaningful name:

.. code-block:: bash

    git checkout -b <name of your new branch>
    git branch  # verify your new branch

Then start editing RLlib's source files and run your scripts.
When you have finished coding, commit your changes and push them back to your forked ``origin`` repository of Ray.

.. code-block:: bash

    git push -u origin <name of your new branch>

Click this link here to `create a new pull request on the github website <https://github.com/ray-project/ray/pulls/>`__.
Git should highlight the pushed branch in yellow:

.. figure:: images/developing/git_pr.png
    :align: left

    Click on the "Compare and pull request" button to start the pull request (PR) process.
    Fill out the form on the next page and submit your PR for review by the Ray team.

Contributing Fixes and Enhancements
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If you are unsure about how to structure your bug-fix or enhancement PRs, create a smaller PR first,
then ask the team questions within the PRs conversation section.

See here for an `example of a good first community PR <https://github.com/ray-project/ray/pull/46317>`__.

New Features
++++++++++++

The Ray team tracks new feature developments, discussions, and upcoming priorities on the
`GitHub issues page <https://github.com/ray-project/ray/issues>`__.
Note that this may not include all development efforts.

Contributing Algorithms
+++++++++++++++++++++++

The guidelines for merging new algorithms into RLlib depend on the level of contribution you would like to make:

* `Example Algorithms <https://github.com/ray-project/ray/tree/master/rllib/examples/algorithms>`__:
    - must subclass :py:class:`~ray.rllib.algorithms.algorithm.Algorithm` and implement the :py:meth:`~ray.rllib.algorithms.algorithm.Algorithm.training_step` method.
    - must include an actual `example script <https://github.com/ray-project/ray/tree/master/rllib/examples/algorithms>`__, which configures and runs the algorithm.
      Add this script to the `BUILD file <https://github.com/ray-project/ray/tree/master/rllib/BUILD>`__ to run as a CI test, proving that the algorithm can learn a certain task.
    - must offer features not present in existing algorithms.

* `Fully integrated Algorithms <https://github.com/ray-project/ray/tree/master/rllib/algorithms>`__ also require the following:
    - must offer a substantial new capability not possible to add to other algorithms.
    - must use RLlib abstractions and support distributed execution on the :py:class:`~ray.rllib.env.env_runner.EnvRunner` axis and :py:class:`~ray.rllib.core.learner.learner.Learner` axis.
      The respective config settings are ``config.env_runners(num_env_runners=...)`` and ``config.learners(num_learners=...)``
    - add your algorithm to the :ref:`algorithms overview page <rllib-algorithms-doc>` and describe it in detail.
    - must support custom :py:class:`~ray.rllib.core.rl_module.rl_module.RLModule` classes.
    - must include at least one `tuned hyperparameter example <https://github.com/ray-project/ray/tree/master/rllib/tuned_examples>`__,
      which you add to the `BUILD file <https://github.com/ray-project/ray/tree/master/rllib/BUILD>`__ for automated CI-testing.

Both example- and fully integrated algorithms ship with the ``ray`` PyPI package and the Ray CI system tests these automatically.

API Stability
+++++++++++++

Objects and methods annotated with ``@PublicAPI``, ``@DeveloperAPI``, or ``@OldAPIStack`` have the following
API compatibility guarantees:

.. autofunction:: ray.util.annotations.PublicAPI
    :noindex:

.. autofunction:: ray.util.annotations.DeveloperAPI
    :noindex:

.. autofunction:: ray.rllib.utils.annotations.OldAPIStack
    :noindex:

Debugging RLlib
---------------

The fastest way to find and fix bugs in RLlib and your custom code is to use a locally installed IDE,
such as `PyCharm <https://www.jetbrains.com/pycharm/>`__ or `VS Code <https://code.visualstudio.com/>`__.
The RLlib team recommends to install either one of these software first, before you start your journey into developing.

Local debugging
~~~~~~~~~~~~~~~

Despite Ray's and RLlib's distributed architectures and the fact that they best unfold all of their potential in production on large,
multi-node clusters, it's often helpful to start running your programs locally on your laptop or desktop machine and see,
whether they roughly work as intended. Even if your local setup doesn't have some compute resources that are
crucial for the actual training to succeed, most bugs already surface in the simplest of setups, for example in a single,
local process running on the CPU.

Thus, before anything else, change your config for running an RLlib program in such a local setup:

.. testcode::

    from ray.rllib.algorithms.ppo import PPOConfig

    config = (
        PPOConfig()
        .environment("Acrobot-v1")

        # Force one local EnvRunner for maximum simplicity on the EnvRunner axis.
        # This EnvRunner instance is part of the main Algorithm process and doesn't run as a ray remote actor.
        .env_runners(num_env_runners=0)

        # Force a local CPU-bound Learner for maximum simplicity on the Learner axis.
        # This Learner instance is part of the main Algorithm process and doesn't run as a ray remote actor.
        .learners(num_learners=0, num_gpus_per_learner=0)
    )


With the preceding config, you can debug your Algorithm code through your IDE's visual debugger.
For example, go to the `PPO training_step() <https://github.com/ray-project/ray/blob/master/rllib/algorithms/ppo/ppo.py>`__ method
implementation and set a breakpoint somewhere inside it:

.. figure:: images/developing/pycharm_breakpoint_ppo.png
    :align: left

    **A breakpoint set inside PPO's ``training_step()`` method**: When running your script without Ray Tune, your IDE should pause
    execution at that location in the code in each iteration.

Your script stops at the breakpoint and you can look into variable values, execute custom python statements in the debugging
console, or go back in the call stack to the callers of ``training_step()``:

.. figure:: images/developing/pycharm_debugging_variables.png
    :align: left

    **Looking at variables at a breakpoint**: When execution pauses because of a breakpoint,
    you can look into variable values, go back to parent callers in the call stack, or execute custom
    code.


Logging with Weights and Biases (WandB)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

After finding bugs in your code and making sure your script runs without throwing errors,
the next problem for you to tackle is typically the learning behavior of your algorithm, models,
and RL environment.

A good place to start are the metrics RLlib provides by default. Each call to the
:py:meth:`~ray.rllib.algorithms.algorithm.Algorithm.train` method returns a result dict,
which Ray Tune may send to `Weights and Biases <https://wandb.ai/>`__, if configured.

.. code-block:: python

    from ray import train, tune
    from ray.air.integrations.wandb import WandbLoggerCallback

    config = ...  # <- your RLlib algorithm config

    results = tune.Tuner(
        config.algo_class,
        param_space=config,
        run_config=train.RunConfig(
            stop=stop,
            verbose=2,  # value between 0 and 3
            callbacks=[  # configure the WandB logger for this experiment
                WandbLoggerCallback(
                    api_key=[your WandB API key],
                    project=[WandB project name],
                    upload_checkpoints=True,
                    name=[your WandB run name for this experiment],
                )
            ],
        ),
    ).fit()


Structure of RLlib result dicts
+++++++++++++++++++++++++++++++

RLlib structures training results by subcomponents, such as
:py:class:`~ray.rllib.env.env_runner.EnvRunner` or :py:class:`~ray.rllib.core.learner.learner.Learner`,
but you can also find timer information and global counts under the respective sub-keys:

.. code-block:: text

    {
     # your RLlib algorithm config
     'config': { ... },
     'date': '2025-01-27_14-46-33',

     # EnvRunnerGroup.
     'env_runner_group': {
         'actor_manager_num_outstanding_async_reqs': 0
         'num_healthy_env_runners': 2,
         'num_remote_env_runner_restarts': 0},

     # Individual EnvRunners (reduced over parallel EnvRunner actors).
     'env_runners': {'agent_episode_returns_mean': {'default_agent': 19.92},
                     'episode_duration_sec_mean': 0.004189562468091026,
                     'episode_len_mean': 19.92,
                     'episode_return_mean': 19.92,
                     'module_episode_returns_mean': {'default_policy': 19.92},
                     ...
                     'weights_seq_no': 0.0},

     # Individual Learners (by ModuleID).
     'learners': {'__all_modules__': {'num_env_steps_trained_lifetime': 4190,
                                      'num_non_trainable_parameters': 0,
                                      'num_trainable_parameters': 259},
                  'default_policy': {'curr_entropy_coeff': 0.0,
                                     'curr_kl_coeff': 0.20000000298023224,
                                     'default_optimizer_learning_rate': 0.0003,
                                     'entropy': 0.6837567687034607,
                                     'mean_kl_loss': 0.01698029786348343,
                                     'module_train_batch_size_mean': 4190,
                                     'num_module_steps_trained_lifetime': 4190,
                                     'num_non_trainable_parameters': 0,
                                     'num_trainable_parameters': 259,
                                     'policy_loss': -0.1476544886827469,
                                     'total_loss': -0.05217256397008896,
                                     'vf_loss': 9.208584785461426,
                                     'weights_seq_no': 1.0}},

     # Algorithm timers (mostly directly from within `training_step()`).
     'timers': {'env_runner_sampling_timer': 6.536753125023097,
                'learner_update_timer': 0.5848377080401406,
                'synch_weights': 0.0052911670645698905,
                'training_iteration': 7.127688749926165,
                'training_step': 7.127490875078365},

     # Other global stats: Lifetime counts, performance, timers, etc.
     'num_env_steps_sampled_lifetime': 4000,
     'num_training_step_calls_per_iteration': 1,
     'perf': {'cpu_util_percent': 11.118181818181817,
              'ram_util_percent': 47.72727272727273},
     'timestamp': 1737985593,
     'training_iteration': 1,
    }


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

You can control the log level through the ``--log_level`` command line argument in most of RLlib's example and tuned example scripts.
Valid values are ``DEBUG``, ``INFO``, ``WARN``, and ``ERROR``. Use different verbosity levels to increase or decrease the information content of your logs.

The default log level is ``WARN``, but you should use at least the ``INFO`` level logging for development.

For example:

.. code-block:: bash

    cd rllib/tuned_examples/ppo
    python atari_ppo.py --env ALE/Pong-v5 --log-level DEBUG

To set the log level in your :py:class:`~ray.rllib.algorithms.algorithm.Algorithm.AlgorithmConfig` instance,
use the :py:meth:`~ray.rllib.algorithms.algorithm_config.AlgorithmConfig.debugging` method and set the ``log_level`` argument in there:

.. code-block:: python

    config.debugging(log_level="INFO")

Stack traces
~~~~~~~~~~~~

You can use the ``ray stack`` command to dump the stack traces of all the Python workers on a single node. This can be useful for debugging unexpected
hangs or performance issues.

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

Next steps
----------

- To check how your application is doing, you can use the :ref:`Ray dashboard <observability-getting-started>`.
