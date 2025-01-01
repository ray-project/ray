.. from former `package_ref/algorithm_config.rst` file.

.. include:: /_includes/rllib/we_are_hiring.rst

.. include:: /_includes/rllib/new_api_stack.rst


.. _rllib-scaling-guide-docs:

RLlib Scaling Guide
===================

RLlib is a distributed and scalable RL library, based on Ray. RLlib's Algorithm makes use of Ray actors where ever parallelization
of its subcomponents can speed up sample and learning throughput.



Scaling the number of EnvRunner actors
--------------------------------------
You can control the degree of parallelism used by setting the ``num_env_runners``
hyperparameter for most algorithms. The Algorithm will construct that many
"remote worker" instances (`see RolloutWorker class <https://github.com/ray-project/ray/blob/master/rllib/evaluation/rollout_worker.py>`__)
that are constructed as ray.remote actors, plus exactly one "local worker", an ``EnvRunner`` object that isn't a
ray actor, but lives directly inside the Algorithm.

For most algorithms, learning updates are performed on the local worker and sample collection from
one or more environments is performed by the remote workers (in parallel).
For example, setting ``num_env_runners=0`` will only create the local worker, in which case both
sample collection and training will be done by the local worker.
On the other hand, setting ``num_env_runners=5`` will create the local worker (responsible for training updates)
and 5 remote workers (responsible for sample collection).


Scaling the number of envs per EnvRunner actor
----------------------------------------------





Scaling the number of Learner actors
------------------------------------

Since learning is most of the time done on the local worker, it may help to provide one or more GPUs
to that worker via the ``num_gpus`` setting.
Similarly, you can control the resource allocation to remote workers with ``num_cpus_per_env_runner``, ``num_gpus_per_env_runner``, and ``custom_resources_per_env_runner``.

The number of GPUs can be fractional quantities (for example, 0.5) to allocate only a fraction
of a GPU. For example, with DQN you can pack five algorithms onto one GPU by setting
``num_gpus: 0.2``. See `this fractional GPU example here <https://github.com/ray-project/ray/blob/master/rllib/examples/gpus/fractional_gpus.py>`__
as well that also demonstrates how environments (running on the remote workers) that
require a GPU can benefit from the ``num_gpus_per_env_runner`` setting.

For synchronous algorithms like PPO and A2C, the driver and workers can make use of
the same GPU. To do this for an amount of ``n`` GPUS:




.. code-block:: python

    gpu_count = n
    num_gpus = 0.0001 # Driver GPU
    num_gpus_per_env_runner = (gpu_count - num_gpus) / num_env_runners

.. Original image: https://docs.google.com/drawings/d/14QINFvx3grVyJyjAnjggOCEVN-Iq6pYVJ3jA2S6j8z0/edit?usp=sharing
.. image:: images/rllib-config.svg

If you specify ``num_gpus`` and your machine does not have the required number of GPUs
available, a RuntimeError will be thrown by the respective worker. On the other hand,
if you set ``num_gpus=0``, your policies will be built solely on the CPU, even if
GPUs are available on the machine.

.. automethod:: ray.rllib.algorithms.algorithm_config.AlgorithmConfig.resources
    :noindex:




Here are some rules of thumb for scaling training with RLlib.

1. If the environment is slow and cannot be replicated (e.g., since it requires interaction with physical systems), then you should use a sample-efficient off-policy algorithm such as :ref:`DQN <dqn>` or :ref:`SAC <sac>`. These algorithms default to ``num_env_runners: 0`` for single-process operation. Make sure to set ``num_gpus: 1`` if you want to use a GPU. Consider also batch RL training with the `offline data <rllib-offline.html>`__ API.

2. If the environment is fast and the model is small (most models for RL are), use time-efficient algorithms such as :ref:`PPO <ppo>`, or :ref:`IMPALA <impala>`.
These can be scaled by increasing ``num_env_runners`` to add rollout workers. It may also make sense to enable `vectorization <rllib-env.html#vectorized>`__ for
inference. Make sure to set ``num_gpus: 1`` if you want to use a GPU. If the learner becomes a bottleneck, you can use multiple GPUs for learning by setting
``num_gpus > 1``.

3. If the model is compute intensive (e.g., a large deep residual network) and inference is the bottleneck, consider allocating GPUs to workers by setting ``num_gpus_per_env_runner: 1``. If you only have a single GPU, consider ``num_env_runners: 0`` to use the learner GPU for inference. For efficient use of GPU time, use a small number of GPU workers and a large number of `envs per worker <rllib-env.html#vectorized>`__.

4. Finally, if both model and environment are compute intensive, then enable `remote worker envs <rllib-env.html#vectorized>`__ with `async batching <rllib-env.html#vectorized>`__ by setting ``remote_worker_envs: True`` and optionally ``remote_env_batch_wait_ms``. This batches inference on GPUs in the rollout workers while letting envs run asynchronously in separate actors, similar to the `SEED <https://ai.googleblog.com/2020/03/massively-scaling-reinforcement.html>`__ architecture. The number of workers and number of envs per worker should be tuned to maximize GPU utilization.

In case you are using lots of workers (``num_env_runners >> 10``) and you observe worker failures for whatever reasons, which normally interrupt your RLlib training runs, consider using
the config settings ``ignore_env_runner_failures=True``, ``restart_failed_env_runners=True``, or ``restart_failed_sub_environments=True``:

``restart_failed_env_runners``: When set to True (default), your Algorithm will attempt to restart any failed EnvRunner and replace it with a newly created one. This way, your number of workers will never decrease, even if some of them fail from time to time.
``ignore_env_runner_failures``: When set to True, your Algorithm will not crash due to an EnvRunner error, but continue for as long as there is at least one functional worker remaining. This setting is ignored when ``restart_failed_env_runners=True``.
``restart_failed_sub_environments``: When set to True and there is a failure in one of the vectorized sub-environments in one of your EnvRunners, RLlib tries to recreate only the failed sub-environment and re-integrate the newly created one into your vectorized env stack on that EnvRunner.

Note that only one of ``ignore_env_runner_failures`` or ``restart_failed_env_runners`` should be set to True (they are mutually exclusive settings). However,
you can combine each of these with the ``restart_failed_sub_environments=True`` setting.
Using these options will make your training runs much more stable and more robust against occasional OOM or other similar "once in a while" errors on the EnvRunners
themselves or inside your custom environments.





.. hint::

    There are other components and aspects in RLlib that should be able to scale up.
    For example, the model size is currently limited to what ever fits on a single GPU, due to the DPP-only
    nature of scaling RLlib's Learner actors.

    The Ray team is working on closing these gaps.

    Future areas of improvements:
    - Enable **training very large models**, such as a "large language model" (LLM). The team is actively working on a
      "Reinforcement Learning from Human Feedback" (RLHF) prototype setup. The main problems to solve are the
      model-parallel and tensor-parallel distribution across multiple GPUs, as well as, a reasonably fast transfer of
      weights between Ray actors.
    - Enable training with **1000s of multi-agent policies**. A possible solution for this scaling problem
      could be to split up the :py:class:`~ray.rllib.core.rl_module.multi_rl_module.MultiRLModule` into
      manageable groups of individual policies across the various :py:class:`~ray.rllib.env.env_runner.EnvRunner`
      and :py:class:`~ray.rllib.core.learner.learner.Learner` actors.
    - Enabling **vector envs for multi-agent**.
