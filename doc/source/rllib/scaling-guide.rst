.. from former `package_ref/algorithm_config.rst` file.

.. include:: /_includes/rllib/we_are_hiring.rst

.. include:: /_includes/rllib/new_api_stack.rst


.. _rllib-scaling-guide-docs:

RLlib Scaling Guide
===================

RLlib is a distributed and scalable RL library, based on Ray.



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

