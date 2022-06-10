.. include:: /_includes/rllib/announcement.rst

.. include:: /_includes/rllib/we_are_hiring.rst

.. TODO: We need trainers, environments, algorithms, policies, models here. Likely in that order.
    Execution plans are not a "core" concept for users. Sample batches should probably also be left out.

.. _rllib-core-concepts:

Key Concepts
============

On this page, we'll cover the key concepts to help you understand how RLlib works and how to use it.
In RLlib you use `trainers` to train `algorithms`.
These algorithms use `policies` to select actions for your agents.
Given a policy, `evaluation` of a policy produces `sample batches` of experiences.
You can also customize the `training_step` method of your RL experiments.

Trainers
--------

Trainers bring all RLlib components together, making algorithms accessible via RLlib's Python API and its command line interface (CLI).
They manage algorithm configuration, setup of the rollout workers and optimizer, and collection of training metrics.
Trainers also implement the :ref:`Tune Trainable API <tune-60-seconds>` for easy experiment management.

You have three ways to interact with a trainer. You can use the basic Python API or the command line to train it, or you
can use Ray Tune to tune hyperparameters of your reinforcement learning algorithm.
The following example shows three equivalent ways of interacting with the ``PPO`` Trainer,
which implements the proximal policy optimization algorithm in RLlib.

.. tabbed:: Basic RLlib Trainer

    .. code-block:: python

        trainer = PPO(env="CartPole-v0", config={"train_batch_size": 4000})
        while True:
            print(trainer.train())

.. tabbed:: RLlib Command Line

    .. code-block:: bash

        rllib train --run=PPO --env=CartPole-v0 --config='{"train_batch_size": 4000}'

.. tabbed:: RLlib Tune Trainer

    .. code-block:: python

        from ray import tune
        tune.run(PPO, config={"env": "CartPole-v0", "train_batch_size": 4000})



RLlib `Trainer classes <rllib-concepts.html#trainers>`__ coordinate the distributed workflow of running rollouts and optimizing policies.
Trainer classes leverage parallel iterators to implement the desired computation pattern.
The following figure shows *synchronous sampling*, the simplest of `these patterns <rllib-algorithms.html>`__:

.. figure:: images/a2c-arch.svg

    Synchronous Sampling (e.g., A2C, PG, PPO)

RLlib uses `Ray actors <actors.html>`__ to scale training from a single core to many thousands of cores in a cluster.
You can `configure the parallelism <rllib-training.html#specifying-resources>`__ used for training by changing the ``num_workers`` parameter.
Check out our `scaling guide <rllib-training.html#scaling-guide>`__ for more details here.


Policies
--------

`Policies <rllib-concepts.html#policies>`__ are a core concept in RLlib. In a nutshell, policies are
Python classes that define how an agent acts in an environment.
`Rollout workers <rllib-concepts.html#policy-evaluation>`__ query the policy to determine agent actions.
In a `gym <rllib-env.html#openai-gym>`__ environment, there is a single agent and policy.
In `vector envs <rllib-env.html#vectorized>`__, policy inference is for multiple agents at once,
and in `multi-agent <rllib-env.html#multi-agent-and-hierarchical>`__, there may be multiple policies,
each controlling one or more agents:

.. image:: images/multi-flat.svg

Policies can be implemented using `any framework <https://github.com/ray-project/ray/blob/master/rllib/policy/policy.py>`__.
However, for TensorFlow and PyTorch, RLlib has
`build_tf_policy <rllib-concepts.html#building-policies-in-tensorflow>`__ and
`build_torch_policy <rllib-concepts.html#building-policies-in-pytorch>`__ helper functions that let you
define a trainable policy with a functional-style API, for example:

.. TODO: test this code snippet

.. code-block:: python

  def policy_gradient_loss(policy, model, dist_class, train_batch):
      logits, _ = model.from_batch(train_batch)
      action_dist = dist_class(logits, model)
      return -tf.reduce_mean(
          action_dist.logp(train_batch["actions"]) * train_batch["rewards"])

  # <class 'ray.rllib.policy.tf_policy_template.MyTFPolicy'>
  MyTFPolicy = build_tf_policy(
      name="MyTFPolicy",
      loss_fn=policy_gradient_loss)


Policy Evaluation
-----------------

Given an environment and policy, policy evaluation produces `batches <https://github.com/ray-project/ray/blob/master/rllib/policy/sample_batch.py>`__ of experiences. This is your classic "environment interaction loop". Efficient policy evaluation can be burdensome to get right, especially when leveraging vectorization, RNNs, or when operating in a multi-agent environment. RLlib provides a `RolloutWorker <https://github.com/ray-project/ray/blob/master/rllib/evaluation/rollout_worker.py>`__ class that manages all of this, and this class is used in most RLlib algorithms.

You can use rollout workers standalone to produce batches of experiences. This can be done by calling ``worker.sample()`` on a worker instance, or ``worker.sample.remote()`` in parallel on worker instances created as Ray actors (see `WorkerSet <https://github.com/ray-project/ray/blob/master/rllib/evaluation/worker_set.py>`__).

Here is an example of creating a set of rollout workers and using them gather experiences in parallel. The trajectories are concatenated, the policy learns on the trajectory batch, and then we broadcast the policy weights to the workers for the next round of rollouts:

.. code-block:: python

    # Setup policy and rollout workers.
    env = gym.make("CartPole-v0")
    policy = CustomPolicy(env.observation_space, env.action_space, {})
    workers = WorkerSet(
        policy_class=CustomPolicy,
        env_creator=lambda c: gym.make("CartPole-v0"),
        num_workers=10)

    while True:
        # Gather a batch of samples.
        T1 = SampleBatch.concat_samples(
            ray.get([w.sample.remote() for w in workers.remote_workers()]))

        # Improve the policy using the T1 batch.
        policy.learn_on_batch(T1)

        # The local worker acts as a "parameter server" here.
        # We put the weights of its `policy` into the Ray object store once (`ray.put`)...
        weights = ray.put({"default_policy": policy.get_weights()})
        for w in workers.remote_workers():
            # ... so that we can broacast these weights to all rollout-workers once.
            w.set_weights.remote(weights)

Sample Batches
--------------

Whether running in a single process or a `large cluster <rllib-training.html#specifying-resources>`__,
all data in RLlib is interchanged in the form of `sample batches <https://github.com/ray-project/ray/blob/master/rllib/policy/sample_batch.py>`__.
Sample batches encode one or more fragments of a trajectory.
Typically, RLlib collects batches of size ``rollout_fragment_length`` from rollout workers, and concatenates one or
more of these batches into a batch of size ``train_batch_size`` that is the input to SGD.

A typical sample batch looks something like the following when summarized.
Since all values are kept in arrays, this allows for efficient encoding and transmission across the network:

.. code-block:: python

    sample_batch = { 'action_logp': np.ndarray((200,), dtype=float32, min=-0.701, max=-0.685, mean=-0.694),
        'actions': np.ndarray((200,), dtype=int64, min=0.0, max=1.0, mean=0.495),
        'dones': np.ndarray((200,), dtype=bool, min=0.0, max=1.0, mean=0.055),
        'infos': np.ndarray((200,), dtype=object, head={}),
        'new_obs': np.ndarray((200, 4), dtype=float32, min=-2.46, max=2.259, mean=0.018),
        'obs': np.ndarray((200, 4), dtype=float32, min=-2.46, max=2.259, mean=0.016),
        'rewards': np.ndarray((200,), dtype=float32, min=1.0, max=1.0, mean=1.0),
        't': np.ndarray((200,), dtype=int64, min=0.0, max=34.0, mean=9.14)
    }

In `multi-agent mode <rllib-concepts.html#policies-in-multi-agent>`__,
sample batches are collected separately for each individual policy.
These batches are wrapped up together in a ``MultiAgentBatch``,
serving as a container for the individual agents' sample batches.


Training Step Method
-------------------------

.. TODO all four execution plan snippets below must be tested
.. note::
    It's important to have a good understanding of the basic :ref:`ray core methods <core-walkthrough>` before reading this section.
    Furthermore, we utilize concepts such as the ``SampleBatch``, ``RolloutWorker``, and ``Trainer``, which can be read about on this page
    and the :ref:`rollout worker reference docs <rolloutworker-reference-docs>`.

    Finally, developers who are looking to implement custom algorithms should familiarize themselves with the :ref:`Policy <rllib-policy-walkthrough>` and
    :ref:`Model <rllib-models-walkthrough>` classes.

What is it?
~~~~~~~~~~~

The ``training_step`` method is an attribute of ``Trainer`` that dictates the execution logic of your algorithm. Specifically, it is used to express how you want to
coordinate the movement of samples and policy data across your distributed workers.

**A developer will need to modify this attribute of an algorithm if they want to
make some custom changes to an algorithm or write their own from scratch if they are implementing a new algorithm.**

When is it invoked?
~~~~~~~~~~~~~~~~~~~

``training_step`` is called in 2 ways:

 1. The ``train()`` method of ``Trainer`` is called.
 2. An RLlib trainer is being used with ray tune and ``training_step`` will be continuously called till the
    :ref:`ray tune stop criteria <tune-run-ref>` is met.

Key Subconcepts
~~~~~~~~~~~~~~~

The vanilla policy gradient algorithm can be thought of as a sequence of repeating steps, or *dataflow*, of:

 1. Sampling (to collect data from an env or offline data store)
 2. Updating (to learn a behavior).
 3. Broadcasting (to make sure all distributed units have the same weights again)
 4. Metrics reporting (returning relevant stats from the previous operations with regards to performance and runtime)

.. code-block:: python

    def training_step(self) -> ResultDict:
        # type: SampleBatchType
        train_batch = synchronous_parallel_sample(
                        worker_set=self.workers,
                        max_env_steps=self.config["train_batch_size"]
                    )

        # type: ResultDict
        train_results = train_one_step(self, train_batch)

        # Update worker weights with the weights from the trained
        # policy on the local worker.
        self.workers.sync_weights()

        return train_results

.. note::
    Note that the training iteration method is framework agnostic. This means that you don’t need to implement torch
    or tensorflow code inside this module. This allows the training iteration method / algorithm to support different frameworks.
    Within the :ref:`Policy <rllib-policy-walkthrough>` and :ref:`Model <rllib-models-walkthrough>` classes, we leverage the
    framework-specific operations and modules.

Breaking that ``training_step`` code down:

.. code-block:: python

    train_batch = synchronous_parallel_sample(
                        worker_set=self.workers,
                        max_env_steps=self.config["train_batch_size"]
                    )

``self.workers`` is a set of ``RolloutWorkers`` that are created for developers in ``Trainer``'s ``setup`` method.
This set is covered in greater depth on the :ref:`WorkerSet documentation page<workerset-reference-docs>`.
The method ``synchronous_parallel_sample`` is an RLlib utility that can be used for sampling in a blocking parallel
fashion across multiple rollout workers. RLlib includes other utilities, such as the ``AsyncRequestsManager``, for
facilitating the dataflow between various components in parallelizable fashion. They are covered in the :ref:`parallel
requests documentation <parallel-requests-docs>`.

.. code-block:: python

    train_results = train_one_step(self, train_batch)

Methods like ``train_one_step`` and ``multi_gpu_train_one_step`` can be used for training a policy. Further documentation
with examples on them can be found on the :ref:`train ops documentation page <train-ops-docs>`.

.. code-block:: python

    self.workers.sync_weights()

The training updates on the policy are applied to ``self.workers.local_worker``. By calling ``self.workers.sync_weights()``,
weights are broadcasted from the local worker to the remote workers if there are remote workers. See :ref:`rollout worker
reference docs <rolloutworker-reference-docs>` for further details.

.. code-block:: python

    return train_results

A dictionary is expected to be returned that contains the results of the training update.
It's generally recommended that the dictionary map keys of type ``str`` to values that are
of type ``float`` or to dictionaries of the same form, allowing for a nested structure.

For example, a results dictionary could map policy_ids to learning and sampling statistics for that policy:

.. code-block:: python

     {
        'agent_1': {
                      'learner_stats': {'policy_loss': 6.7291455},
                      'num_agent_steps_trained': 32.0},
        'agent_2': {
                     'learner_stats': {'policy_loss': 3.554927},
                     'num_agent_steps_trained': 32.0
                   }
     }

Training Step Method Utilities
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

RLlib provides a collection of utilities that abstract away common tasks in RL training.

`Sample Batch <core-concepts.html#sample-batches>`__:
``SampleBatch`` and ``MultiAgentBatch`` are the two datatypes that we use for containing timesteps in RLlib. All of our
RLlib abstractions (policies, replay buffers, etc.) operate using these types.

:ref:`Rollout Workers <rolloutworker-reference-docs>`:
Rollout workers are an abstraction that wraps a policy (or policies in the case of multi-agent) and an environment.
From a high level, we can use rollout workers to collect experiences from the environment by calling
their ``sample`` method and we can train their policies by calling their ``learn_on_batch`` method.
By default, in RLlib, we create a set of workers that can be used for sampling and training. We create a ``WorkerSet``
object inside of ``setup`` which is called when an RLlib algorithm is created. The ``WorkerSet`` has a ``local_worker``
and ``remote_workers`` if ``num_workers > 0`` in the experiment config. In RLlib we use typically use ``local_worker``
for training and ``remote_workers`` for sampling.


:ref:`Train Ops <train-ops-docs>`:
These are methods that improve the policy and update workers. The most basic operator, ``train_one_step``, takes in as
input a batch of experiences and emits a ``ResultDict`` with metrics as output. For training with GPUs, use
``multi_gpu_train_one_step``. These methods use the ``learn_on_batch`` method of rollout workers to complete the
training update.

:ref:`Replay Buffers < replay-buffer-reference-docs>`:
RLlib provides `a collection <https://github.com/ray-project/ray/tree/master/rllib/utils/replay_buffers>`__ of replay
buffers that can be used for storing and sampling experiences.

:ref:`Parallel Request Utilities <parallel-requests-docs>`:
RLlib provides a collection of concurrency ops that can be asynchronous and synchronous operations in the training loop.
``AsyncRequestsManager`` is used for launching and managing asynchronous requests on actors. Currently, in RLlib, it is
used for asynchronous sampling on rollout workers and asynchronously adding to and sampling from replay buffer actors.
``synchronous_parallel_sample`` has a more narrow but common usage of synchronously sampling from a set of rollout workers.

