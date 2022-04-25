.. include:: /_includes/rllib/announcement.rst

.. include:: /_includes/rllib/we_are_hiring.rst

Training APIs
=============

Getting Started
---------------

At a high level, RLlib provides an ``Trainer`` class which
holds a policy for environment interaction. Through the trainer interface, the policy can
be trained, checkpointed, or an action computed. In multi-agent training, the trainer manages the querying and optimization of multiple policies at once.

.. image:: images/rllib-api.svg

You can train a simple DQN trainer with the following commands:

.. code-block:: bash

    pip install "ray[rllib]" tensorflow
    rllib train --run DQN --env CartPole-v0  # --config '{"framework": "tf2", "eager_tracing": true}' for eager execution

By default, the results will be logged to a subdirectory of ``~/ray_results``.
This subdirectory will contain a file ``params.json`` which contains the
hyperparameters, a file ``result.json`` which contains a training summary
for each episode and a TensorBoard file that can be used to visualize
training process with TensorBoard by running

.. code-block:: bash

     tensorboard --logdir=~/ray_results

The ``rllib train`` command (same as the ``train.py`` script in the repo) has a number of options you can show by running:

.. code-block:: bash

    rllib train --help
    -or-
    python ray/rllib/train.py --help

The most important options are for choosing the environment
with ``--env`` (any OpenAI gym environment including ones registered by the user
can be used) and for choosing the algorithm with ``--run``
(available options include ``SAC``, ``PPO``, ``PG``, ``A2C``, ``A3C``, ``IMPALA``, ``ES``, ``DDPG``, ``DQN``, ``MARWIL``, ``APEX``, and ``APEX_DDPG``).

Evaluating Trained Policies
~~~~~~~~~~~~~~~~~~~~~~~~~~~

In order to save checkpoints from which to evaluate policies,
set ``--checkpoint-freq`` (number of training iterations between checkpoints)
when running ``rllib train``.


An example of evaluating a previously trained DQN policy is as follows:

.. code-block:: bash

    rllib rollout \
        ~/ray_results/default/DQN_CartPole-v0_0upjmdgr0/checkpoint_1/checkpoint-1 \
        --run DQN --env CartPole-v0 --steps 10000

The ``rollout.py`` helper script reconstructs a DQN policy from the checkpoint
located at ``~/ray_results/default/DQN_CartPole-v0_0upjmdgr0/checkpoint_1/checkpoint-1``
and renders its behavior in the environment specified by ``--env``.

(Type ``rllib rollout --help`` to see the available evaluation options.)

For more advanced evaluation functionality, refer to `Customized Evaluation During Training <#customized-evaluation-during-training>`__.

Configuration
-------------

Specifying Parameters
~~~~~~~~~~~~~~~~~~~~~

Each algorithm has specific hyperparameters that can be set with ``--config``, in addition to a number of
`common hyperparameters <https://github.com/ray-project/ray/blob/master/rllib/agents/trainer.py>`__
(soon to be replaced by `TrainerConfig objects <https://github.com/ray-project/ray/blob/master/rllib/agents/trainer_config.py>`__).

See the `algorithms documentation <rllib-algorithms.html>`__ for more information.

In an example below, we train A2C by specifying 8 workers through the config flag.

.. code-block:: bash

    rllib train --env=PongDeterministic-v4 --run=A2C --config '{"num_workers": 8}'

Specifying Resources
~~~~~~~~~~~~~~~~~~~~

You can control the degree of parallelism used by setting the ``num_workers``
hyperparameter for most algorithms. The Trainer will construct that many
"remote worker" instances (`see RolloutWorker class <https://github.com/ray-project/ray/blob/master/rllib/evaluation/rollout_worker.py>`__)
that are constructed as ray.remote actors, plus exactly one "local worker", a ``RolloutWorker`` object that is not a
ray actor, but lives directly inside the Trainer.
For most algorithms, learning updates are performed on the local worker and sample collection from
one or more environments is performed by the remote workers (in parallel).
For example, setting ``num_workers=0`` will only create the local worker, in which case both
sample collection and training will be done by the local worker.
On the other hand, setting ``num_workers=5`` will create the local worker (responsible for training updates)
and 5 remote workers (responsible for sample collection).

Since learning is most of the time done on the local worker, it may help to provide one or more GPUs
to that worker via the ``num_gpus`` setting.
Similarly, the resource allocation to remote workers can be controlled via ``num_cpus_per_worker``, ``num_gpus_per_worker``, and ``custom_resources_per_worker``.

The number of GPUs can be fractional quantities (e.g. 0.5) to allocate only a fraction
of a GPU. For example, with DQN you can pack five trainers onto one GPU by setting
``num_gpus: 0.2``. Check out `this fractional GPU example here <https://github.com/ray-project/ray/blob/master/rllib/examples/fractional_gpus.py>`__
as well that also demonstrates how environments (running on the remote workers) that
require a GPU can benefit from the ``num_gpus_per_worker`` setting.

For synchronous algorithms like PPO and A2C, the driver and workers can make use of
the same GPU. To do this for an amount of ``n`` GPUS:

.. code-block:: python

    gpu_count = n
    num_gpus = 0.0001 # Driver GPU
    num_gpus_per_worker = (gpu_count - num_gpus) / num_workers

.. Original image: https://docs.google.com/drawings/d/14QINFvx3grVyJyjAnjggOCEVN-Iq6pYVJ3jA2S6j8z0/edit?usp=sharing
.. image:: images/rllib-config.svg

If you specify ``num_gpus`` and your machine does not have the required number of GPUs
available, a RuntimeError will be thrown by the respective worker. On the other hand,
if you set ``num_gpus=0``, your policies will be built solely on the CPU, even if
GPUs are available on the machine.

Scaling Guide
~~~~~~~~~~~~~

.. _rllib-scaling-guide:

Here are some rules of thumb for scaling training with RLlib.

1. If the environment is slow and cannot be replicated (e.g., since it requires interaction with physical systems), then you should use a sample-efficient off-policy algorithm such as :ref:`DQN <dqn>` or :ref:`SAC <sac>`. These algorithms default to ``num_workers: 0`` for single-process operation. Make sure to set ``num_gpus: 1`` if you want to use a GPU. Consider also batch RL training with the `offline data <rllib-offline.html>`__ API.


2. If the environment is fast and the model is small (most models for RL are), use time-efficient algorithms such as :ref:`PPO <ppo>`, :ref:`IMPALA <impala>`, or :ref:`APEX <apex>`. These can be scaled by increasing ``num_workers`` to add rollout workers. It may also make sense to enable `vectorization <rllib-env.html#vectorized>`__ for inference. Make sure to set ``num_gpus: 1`` if you want to use a GPU. If the learner becomes a bottleneck, multiple GPUs can be used for learning by setting ``num_gpus > 1``.


3. If the model is compute intensive (e.g., a large deep residual network) and inference is the bottleneck, consider allocating GPUs to workers by setting ``num_gpus_per_worker: 1``. If you only have a single GPU, consider ``num_workers: 0`` to use the learner GPU for inference. For efficient use of GPU time, use a small number of GPU workers and a large number of `envs per worker <rllib-env.html#vectorized>`__.


4. Finally, if both model and environment are compute intensive, then enable `remote worker envs <rllib-env.html#vectorized>`__ with `async batching <rllib-env.html#vectorized>`__ by setting ``remote_worker_envs: True`` and optionally ``remote_env_batch_wait_ms``. This batches inference on GPUs in the rollout workers while letting envs run asynchronously in separate actors, similar to the `SEED <https://ai.googleblog.com/2020/03/massively-scaling-reinforcement.html>`__ architecture. The number of workers and number of envs per worker should be tuned to maximize GPU utilization. If your env requires GPUs to function, or if multi-node SGD is needed, then also consider :ref:`DD-PPO <ddppo>`.


In case you are using lots of workers (``num_workers >> 10``) and you observe worker failures for whatever reasons, which normally interrupt your RLlib training runs, consider using
the config settings ``ignore_worker_failures=True`` or ``recreate_failed_workers=True``:

``ignore_worker_failures=True`` allows your Trainer to not crash due to a single worker error, but to continue for as long as there is at least one functional worker remaining.
``recreate_failed_workers=True`` will have your Trainer attempt to replace/recreate any failed worker(s) with a new one.

Both these settings will make your training runs much more stable and more robust against occasional OOM or other similar "once in a while" errors.


Common Parameters
~~~~~~~~~~~~~~~~~

.. tip::
    Plain python config dicts will soon be replaced by :py:class:`~ray.rllib.agents.trainer_config.TrainerConfig`
    objects, which have the advantage of being type safe, allowing users to set different config settings within
    meaningful sub-categories (e.g. ``my_config.training(lr=0.0003)``), and offer the ability to
    construct a Trainer instance from these config objects (via their ``build()`` method).
    So far, this is only supported for the :py:class:`~ray.rllib.agents.ppo.ppo.PPOTrainer`.

The following is a list of the common algorithm hyperparameters:

.. literalinclude:: ../../../rllib/agents/trainer.py
   :language: python
   :start-after: __sphinx_doc_begin__
   :end-before: __sphinx_doc_end__

Tuned Examples
~~~~~~~~~~~~~~

Some good hyperparameters and settings are available in
`the repository <https://github.com/ray-project/ray/blob/master/rllib/tuned_examples>`__
(some of them are tuned to run on GPUs). If you find better settings or tune
an algorithm on a different domain, consider submitting a Pull Request!

You can run these with the ``rllib train`` command as follows:

.. code-block:: bash

    rllib train -f /path/to/tuned/example.yaml

Basic Python API
----------------

The Python API provides the needed flexibility for applying RLlib to new problems. You will need to use this API if you wish to use `custom environments, preprocessors, or models <rllib-models.html>`__ with RLlib.

Here is an example of the basic usage (for a more complete example, see `custom_env.py <https://github.com/ray-project/ray/blob/master/rllib/examples/custom_env.py>`__):

.. code-block:: python

    import ray
    import ray.rllib.agents.ppo as ppo
    from ray.tune.logger import pretty_print

    ray.init()
    config = ppo.DEFAULT_CONFIG.copy()
    config["num_gpus"] = 0
    config["num_workers"] = 1
    trainer = ppo.PPOTrainer(config=config, env="CartPole-v0")

    # Can optionally call trainer.restore(path) to load a checkpoint.

    for i in range(1000):
       # Perform one iteration of training the policy with PPO
       result = trainer.train()
       print(pretty_print(result))

       if i % 100 == 0:
           checkpoint = trainer.save()
           print("checkpoint saved at", checkpoint)

    # Also, in case you have trained a model outside of ray/RLlib and have created
    # an h5-file with weight values in it, e.g.
    # my_keras_model_trained_outside_rllib.save_weights("model.h5")
    # (see: https://keras.io/models/about-keras-models/)

    # ... you can load the h5-weights into your Trainer's Policy's ModelV2
    # (tf or torch) by doing:
    trainer.import_model("my_weights.h5")
    # NOTE: In order for this to work, your (custom) model needs to implement
    # the `import_from_h5` method.
    # See https://github.com/ray-project/ray/blob/master/rllib/tests/test_model_imports.py
    # for detailed examples for tf- and torch trainers/models.

.. note::

    It's recommended that you run RLlib trainers with :doc:`Tune <../tune/index>`, for easy experiment management and visualization of results. Just set ``"run": ALG_NAME, "env": ENV_NAME`` in the experiment config.

All RLlib trainers are compatible with the :ref:`Tune API <tune-60-seconds>`. This enables them to be easily used in experiments with :doc:`Tune <../tune/index>`. For example, the following code performs a simple hyperparam sweep of PPO:

.. code-block:: python

    import ray
    from ray import tune

    ray.init()
    tune.run(
        "PPO",
        stop={"episode_reward_mean": 200},
        config={
            "env": "CartPole-v0",
            "num_gpus": 0,
            "num_workers": 1,
            "lr": tune.grid_search([0.01, 0.001, 0.0001]),
        },
    )

Tune will schedule the trials to run in parallel on your Ray cluster:

::

    == Status ==
    Using FIFO scheduling algorithm.
    Resources requested: 4/4 CPUs, 0/0 GPUs
    Result logdir: ~/ray_results/my_experiment
    PENDING trials:
     - PPO_CartPole-v0_2_lr=0.0001:	PENDING
    RUNNING trials:
     - PPO_CartPole-v0_0_lr=0.01:	RUNNING [pid=21940], 16 s, 4013 ts, 22 rew
     - PPO_CartPole-v0_1_lr=0.001:	RUNNING [pid=21942], 27 s, 8111 ts, 54.7 rew

``tune.run()`` returns an ExperimentAnalysis object that allows further analysis of the training results and retrieving the checkpoint(s) of the trained agent.
It also simplifies saving the trained agent. For example:

.. code-block:: python

    # tune.run() allows setting a custom log directory (other than ``~/ray-results``)
    # and automatically saving the trained agent
    analysis = ray.tune.run(
        ppo.PPOTrainer,
        config=config,
        local_dir=log_dir,
        stop=stop_criteria,
        checkpoint_at_end=True)

    # list of lists: one list per checkpoint; each checkpoint list contains
    # 1st the path, 2nd the metric value
    checkpoints = analysis.get_trial_checkpoints_paths(
        trial=analysis.get_best_trial("episode_reward_mean"),
        metric="episode_reward_mean")

    # or simply get the last checkpoint (with highest "training_iteration")
    last_checkpoint = analysis.get_last_checkpoint()
    # if there are multiple trials, select a specific trial or automatically
    # choose the best one according to a given metric
    last_checkpoint = analysis.get_last_checkpoint(
        metric="episode_reward_mean", mode="max"
    )

Loading and restoring a trained agent from a checkpoint is simple:

.. code-block:: python

    agent = ppo.PPOTrainer(config=config, env=env_class)
    agent.restore(checkpoint_path)


Computing Actions
~~~~~~~~~~~~~~~~~

The simplest way to programmatically compute actions from a trained agent is to use ``trainer.compute_action()``.
This method preprocesses and filters the observation before passing it to the agent policy.
Here is a simple example of testing a trained agent for one episode:

.. code-block:: python

    # instantiate env class
    env = env_class(env_config)

    # run until episode ends
    episode_reward = 0
    done = False
    obs = env.reset()
    while not done:
        action = agent.compute_action(obs)
        obs, reward, done, info = env.step(action)
        episode_reward += reward

For more advanced usage, you can access the ``workers`` and policies held by the trainer
directly as ``compute_action()`` does:

.. code-block:: python

  class Trainer(Trainable):

    @PublicAPI
    def compute_action(self,
                       observation,
                       state=None,
                       prev_action=None,
                       prev_reward=None,
                       info=None,
                       policy_id=DEFAULT_POLICY_ID,
                       full_fetch=False):
        """Computes an action for the specified policy.

        Note that you can also access the policy object through
        self.get_policy(policy_id) and call compute_actions() on it directly.

        Arguments:
            observation (obj): observation from the environment.
            state (list): RNN hidden state, if any. If state is not None,
                          then all of compute_single_action(...) is returned
                          (computed action, rnn state, logits dictionary).
                          Otherwise compute_single_action(...)[0] is
                          returned (computed action).
            prev_action (obj): previous action value, if any
            prev_reward (int): previous reward, if any
            info (dict): info object, if any
            policy_id (str): policy to query (only applies to multi-agent).
            full_fetch (bool): whether to return extra action fetch results.
                This is always set to true if RNN state is specified.

        Returns:
            Just the computed action if full_fetch=False, or the full output
            of policy.compute_actions() otherwise.
        """

        if state is None:
            state = []
        preprocessed = self.workers.local_worker().preprocessors[
            policy_id].transform(observation)
        filtered_obs = self.workers.local_worker().filters[policy_id](
            preprocessed, update=False)
        if state:
            return self.get_policy(policy_id).compute_single_action(
                filtered_obs,
                state,
                prev_action,
                prev_reward,
                info,
                clip_actions=self.config["clip_actions"])
        res = self.get_policy(policy_id).compute_single_action(
            filtered_obs,
            state,
            prev_action,
            prev_reward,
            info,
            clip_actions=self.config["clip_actions"])
        if full_fetch:
            return res
        else:
            return res[0]  # backwards compatibility


Accessing Policy State
~~~~~~~~~~~~~~~~~~~~~~
It is common to need to access a trainer's internal state, e.g., to set or get internal weights. In RLlib trainer state is replicated across multiple *rollout workers* (Ray actors) in the cluster. However, you can easily get and update this state between calls to ``train()`` via ``trainer.workers.foreach_worker()`` or ``trainer.workers.foreach_worker_with_index()``. These functions take a lambda function that is applied with the worker as an arg. You can also return values from these functions and those will be returned as a list.

You can also access just the "master" copy of the trainer state through ``trainer.get_policy()`` or ``trainer.workers.local_worker()``, but note that updates here may not be immediately reflected in remote replicas if you have configured ``num_workers > 0``. For example, to access the weights of a local TF policy, you can run ``trainer.get_policy().get_weights()``. This is also equivalent to ``trainer.workers.local_worker().policy_map["default_policy"].get_weights()``:

.. code-block:: python

    # Get weights of the default local policy
    trainer.get_policy().get_weights()

    # Same as above
    trainer.workers.local_worker().policy_map["default_policy"].get_weights()

    # Get list of weights of each worker, including remote replicas
    trainer.workers.foreach_worker(lambda ev: ev.get_policy().get_weights())

    # Same as above
    trainer.workers.foreach_worker_with_index(lambda ev, i: ev.get_policy().get_weights())

Accessing Model State
~~~~~~~~~~~~~~~~~~~~~

Similar to accessing policy state, you may want to get a reference to the underlying neural network model being trained. For example, you may want to pre-train it separately, or otherwise update its weights outside of RLlib. This can be done by accessing the ``model`` of the policy:

**Example: Preprocessing observations for feeding into a model**

First, install the dependencies:

.. code-block:: python

    # The "Pong-v0" Atari environment requires a few additional gym installs:
    pip install "ray[rllib]" tensorflow torch "gym[atari]" "gym[accept-rom-license]" atari_py

Then for the code:

.. code-block:: python

    >>> import gym
    >>> env = gym.make("Pong-v0")

    # RLlib uses preprocessors to implement transforms such as one-hot encoding
    # and flattening of tuple and dict observations.
    >>> from ray.rllib.models.preprocessors import get_preprocessor
    >>> prep = get_preprocessor(env.observation_space)(env.observation_space)
    <ray.rllib.models.preprocessors.GenericPixelPreprocessor object at 0x7fc4d049de80>

    # Observations should be preprocessed prior to feeding into a model
    >>> env.reset().shape
    (210, 160, 3)
    >>> prep.transform(env.reset()).shape
    (84, 84, 3)

**Example: Querying a policy's action distribution**

.. code-block:: python

    # Get a reference to the policy
    >>> from ray.rllib.agents.ppo import PPOTrainer
    >>> trainer = PPOTrainer(env="CartPole-v0", config={"framework": "tf2", "num_workers": 0})
    >>> policy = trainer.get_policy()
    <ray.rllib.policy.eager_tf_policy.PPOTFPolicy_eager object at 0x7fd020165470>

    # Run a forward pass to get model output logits. Note that complex observations
    # must be preprocessed as in the above code block.
    >>> logits, _ = policy.model.from_batch({"obs": np.array([[0.1, 0.2, 0.3, 0.4]])})
    (<tf.Tensor: id=1274, shape=(1, 2), dtype=float32, numpy=...>, [])

    # Compute action distribution given logits
    >>> policy.dist_class
    <class_object 'ray.rllib.models.tf.tf_action_dist.Categorical'>
    >>> dist = policy.dist_class(logits, policy.model)
    <ray.rllib.models.tf.tf_action_dist.Categorical object at 0x7fd02301d710>

    # Query the distribution for samples, sample logps
    >>> dist.sample()
    <tf.Tensor: id=661, shape=(1,), dtype=int64, numpy=..>
    >>> dist.logp([1])
    <tf.Tensor: id=1298, shape=(1,), dtype=float32, numpy=...>

    # Get the estimated values for the most recent forward pass
    >>> policy.model.value_function()
    <tf.Tensor: id=670, shape=(1,), dtype=float32, numpy=...>

    >>> policy.model.base_model.summary()
    Model: "model"
    _____________________________________________________________________
    Layer (type)               Output Shape  Param #  Connected to
    =====================================================================
    observations (InputLayer)  [(None, 4)]   0
    _____________________________________________________________________
    fc_1 (Dense)               (None, 256)   1280     observations[0][0]
    _____________________________________________________________________
    fc_value_1 (Dense)         (None, 256)   1280     observations[0][0]
    _____________________________________________________________________
    fc_2 (Dense)               (None, 256)   65792    fc_1[0][0]
    _____________________________________________________________________
    fc_value_2 (Dense)         (None, 256)   65792    fc_value_1[0][0]
    _____________________________________________________________________
    fc_out (Dense)             (None, 2)     514      fc_2[0][0]
    _____________________________________________________________________
    value_out (Dense)          (None, 1)     257      fc_value_2[0][0]
    =====================================================================
    Total params: 134,915
    Trainable params: 134,915
    Non-trainable params: 0
    _____________________________________________________________________

**Example: Getting Q values from a DQN model**

.. code-block:: python

    # Get a reference to the model through the policy
    >>> from ray.rllib.agents.dqn import DQNTrainer
    >>> trainer = DQNTrainer(env="CartPole-v0", config={"framework": "tf2"})
    >>> model = trainer.get_policy().model
    <ray.rllib.models.catalog.FullyConnectedNetwork_as_DistributionalQModel ...>

    # List of all model variables
    >>> model.variables()
    [<tf.Variable 'default_policy/fc_1/kernel:0' shape=(4, 256) dtype=float32>, ...]

    # Run a forward pass to get base model output. Note that complex observations
    # must be preprocessed. An example of preprocessing is examples/saving_experiences.py
    >>> model_out = model.from_batch({"obs": np.array([[0.1, 0.2, 0.3, 0.4]])})
    (<tf.Tensor: id=832, shape=(1, 256), dtype=float32, numpy=...)

    # Access the base Keras models (all default models have a base)
    >>> model.base_model.summary()
    Model: "model"
    _______________________________________________________________________
    Layer (type)                Output Shape    Param #  Connected to
    =======================================================================
    observations (InputLayer)   [(None, 4)]     0
    _______________________________________________________________________
    fc_1 (Dense)                (None, 256)     1280     observations[0][0]
    _______________________________________________________________________
    fc_out (Dense)              (None, 256)     65792    fc_1[0][0]
    _______________________________________________________________________
    value_out (Dense)           (None, 1)       257      fc_1[0][0]
    =======================================================================
    Total params: 67,329
    Trainable params: 67,329
    Non-trainable params: 0
    ______________________________________________________________________________

    # Access the Q value model (specific to DQN)
    >>> model.get_q_value_distributions(model_out)
    [<tf.Tensor: id=891, shape=(1, 2)>, <tf.Tensor: id=896, shape=(1, 2, 1)>]

    >>> model.q_value_head.summary()
    Model: "model_1"
    _________________________________________________________________
    Layer (type)                 Output Shape              Param #
    =================================================================
    model_out (InputLayer)       [(None, 256)]             0
    _________________________________________________________________
    lambda (Lambda)              [(None, 2), (None, 2, 1), 66306
    =================================================================
    Total params: 66,306
    Trainable params: 66,306
    Non-trainable params: 0
    _________________________________________________________________

    # Access the state value model (specific to DQN)
    >>> model.get_state_value(model_out)
    <tf.Tensor: id=913, shape=(1, 1), dtype=float32>

    >>> model.state_value_head.summary()
    Model: "model_2"
    _________________________________________________________________
    Layer (type)                 Output Shape              Param #
    =================================================================
    model_out (InputLayer)       [(None, 256)]             0
    _________________________________________________________________
    lambda_1 (Lambda)            (None, 1)                 66049
    =================================================================
    Total params: 66,049
    Trainable params: 66,049
    Non-trainable params: 0
    _________________________________________________________________

This is especially useful when used with `custom model classes <rllib-models.html>`__.

Advanced Python APIs
--------------------

Custom Training Workflows
~~~~~~~~~~~~~~~~~~~~~~~~~

In the `basic training example <https://github.com/ray-project/ray/blob/master/rllib/examples/custom_env.py>`__, Tune will call ``train()`` on your trainer once per training iteration and report the new training results. Sometimes, it is desirable to have full control over training, but still run inside Tune. Tune supports :ref:`custom trainable functions <trainable-docs>` that can be used to implement `custom training workflows (example) <https://github.com/ray-project/ray/blob/master/rllib/examples/custom_train_fn.py>`__.

For even finer-grained control over training, you can use RLlib's lower-level `building blocks <rllib-concepts.html>`__ directly to implement `fully customized training workflows <https://github.com/ray-project/ray/blob/master/rllib/examples/rollout_worker_custom_workflow.py>`__.

Global Coordination
~~~~~~~~~~~~~~~~~~~
Sometimes, it is necessary to coordinate between pieces of code that live in different processes managed by RLlib. For example, it can be useful to maintain a global average of a certain variable, or centrally control a hyperparameter used by policies. Ray provides a general way to achieve this through *named actors* (learn more about Ray actors `here <actors.html>`__). These actors are assigned a global name and handles to them can be retrieved using these names. As an example, consider maintaining a shared global counter that is incremented by environments and read periodically from your driver program:

.. code-block:: python

    @ray.remote
    class Counter:
       def __init__(self):
          self.count = 0
       def inc(self, n):
          self.count += n
       def get(self):
          return self.count

    # on the driver
    counter = Counter.options(name="global_counter").remote()
    print(ray.get(counter.get.remote()))  # get the latest count

    # in your envs
    counter = ray.get_actor("global_counter")
    counter.inc.remote(1)  # async call to increment the global count

Ray actors provide high levels of performance, so in more complex cases they can be used implement communication patterns such as parameter servers and allreduce.

Callbacks and Custom Metrics
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can provide callbacks to be called at points during policy evaluation. These callbacks have access to state for the current `episode <https://github.com/ray-project/ray/blob/master/rllib/evaluation/episode.py>`__. Certain callbacks such as ``on_postprocess_trajectory``, ``on_sample_end``, and ``on_train_result`` are also places where custom postprocessing can be applied to intermediate data or results.

User-defined state can be stored for the `episode <https://github.com/ray-project/ray/blob/master/rllib/evaluation/episode.py>`__ in the ``episode.user_data`` dict, and custom scalar metrics reported by saving values to the ``episode.custom_metrics`` dict. These custom metrics will be aggregated and reported as part of training results. For a full example, see `custom_metrics_and_callbacks.py <https://github.com/ray-project/ray/blob/master/rllib/examples/custom_metrics_and_callbacks.py>`__.

.. autoclass:: ray.rllib.agents.callbacks.DefaultCallbacks
    :members:


Chaining Callbacks
~~~~~~~~~~~~~~~~~~

Use the ``MultiCallbacks`` class to chaim multiple callbacks together.

.. autoclass:: ray.rllib.agents.callbacks.MultiCallbacks


Visualizing Custom Metrics
~~~~~~~~~~~~~~~~~~~~~~~~~~

Custom metrics can be accessed and visualized like any other training result:

.. image:: images/custom_metric.png

.. _exploration-api:

Customizing Exploration Behavior
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

RLlib offers a unified top-level API to configure and customize an agent’s
exploration behavior, including the decisions (how and whether) to sample
actions from distributions (stochastically or deterministically).
The setup can be done via using built-in Exploration classes
(see `this package <https://github.com/ray-project/ray/blob/master/rllib/utils/exploration/>`__),
which are specified (and further configured) inside ``Trainer.config["exploration_config"]``.
Besides using one of the available classes, one can sub-class any of
these built-ins, add custom behavior to it, and use that new class in
the config instead.

Every policy has-an Exploration object, which is created from the Trainer’s
``config[“exploration_config”]`` dict, which specifies the class to use via the
special “type” key, as well as constructor arguments via all other keys,
e.g.:

.. code-block:: python

    # in Trainer.config:
    "exploration_config": {
        "type": "StochasticSampling",  # <- Special `type` key provides class information
        "[c'tor arg]" : "[value]",  # <- Add any needed constructor args here.
        # etc
    }
    # ...

The following table lists all built-in Exploration sub-classes and the agents
that currently use these by default:

.. View table below at: https://docs.google.com/drawings/d/1dEMhosbu7HVgHEwGBuMlEDyPiwjqp_g6bZ0DzCMaoUM/edit?usp=sharing
.. image:: images/rllib-exploration-api-table.svg

An Exploration class implements the ``get_exploration_action`` method,
in which the exact exploratory behavior is defined.
It takes the model’s output, the action distribution class, the model itself,
a timestep (the global env-sampling steps already taken),
and an ``explore`` switch and outputs a tuple of a) action and
b) log-likelihood:

.. literalinclude:: ../../../rllib/utils/exploration/exploration.py
   :language: python
   :start-after: __sphinx_doc_begin_get_exploration_action__
   :end-before: __sphinx_doc_end_get_exploration_action__

On the highest level, the ``Trainer.compute_action`` and ``Policy.compute_action(s)``
methods have a boolean ``explore`` switch, which is passed into
``Exploration.get_exploration_action``. If ``explore=None``, the value of
``Trainer.config[“explore”]`` is used, which thus serves as a main switch for
exploratory behavior, allowing e.g. turning off any exploration easily for
evaluation purposes (see :ref:`CustomEvaluation`).

The following are example excerpts from different Trainers' configs
(see rllib/agents/trainer.py) to setup different exploration behaviors:

.. code-block:: python

    # All of the following configs go into Trainer.config.

    # 1) Switching *off* exploration by default.
    # Behavior: Calling `compute_action(s)` without explicitly setting its `explore`
    # param will result in no exploration.
    # However, explicitly calling `compute_action(s)` with `explore=True` will
    # still(!) result in exploration (per-call overrides default).
    "explore": False,

    # 2) Switching *on* exploration by default.
    # Behavior: Calling `compute_action(s)` without explicitly setting its
    # explore param will result in exploration.
    # However, explicitly calling `compute_action(s)` with `explore=False`
    # will result in no(!) exploration (per-call overrides default).
    "explore": True,

    # 3) Example exploration_config usages:
    # a) DQN: see rllib/agents/dqn/dqn.py
    "explore": True,
    "exploration_config": {
       # Exploration sub-class by name or full path to module+class
       # (e.g. “ray.rllib.utils.exploration.epsilon_greedy.EpsilonGreedy”)
       "type": "EpsilonGreedy",
       # Parameters for the Exploration class' constructor:
       "initial_epsilon": 1.0,
       "final_epsilon": 0.02,
       "epsilon_timesteps": 10000,  # Timesteps over which to anneal epsilon.
    },

    # b) DQN Soft-Q: In order to switch to Soft-Q exploration, do instead:
    "explore": True,
    "exploration_config": {
       "type": "SoftQ",
       # Parameters for the Exploration class' constructor:
       "temperature": 1.0,
    },

    # c) All policy-gradient algos and SAC: see rllib/agents/trainer.py
    # Behavior: The algo samples stochastically from the
    # model-parameterized distribution. This is the global Trainer default
    # setting defined in trainer.py and used by all PG-type algos (plus SAC).
    "explore": True,
    "exploration_config": {
       "type": "StochasticSampling",
       "random_timesteps": 0,  # timesteps at beginning, over which to act uniformly randomly
    },


.. _CustomEvaluation:

Customized Evaluation During Training
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

RLlib will report online training rewards, however in some cases you may want to compute
rewards with different settings (e.g., with exploration turned off, or on a specific set
of environment configurations). You can activate evaluating policies during training (``Trainer.train()``) by setting
the ``evaluation_interval`` to an int value (> 0) indicating every how many ``Trainer.train()``
calls an "evaluation step" is run:

.. code-block:: python

    # Run one evaluation step on every 3rd `Trainer.train()` call.
    {
        "evaluation_interval": 3,
    }


One such evaluation step runs over ``evaluation_duration`` episodes or timesteps, depending
on the ``evaluation_duration_unit`` setting, which can be either "episodes" (default) or "timesteps".


.. code-block:: python

    # Every time we do run an evaluation step, run it for exactly 10 episodes.
    {
        "evaluation_duration": 10,
        "evaluation_duration_unit": "episodes",
    }
    # Every time we do run an evaluation step, run it for close to 200 timesteps.
    {
        "evaluation_duration": 200,
        "evaluation_duration_unit": "timesteps",
    }


Before each evaluation step, weights from the main model are synchronized to all evaluation workers.

Normally, the evaluation step is run right after the respective train step. For example, for
``evaluation_interval=2``, the sequence of steps is: ``train, train, eval, train, train, eval, ...``.
For ``evaluation_interval=1``, the sequence is: ``train, eval, train, eval, ...``.

However, it is possible to run evaluation in parallel to training via the ``evaluation_parallel_to_training=True``
config setting. In this case, both steps (train and eval) are run at the same time via threading.
This can speed up the evaluation process significantly, but leads to a 1-iteration delay between reported
training results and evaluation results (the evaluation results are behind b/c they use slightly outdated
model weights).

When running with the ``evaluation_parallel_to_training=True`` setting, a special "auto" value
is supported for ``evaluation_duration``. This can be used to make the evaluation step take
roughly as long as the train step:

.. code-block:: python

    # Run eval and train at the same time via threading and make sure they roughly
    # take the same time, such that the next `Trainer.train()` call can execute
    # immediately and not have to wait for a still ongoing (e.g. very long episode)
    # evaluation step:
    {
        "evaluation_interval": 1,
        "evaluation_parallel_to_training": True,
        "evaluation_duration": "auto",  # automatically end evaluation when train step has finished
        "evaluation_duration_unit": "timesteps",  # <- more fine grained than "episodes"
    }


The ``evaluation_config`` key allows you to override any config settings for
the evaluation workers. For example, to switch off exploration in the evaluation steps,
do:

.. code-block:: python

    # Switching off exploration behavior for evaluation workers
    # (see rllib/agents/trainer.py). Use any keys in this sub-dict that are
    # also supported in the main Trainer config.
    "evaluation_config": {
       "explore": False
    }

.. note::

    Policy gradient algorithms are able to find the optimal
    policy, even if this is a stochastic one. Setting "explore=False" above
    will result in the evaluation workers not using this stochastic policy.

Parallelism for the evaluation step is determined via the ``evaluation_num_workers``
setting. Set this to larger values if you want the desired evaluation episodes or timesteps to
run as much in parallel as possible. For example, if your ``evaluation_duration=10``,
``evaluation_duration_unit=episodes``, and ``evaluation_num_workers=10``, each eval worker
only has to run 1 episode in each eval step.

In case you would like to entirely customize the evaluation step, set ``custom_eval_function`` in your
config to a callable taking the Trainer object and a WorkerSet object (the evaluation WorkerSet)
and returning a metrics dict. See `trainer.py <https://github.com/ray-project/ray/blob/master/rllib/agents/trainer.py>`__
for further documentation.

There is an end to end example of how to set up custom online evaluation in `custom_eval.py <https://github.com/ray-project/ray/blob/master/rllib/examples/custom_eval.py>`__. Note that if you only want to eval your policy at the end of training, you can set ``evaluation_interval: N``, where ``N`` is the number of training iterations before stopping.

Below are some examples of how the custom evaluation metrics are reported nested under the ``evaluation`` key of normal training results:

.. code-block:: bash

    ------------------------------------------------------------------------
    Sample output for `python custom_eval.py`
    ------------------------------------------------------------------------

    INFO trainer.py:623 -- Evaluating current policy for 10 episodes.
    INFO trainer.py:650 -- Running round 0 of parallel evaluation (2/10 episodes)
    INFO trainer.py:650 -- Running round 1 of parallel evaluation (4/10 episodes)
    INFO trainer.py:650 -- Running round 2 of parallel evaluation (6/10 episodes)
    INFO trainer.py:650 -- Running round 3 of parallel evaluation (8/10 episodes)
    INFO trainer.py:650 -- Running round 4 of parallel evaluation (10/10 episodes)

    Result for PG_SimpleCorridor_2c6b27dc:
      ...
      evaluation:
        custom_metrics: {}
        episode_len_mean: 15.864661654135338
        episode_reward_max: 1.0
        episode_reward_mean: 0.49624060150375937
        episode_reward_min: 0.0
        episodes_this_iter: 133

.. code-block:: bash

    ------------------------------------------------------------------------
    Sample output for `python custom_eval.py --custom-eval`
    ------------------------------------------------------------------------

    INFO trainer.py:631 -- Running custom eval function <function ...>
    Update corridor length to 4
    Update corridor length to 7
    Custom evaluation round 1
    Custom evaluation round 2
    Custom evaluation round 3
    Custom evaluation round 4

    Result for PG_SimpleCorridor_0de4e686:
      ...
      evaluation:
        custom_metrics: {}
        episode_len_mean: 9.15695067264574
        episode_reward_max: 1.0
        episode_reward_mean: 0.9596412556053812
        episode_reward_min: 0.0
        episodes_this_iter: 223
        foo: 1

Rewriting Trajectories
~~~~~~~~~~~~~~~~~~~~~~

Note that in the ``on_postprocess_traj`` callback you have full access to the trajectory batch (``post_batch``) and other training state. This can be used to rewrite the trajectory, which has a number of uses including:

 * Backdating rewards to previous time steps (e.g., based on values in ``info``).
 * Adding model-based curiosity bonuses to rewards (you can train the model with a `custom model supervised loss <rllib-models.html#supervised-model-losses>`__).

To access the policy / model (``policy.model``) in the callbacks, note that ``info['pre_batch']`` returns a tuple where the first element is a policy and the second one is the batch itself. You can also access all the rollout worker state using the following call:

.. code-block:: python

    from ray.rllib.evaluation.rollout_worker import get_global_worker

    # You can use this from any callback to get a reference to the
    # RolloutWorker running in the process, which in turn has references to
    # all the policies, etc: see rollout_worker.py for more info.
    rollout_worker = get_global_worker()

Policy losses are defined over the ``post_batch`` data, so you can mutate that in the callbacks to change what data the policy loss function sees.

Curriculum Learning
~~~~~~~~~~~~~~~~~~~

In Curriculum learning, the environment can be set to different difficulties (or "tasks") to allow for learning to progress through controlled phases
(from easy to more difficult). RLlib comes with a basic curriculum learning API utilizing the
`TaskSettableEnv <https://github.com/ray-project/ray/blob/master/rllib/env/apis/task_settable_env.py>`__ environment API.
Your environment only needs to implement the `set_task` and `get_task` methods for this to work. You can then define an `env_task_fn` in your config,
which receives the last training results and returns a new task for the env to be set to:

.. code-block:: python

    from ray.rllib.env.apis.task_settable_env import TaskSettableEnv

    class MyEnv(TaskSettableEnv):
        def get_task(self):
            return self.current_difficulty

        def set_task(self, task):
            self.current_difficulty = task

    def curriculum_fn(train_results, task_settable_env, env_ctx):
        # Very simple curriculum function.
        current_task = task_settable_env.get_task()
        new_task = current_task + 1
        return new_task

    # Setup your Trainer's config like so:
    config = {
        "env": MyEnv,
        "env_task_fn": curriculum_fn,
    }
    # Train using `tune.run` or `Trainer.train()` and the above config stub.
    # ...

There are two more ways to use the RLlib's other APIs to implement `curriculum learning <https://bair.berkeley.edu/blog/2017/12/20/reverse-curriculum/>`__.

Use the Trainer API and update the environment between calls to ``train()``. This example shows the trainer being run inside a Tune function.
This is basically the same as what the built-in `env_task_fn` API described above already does under the hood, but allows you to do even more
customizations to your training loop.

.. code-block:: python

    import ray
    from ray import tune
    from ray.rllib.agents.ppo import PPOTrainer

    def train(config, reporter):
        trainer = PPOTrainer(config=config, env=YourEnv)
        while True:
            result = trainer.train()
            reporter(**result)
            if result["episode_reward_mean"] > 200:
                task = 2
            elif result["episode_reward_mean"] > 100:
                task = 1
            else:
                task = 0
            trainer.workers.foreach_worker(
                lambda ev: ev.foreach_env(
                    lambda env: env.set_task(task)))

    num_gpus = 0
    num_workers = 2

    ray.init()
    tune.run(
        train,
        config={
            "num_gpus": num_gpus,
            "num_workers": num_workers,
        },
        resources_per_trial=tune.PlacementGroupFactory(
            [{"CPU": 1}, {"GPU": num_gpus}] + [{"CPU": 1}] * num_workers
        ),
    )

You could also use RLlib's callbacks API to update the environment on new training results:

.. code-block:: python

    import ray
    from ray import tune

    def on_train_result(info):
        result = info["result"]
        if result["episode_reward_mean"] > 200:
            task = 2
        elif result["episode_reward_mean"] > 100:
            task = 1
        else:
            task = 0
        trainer = info["trainer"]
        trainer.workers.foreach_worker(
            lambda ev: ev.foreach_env(
                lambda env: env.set_task(task)))

    ray.init()
    tune.run(
        "PPO",
        config={
            "env": YourEnv,
            "callbacks": {
                "on_train_result": on_train_result,
            },
        },
    )

Debugging
---------

Gym Monitor
~~~~~~~~~~~

The ``"monitor": true`` config can be used to save Gym episode videos to the result dir. For example:

.. code-block:: bash

    rllib train --env=PongDeterministic-v4 \
        --run=A2C --config '{"num_workers": 2, "monitor": true}'

    # videos will be saved in the ~/ray_results/<experiment> dir, for example
    openaigym.video.0.31401.video000000.meta.json
    openaigym.video.0.31401.video000000.mp4
    openaigym.video.0.31403.video000000.meta.json
    openaigym.video.0.31403.video000000.mp4

Eager Mode
~~~~~~~~~~

Policies built with ``build_tf_policy`` (most of the reference algorithms are)
can be run in eager mode by setting the
``"framework": "[tf2|tfe]"`` / ``"eager_tracing": true`` config options or using
``rllib train --config '{"framework": "tf2"}' [--trace]``.
This will tell RLlib to execute the model forward pass, action distribution,
loss, and stats functions in eager mode.

Eager mode makes debugging much easier, since you can now use line-by-line
debugging with breakpoints or Python ``print()`` to inspect
intermediate tensor values.
However, eager can be slower than graph mode unless tracing is enabled.

Using PyTorch
~~~~~~~~~~~~~

Trainers that have an implemented TorchPolicy, will allow you to run
`rllib train` using the command line ``--torch`` flag.
Algorithms that do not have a torch version yet will complain with an error in
this case.


Episode Traces
~~~~~~~~~~~~~~

You can use the `data output API <rllib-offline.html>`__ to save episode traces for debugging. For example, the following command will run PPO while saving episode traces to ``/tmp/debug``.

.. code-block:: bash

    rllib train --run=PPO --env=CartPole-v0 \
        --config='{"output": "/tmp/debug", "output_compress_columns": []}'

    # episode traces will be saved in /tmp/debug, for example
    output-2019-02-23_12-02-03_worker-2_0.json
    output-2019-02-23_12-02-04_worker-1_0.json

Log Verbosity
~~~~~~~~~~~~~

You can control the trainer log level via the ``"log_level"`` flag. Valid values are "DEBUG", "INFO", "WARN" (default), and "ERROR". This can be used to increase or decrease the verbosity of internal logging. You can also use the ``-v`` and ``-vv`` flags. For example, the following two commands are about equivalent:

.. code-block:: bash

    rllib train --env=PongDeterministic-v4 \
        --run=A2C --config '{"num_workers": 2, "log_level": "DEBUG"}'

    rllib train --env=PongDeterministic-v4 \
        --run=A2C --config '{"num_workers": 2}' -vv

The default log level is ``WARN``. We strongly recommend using at least ``INFO`` level logging for development.

Stack Traces
~~~~~~~~~~~~

You can use the ``ray stack`` command to dump the stack traces of all the Python workers on a single node. This can be useful for debugging unexpected hangs or performance issues.

External Application API
------------------------

In some cases (i.e., when interacting with an externally hosted simulator or production environment) it makes more sense to interact with RLlib as if it were an independently running service, rather than RLlib hosting the simulations itself. This is possible via RLlib's external applications interface `(full documentation) <rllib-env.html#external-agents-and-applications>`__.

.. autoclass:: ray.rllib.env.policy_client.PolicyClient
    :members:

.. autoclass:: ray.rllib.env.policy_server_input.PolicyServerInput
    :members:

.. include:: /_includes/rllib/announcement_bottom.rst
