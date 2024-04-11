
.. include:: /_includes/rllib/new_api_stack.rst

.. _rllib-advanced-api-doc:

Advanced Python APIs
--------------------

Custom Training Workflows
~~~~~~~~~~~~~~~~~~~~~~~~~

In the `basic training example <https://github.com/ray-project/ray/blob/master/rllib/examples/envs/custom_gym_env.py>`__,
Tune will call ``train()`` on your algorithm once per training iteration and report
the new training results.
Sometimes, it is desirable to have full control over training, but still run inside Tune.
Tune supports :ref:`custom trainable functions <trainable-docs>` that can be used to
implement `custom training workflows (example) <https://github.com/ray-project/ray/blob/master/rllib/examples/ray_tune/custom_train_function.py>`__.

Curriculum Learning
~~~~~~~~~~~~~~~~~~~

In Curriculum learning, the environment can be set to different difficulties
(or "tasks") to allow for learning to progress through controlled phases (from easy to
more difficult). RLlib comes with a basic curriculum learning API utilizing the
`TaskSettableEnv <https://github.com/ray-project/ray/blob/master/rllib/env/apis/task_settable_env.py>`__ environment API.
Your environment only needs to implement the `set_task` and `get_task` methods
for this to work. You can then define an `env_task_fn` in your config,
which receives the last training results and returns a new task for the env to be set to:

.. TODO move to doc_code and make it use algo configs.
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

    # Setup your Algorithm's config like so:
    config = {
        "env": MyEnv,
        "env_task_fn": curriculum_fn,
    }
    # Train using `Tuner.fit()` or `Algorithm.train()` and the above config stub.
    # ...

There are two more ways to use the RLlib's other APIs to implement
`curriculum learning <https://bair.berkeley.edu/blog/2017/12/20/reverse-curriculum/>`__.

Use the Algorithm API and update the environment between calls to ``train()``.
This example shows the algorithm being run inside a Tune function.
This is basically the same as what the built-in `env_task_fn` API described above
already does under the hood, but allows you to do even more customizations to your
training loop.

.. TODO move to doc_code and make it use algo configs.
.. code-block:: python

    import ray
    from ray import train, tune
    from ray.rllib.algorithms.ppo import PPO

    def train_fn(config):
        algo = PPO(config=config, env=YourEnv)
        while True:
            result = algo.train()
            train.report(result)
            if result["episode_reward_mean"] > 200:
                task = 2
            elif result["episode_reward_mean"] > 100:
                task = 1
            else:
                task = 0
            algo.workers.foreach_worker(
                lambda ev: ev.foreach_env(
                    lambda env: env.set_task(task)))

    num_gpus = 0
    num_workers = 2

    ray.init()
    tune.Tuner(
        tune.with_resources(train_fn, resources=tune.PlacementGroupFactory(
            [{"CPU": 1}, {"GPU": num_gpus}] + [{"CPU": 1}] * num_workers
        ),)
        param_space={
            "num_gpus": num_gpus,
            "num_workers": num_workers,
        },
    ).fit()

You could also use RLlib's callbacks API to update the environment on new training
results:

.. TODO move to doc_code and make it use algo configs.
.. code-block:: python

    import ray
    from ray import tune
    from ray.rllib.agents.callbacks import DefaultCallbacks

    class MyCallbacks(DefaultCallbacks):
        def on_train_result(self, algorithm, result, **kwargs):
            if result["episode_reward_mean"] > 200:
                task = 2
            elif result["episode_reward_mean"] > 100:
                task = 1
            else:
                task = 0
            algorithm.workers.foreach_worker(
                lambda ev: ev.foreach_env(
                    lambda env: env.set_task(task)))

    ray.init()
    tune.Tuner(
        "PPO",
        param_space={
            "env": YourEnv,
            "callbacks": MyCallbacks,
        },
    ).fit()


Global Coordination
~~~~~~~~~~~~~~~~~~~
Sometimes, it is necessary to coordinate between pieces of code that live in different
processes managed by RLlib.
For example, it can be useful to maintain a global average of a certain variable,
or centrally control a hyperparameter used by policies.
Ray provides a general way to achieve this through *named actors*
(learn more about :ref:`Ray actors here <actor-guide>`).
These actors are assigned a global name and handles to them can be retrieved using
these names. As an example, consider maintaining a shared global counter that is
incremented by environments and read periodically from your driver program:

.. literalinclude:: ./doc_code/advanced_api.py
   :language: python
   :start-after: __rllib-adv_api_counter_begin__
   :end-before: __rllib-adv_api_counter_end__

Ray actors provide high levels of performance, so in more complex cases they can be
used implement communication patterns such as parameter servers and allreduce.

Callbacks and Custom Metrics
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can provide callbacks to be called at points during policy evaluation.
These callbacks have access to state for the current
`episode <https://github.com/ray-project/ray/blob/master/rllib/evaluation/episode.py>`__.
Certain callbacks such as ``on_postprocess_trajectory``, ``on_sample_end``,
and ``on_train_result`` are also places where custom postprocessing can be applied to
intermediate data or results.

User-defined state can be stored for the
`episode <https://github.com/ray-project/ray/blob/master/rllib/evaluation/episode.py>`__
in the ``episode.user_data`` dict, and custom scalar metrics reported by saving values
to the ``episode.custom_metrics`` dict. These custom metrics will be aggregated and
reported as part of training results. For a full example, take a look at
`this example script here <https://github.com/ray-project/ray/blob/master/rllib/examples/custom_metrics_and_callbacks.py>`__
and
`these unit test cases here <https://github.com/ray-project/ray/blob/master/rllib/algorithms/tests/test_callbacks_old_stack.py>`__.

.. tip::
    You can create custom logic that can run on each evaluation episode by checking
    if the :py:class:`~ray.rllib.evaluation.rollout_worker.RolloutWorker` is in
    evaluation mode, through accessing ``worker.policy_config["in_evaluation"]``.
    You can then implement this check in ``on_episode_start()`` or ``on_episode_end()``
    in your subclass of :py:class:`~ray.rllib.algorithms.callbacks.DefaultCallbacks`.
    For running callbacks before and after the evaluation
    runs in whole we provide ``on_evaluate_start()`` and ``on_evaluate_end``.

.. dropdown:: Click here to see the full API of the ``DefaultCallbacks`` class

    .. autoclass:: ray.rllib.algorithms.callbacks.DefaultCallbacks
        :members:


Chaining Callbacks
~~~~~~~~~~~~~~~~~~

Use the :py:func:`~ray.rllib.algorithms.callbacks.make_multi_callbacks` utility to chain
multiple callbacks together.

.. autofunction:: ray.rllib.algorithms.callbacks.make_multi_callbacks


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
which are specified (and further configured) inside
``AlgorithmConfig().exploration(..)``.
Besides using one of the available classes, one can sub-class any of
these built-ins, add custom behavior to it, and use that new class in
the config instead.

Every policy has-an Exploration object, which is created from the AlgorithmConfig’s
``.exploration(exploration_config=...)`` method, which specifies the class to use via the
special “type” key, as well as constructor arguments via all other keys,
e.g.:

.. literalinclude:: ./doc_code/advanced_api.py
   :language: python
   :start-after: __rllib-adv_api_explore_begin__
   :end-before: __rllib-adv_api_explore_end__

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

On the highest level, the ``Algorithm.compute_actions`` and ``Policy.compute_actions``
methods have a boolean ``explore`` switch, which is passed into
``Exploration.get_exploration_action``. If ``explore=None``, the value of
``Algorithm.config[“explore”]`` is used, which thus serves as a main switch for
exploratory behavior, allowing e.g. turning off any exploration easily for
evaluation purposes (see :ref:`CustomEvaluation`).

The following are example excerpts from different Algorithms' configs
(see ``rllib/algorithms/algorithm.py``) to setup different exploration behaviors:

.. TODO move to doc_code and make it use algo configs.
.. code-block:: python

    # All of the following configs go into Algorithm.config.

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
    # a) DQN: see rllib/algorithms/dqn/dqn.py
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

    # c) All policy-gradient algos and SAC: see rllib/algorithms/algorithm.py
    # Behavior: The algo samples stochastically from the
    # model-parameterized distribution. This is the global Algorithm default
    # setting defined in algorithm.py and used by all PG-type algos (plus SAC).
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
of environment configurations). You can activate evaluating policies during training
(``Algorithm.train()``) by setting the ``evaluation_interval`` to an int value (> 0)
indicating every how many ``Algorithm.train()`` calls an "evaluation step" should be run:

.. literalinclude:: ./doc_code/advanced_api.py
   :language: python
   :start-after: __rllib-adv_api_evaluation_1_begin__
   :end-before: __rllib-adv_api_evaluation_1_end__

An evaluation step runs - using its own ``EnvRunner``s - for ``evaluation_duration``
episodes or time-steps, depending on the ``evaluation_duration_unit`` setting, which can
take values of either ``"episodes"`` (default) or ``"timesteps"``.

.. literalinclude:: ./doc_code/advanced_api.py
   :language: python
   :start-after: __rllib-adv_api_evaluation_2_begin__
   :end-before: __rllib-adv_api_evaluation_2_end__

Note: When using ``evaluation_duration_unit=timesteps`` and your ``evaluation_duration``
setting is not divisible by the number of evaluation workers (configurable via
``evaluation_num_workers``), RLlib will round up the number of time-steps specified to
the nearest whole number of time-steps that is divisible by the number of evaluation
workers.
Also, when using ``evaluation_duration_unit=episodes`` and your
``evaluation_duration`` setting is not divisible by the number of evaluation workers
(configurable via ``evaluation_num_workers``), RLlib will run the remainder of episodes
on the first n eval EnvRunners and leave the remaining workers idle for that time.

For example:

.. literalinclude:: ./doc_code/advanced_api.py
   :language: python
   :start-after: __rllib-adv_api_evaluation_3_begin__
   :end-before: __rllib-adv_api_evaluation_3_end__

Before each evaluation step, weights from the main model are synchronized
to all evaluation workers.

By default, the evaluation step (if there is one in the current iteration) is run
right **after** the respective training step.
For example, for ``evaluation_interval=1``, the sequence of events is:
``train(0->1), eval(1), train(1->2), eval(2), train(2->3), ...``.
Here, the indices show the version of neural network weights used.
``train(0->1)`` is an update step that changes the weights from version 0 to
version 1 and ``eval(1)`` then uses weights version 1.
Weights index 0 represents the randomly initialized weights of our neural network(s).

Another example: For ``evaluation_interval=2``, the sequence is:
``train(0->1), train(1->2), eval(2), train(2->3), train(3->4), eval(4), ...``.

Instead of running ``train``- and ``eval``-steps in sequence, it is also possible to
run them in parallel via the ``evaluation_parallel_to_training=True`` config setting.
In this case, both training- and evaluation steps are run at the same time via
multi-threading.
This can speed up the evaluation process significantly, but leads to a 1-iteration
delay between reported training- and evaluation results.
The evaluation results are behind in this case b/c they use slightly outdated
model weights (synchronized after the previous training step).

For example, for ``evaluation_parallel_to_training=True`` and ``evaluation_interval=1``,
the sequence is now:
``train(0->1) + eval(0), train(1->2) + eval(1), train(2->3) + eval(2)``,
where ``+`` means: "at the same time".
Note that the change in the weights indices with respect to the non-parallel examples above.
The evaluation weights indices are now "one behind"
the resulting train weights indices (``train(1->**2**) + eval(**1**)``).

When running with the ``evaluation_parallel_to_training=True`` setting, a special "auto" value
is supported for ``evaluation_duration``. This can be used to make the evaluation step take
roughly as long as the concurrently ongoing training step:

.. literalinclude:: ./doc_code/advanced_api.py
   :language: python
   :start-after: __rllib-adv_api_evaluation_4_begin__
   :end-before: __rllib-adv_api_evaluation_4_end__

The ``evaluation_config`` key allows you to override any config settings for
the evaluation workers. For example, to switch off exploration in the evaluation steps,
do:

.. literalinclude:: ./doc_code/advanced_api.py
   :language: python
   :start-after: __rllib-adv_api_evaluation_5_begin__
   :end-before: __rllib-adv_api_evaluation_5_end__


.. note::

    Policy gradient algorithms are able to find the optimal
    policy, even if this is a stochastic one. Setting "explore=False" above
    will result in the evaluation workers not using this stochastic policy.


The level of parallelism within the evaluation step is determined via the
``evaluation_num_workers`` setting. Set this to larger values if you want the desired
evaluation episodes or time-steps to run as much in parallel as possible.
For example, if your ``evaluation_duration=10``, ``evaluation_duration_unit=episodes``,
and ``evaluation_num_workers=10``, each evaluation ``EnvRunner``
only has to run one episode in each evaluation step.

In case you observe occasional failures in your (evaluation) EnvRunners during
evaluation (e.g. you have an environment that sometimes crashes or stalls),
you should use the following combination of settings, minimizing the negative effects
of such environment behavior:

Note that with or without parallel evaluation, all
:ref:`fault tolerance settings <rllib-scaling-guide>`, such as
``ignore_worker_failures`` or ``recreate_failed_workers`` will be respected and applied
to the failed evaluation workers.

Here's an example:

.. literalinclude:: ./doc_code/advanced_api.py
   :language: python
   :start-after: __rllib-adv_api_evaluation_6_begin__
   :end-before: __rllib-adv_api_evaluation_6_end__

This will run the parallel sampling of all evaluation EnvRunners, such that if one of
the workers takes too long to run through an episode and return data or fails entirely,
the other evaluation EnvRunners will still complete the job.

In case you would like to entirely customize the evaluation step,
set ``custom_eval_function`` in your config to a callable, which takes the Algorithm
object and a WorkerSet object (the Algorithm's ``self.evaluation_workers``
WorkerSet instance) and returns a metrics dictionary.
See `algorithm.py <https://github.com/ray-project/ray/blob/master/rllib/algorithms/algorithm.py>`__
for further documentation.

There is also an end-to-end example of how to set up a custom online evaluation in
`custom_evaluation.py <https://github.com/ray-project/ray/blob/master/rllib/examples/evaluation/custom_evaluation.py>`__.
Note that if you only want to evaluate your policy at the end of training,
you can set ``evaluation_interval: [int]``, where ``[int]`` should be the number
of training iterations before stopping.

Below are some examples of how the custom evaluation metrics are reported nested under
the ``evaluation`` key of normal training results:

.. TODO make sure these outputs are still valid.
.. code-block:: bash

    ------------------------------------------------------------------------
    Sample output for `python custom_evaluation.py --no-custom-eval`
    ------------------------------------------------------------------------

    INFO algorithm.py:623 -- Evaluating current policy for 10 episodes.
    INFO algorithm.py:650 -- Running round 0 of parallel evaluation (2/10 episodes)
    INFO algorithm.py:650 -- Running round 1 of parallel evaluation (4/10 episodes)
    INFO algorithm.py:650 -- Running round 2 of parallel evaluation (6/10 episodes)
    INFO algorithm.py:650 -- Running round 3 of parallel evaluation (8/10 episodes)
    INFO algorithm.py:650 -- Running round 4 of parallel evaluation (10/10 episodes)

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
    Sample output for `python custom_evaluation.py`
    ------------------------------------------------------------------------

    INFO algorithm.py:631 -- Running custom eval function <function ...>
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

Note that in the ``on_postprocess_traj`` callback you have full access to the
trajectory batch (``post_batch``) and other training state. This can be used to
rewrite the trajectory, which has a number of uses including:

 * Backdating rewards to previous time steps (e.g., based on values in ``info``).
 * Adding model-based curiosity bonuses to rewards (you can train the model with a
   `custom model supervised loss <rllib-models.html#supervised-model-losses>`__).

To access the policy / model (``policy.model``) in the callbacks, note that
``info['pre_batch']`` returns a tuple where the first element is a policy and the
second one is the batch itself. You can also access all the rollout worker state
using the following call:

.. TODO move to doc_code and make it use algo configs.
.. code-block:: python

    from ray.rllib.evaluation.rollout_worker import get_global_worker

    # You can use this from any callback to get a reference to the
    # RolloutWorker running in the process, which in turn has references to
    # all the policies, etc: see rollout_worker.py for more info.
    rollout_worker = get_global_worker()

Policy losses are defined over the ``post_batch`` data, so you can mutate that in
the callbacks to change what data the policy loss function sees.
