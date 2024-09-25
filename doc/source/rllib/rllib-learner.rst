.. include:: /_includes/rllib/we_are_hiring.rst

.. include:: /_includes/rllib/new_api_stack.rst

.. |tensorflow| image:: images/tensorflow.png
    :class: inline-figure
    :width: 16

.. |pytorch| image:: images/pytorch.png
    :class: inline-figure
    :width: 16


Learner (Alpha)
===============

:py:class:`~ray.rllib.core.learner.learner.Learner` allows you to abstract the training
logic of RLModules. It supports both gradient-based and non-gradient-based updates (e.g.
polyak averaging, etc.) The API enables you to distribute the Learner using data-
distributed parallel (DDP). The Learner achieves the following:


(1) Facilitates gradient-based updates on :ref:`RLModule <rlmodule-guide>`.
(2) Provides abstractions for non-gradient based updates such as polyak averaging, etc.
(3) Reporting training statistics.
(4) Checkpoints the modules and optimizer states for durable training.

The :py:class:`~ray.rllib.core.learner.learner.Learner` class supports data-distributed-
parallel style training using the
:py:class:`~ray.rllib.core.learner.learner_group.LearnerGroup` API. Under this paradigm,
the :py:class:`~ray.rllib.core.learner.learner_group.LearnerGroup` maintains multiple
copies of the same :py:class:`~ray.rllib.core.learner.learner.Learner` with identical
parameters and hyperparameters. Each of these
:py:class:`~ray.rllib.core.learner.learner.Learner` instances computes the loss and gradients on a
shard of a sample batch and then accumulates the gradients across the
:py:class:`~ray.rllib.core.learner.learner.Learner` instances. Learn more about data-distributed
parallel learning in
`this article. <https://pytorch.org/tutorials/intermediate/ddp_tutorial.html>`_

:py:class:`~ray.rllib.core.learner.learner_group.LearnerGroup` also allows for
asynchronous training and (distributed) checkpointing for durability during training.

Enabling Learner API in RLlib experiments
=========================================

Adjust the amount of resources for training using the
`num_gpus_per_learner`, `num_cpus_per_learner`, and `num_learners`
arguments in the :py:class:`~ray.rllib.algorithms.algorithm_config.AlgorithmConfig`.

.. testcode::
	:hide:

    from ray.rllib.algorithms.ppo.ppo import PPOConfig

.. testcode::

    config = (
        PPOConfig()
        .api_stack(enable_rl_module_and_learner=True)
        .learners(
            num_learners=0,  # Set this to greater than 1 to allow for DDP style updates.
            num_gpus_per_learner=0,  # Set this to 1 to enable GPU training.
            num_cpus_per_learner=1,
        )
    )

.. testcode::
	:hide:

	config = config.environment("CartPole-v1")
	config.build()  # test that the algorithm can be built with the given resources


.. note::

    This features is in alpha. If you migrate to this algorithm, enable the feature by
    via `AlgorithmConfig.api_stack(enable_rl_module_and_learner=True)`.

    The following algorithms support :py:class:`~ray.rllib.core.learner.learner.Learner` out of the box. Implement
    an algorithm with a custom :py:class:`~ray.rllib.core.learner.learner.Learner` to leverage this API for other algorithms.

    .. list-table::
       :header-rows: 1
       :widths: 60 60

       * - Algorithm
         - Supported Framework
       * - **PPO**
         - |pytorch| |tensorflow|
       * - **Impala**
         - |pytorch| |tensorflow|
       * - **APPO**
         - |pytorch| |tensorflow|


Basic usage
===========

Use the :py:class:`~ray.rllib.core.learner.learner_group.LearnerGroup` utility to interact with multiple learners.

Construction
------------

If you enable the :ref:`RLModule <rlmodule-guide>`
and :py:class:`~ray.rllib.core.learner.learner.Learner` APIs via the :py:class:`~ray.rllib.algorithms.algorithm_config.AlgorithmConfig`, then calling :py:meth:`~ray.rllib.algorithms.algorithm_config.AlgorithmConfig.build` constructs a :py:class:`~ray.rllib.core.learner.learner_group.LearnerGroup` for you, but if you’re using these APIs standalone, you can construct the :py:class:`~ray.rllib.core.learner.learner_group.LearnerGroup` as follows.

.. testcode::
    :hide:

    # imports for the examples
    import gymnasium as gym
    import numpy as np

    import ray
    from ray.rllib.algorithms.ppo import PPOConfig
    from ray.rllib.core.rl_module.rl_module import RLModuleSpec
    from ray.rllib.core.learner.learner_group import LearnerGroup


.. tab-set::

    .. tab-item:: Contstructing a LearnerGroup


        .. testcode::

            env = gym.make("CartPole-v1")

            # Create an AlgorithmConfig object from which we can build the
            # LearnerGroup.
            config = (
                PPOConfig()
                # Number of Learner workers (Ray actors).
                # Use 0 for no actors, only create a local Learner.
                # Use >=1 to create n DDP-style Learner workers (Ray actors).
                .learners(num_learners=1)
                # Specify the learner's hyperparameters.
                .training(
                    use_kl_loss=True,
                    kl_coeff=0.01,
                    kl_target=0.05,
                    clip_param=0.2,
                    vf_clip_param=0.2,
                    entropy_coeff=0.05,
                    vf_loss_coeff=0.5
                )
            )

            # Construct a new LearnerGroup using our config object.
            learner_group = config.build_learner_group(env=env)

    .. tab-item:: Constructing a Learner

        .. testcode::

            env = gym.make("CartPole-v1")

            # Create an AlgorithmConfig object from which we can build the
            # Learner.
            config = (
                PPOConfig()
                # Specify the Learner's hyperparameters.
                .training(
                    use_kl_loss=True,
                    kl_coeff=0.01,
                    kl_target=0.05,
                    clip_param=0.2,
                    vf_clip_param=0.2,
                    entropy_coeff=0.05,
                    vf_loss_coeff=0.5
                )
            )
            # Construct a new Learner using our config object.
            learner = config.build_learner(env=env)


Updates
-------

.. testcode::
    :hide:

    import time

    from ray.rllib.core import DEFAULT_MODULE_ID
    from ray.rllib.evaluation.postprocessing import Postprocessing
    from ray.rllib.policy.sample_batch import SampleBatch, MultiAgentBatch

    DUMMY_BATCH = {
        SampleBatch.OBS: np.array(
            [[0.1, 0.2, 0.3, 0.4], [0.5, 0.6, 0.7, 0.8], [0.9, 1.0, 1.1, 1.2]],
            dtype=np.float32,
        ),
        SampleBatch.NEXT_OBS: np.array(
            [[0.1, 0.2, 0.3, 0.4], [0.5, 0.6, 0.7, 0.8], [0.9, 1.0, 1.1, 1.2]],
            dtype=np.float32,
        ),
        SampleBatch.ACTIONS: np.array([0, 1, 1]),
        SampleBatch.PREV_ACTIONS: np.array([0, 1, 1]),
        SampleBatch.REWARDS: np.array([1.0, -1.0, 0.5], dtype=np.float32),
        SampleBatch.PREV_REWARDS: np.array([1.0, -1.0, 0.5], dtype=np.float32),
        SampleBatch.TERMINATEDS: np.array([False, False, True]),
        SampleBatch.TRUNCATEDS: np.array([False, False, False]),
        SampleBatch.VF_PREDS: np.array([0.5, 0.6, 0.7], dtype=np.float32),
        SampleBatch.ACTION_DIST_INPUTS: np.array(
            [[-2.0, 0.5], [-3.0, -0.3], [-0.1, 2.5]], dtype=np.float32
        ),
        SampleBatch.ACTION_LOGP: np.array([-0.5, -0.1, -0.2], dtype=np.float32),
        SampleBatch.EPS_ID: np.array([0, 0, 0]),
        SampleBatch.AGENT_INDEX: np.array([0, 0, 0]),
        Postprocessing.ADVANTAGES: np.array([0.1, 0.2, 0.3], dtype=np.float32),
        Postprocessing.VALUE_TARGETS: np.array([0.5, 0.6, 0.7], dtype=np.float32),
    }
    default_batch = SampleBatch(DUMMY_BATCH)
    DUMMY_BATCH = default_batch.as_multi_agent()

    learner.build() # needs to be called on the learner before calling any functions


.. tab-set::

    .. tab-item:: Updating a LearnerGroup

        .. testcode::

            TIMESTEPS = {"num_env_steps_sampled_lifetime": 250}

            # This is a blocking update.
            results = learner_group.update_from_batch(batch=DUMMY_BATCH, timesteps=TIMESTEPS)

            # This is a non-blocking update. The results are returned in a future
            # call to `update_from_batch(..., async_update=True)`
            _ = learner_group.update_from_batch(batch=DUMMY_BATCH, async_update=True, timesteps=TIMESTEPS)

            # Artificially wait for async request to be done to get the results
            # in the next call to
            # `LearnerGroup.update_from_batch(..., async_update=True)`.
            time.sleep(5)
            results = learner_group.update_from_batch(
                batch=DUMMY_BATCH, async_update=True, timesteps=TIMESTEPS
            )
            # `results` is an already reduced dict, which is the result of
            # reducing over the individual async `update_from_batch(..., async_update=True)`
            # calls.
            assert isinstance(results, dict), results

        When updating a :py:class:`~ray.rllib.core.learner.learner_group.LearnerGroup` you can perform blocking or async updates on batches of data.
        Async updates are necessary for implementing async algorithms such as APPO/IMPALA.

    .. tab-item:: Updating a Learner

        .. testcode::

            # This is a blocking update (given a training batch).
            result = learner.update_from_batch(batch=DUMMY_BATCH, timesteps=TIMESTEPS)

        When updating a :py:class:`~ray.rllib.core.learner.learner.Learner` you can only perform blocking updates on batches of data.
        You can perform non-gradient based updates before or after the gradient-based ones by overriding
        :py:meth:`~ray.rllib.core.learner.learner.Learner.before_gradient_based_update` and
        :py:meth:`~ray.rllib.core.learner.learner.Learner.after_gradient_based_update`.


Getting and setting state
-------------------------

.. tab-set::

    .. tab-item:: Getting and Setting State for a LearnerGroup

        .. testcode::

            # Get the LearnerGroup's RLModule weights and optimizer states.
            state = learner_group.get_state()
            learner_group.set_state(state)

            # Only get the RLModule weights.
            weights = learner_group.get_weights()
            learner_group.set_weights(weights)

        Set/get the state dict of all learners through learner_group through
        `LearnerGroup.set_state` or `LearnerGroup.get_state`.
        This includes the neural network weights
        and the optimizer states on each learner. For example an Adam optimizer's state
        has momentum information based on recently computed gradients.
        If you only want to get or set the weights of the RLModules (neural networks) of
        all Learners, you can do so through the LearnerGroup APIs
        `LearnerGroup.get_weights` and `LearnerGroup.set_weights`.

    .. tab-item:: Getting and Setting State for a Learner

        .. testcode::

            from ray.rllib.core import COMPONENT_RL_MODULE

            # Get the Learner's RLModule weights and optimizer states.
            state = learner.get_state()
            # Note that `state` is now a dict:
            # {
            #    COMPONENT_RL_MODULE: [RLModule's state],
            #    COMPONENT_OPTIMIZER: [Optimizer states],
            # }
            learner.set_state(state)

            # Only get the RLModule weights (as numpy, not torch/tf).
            rl_module_only_state = learner.get_state(components=COMPONENT_RL_MODULE)
            # Note that `rl_module_only_state` is now a dict:
            # {COMPONENT_RL_MODULE: [RLModule's state]}
            learner.module.set_state(rl_module_only_state)

        You can set and get the entire state of a :py:class:`~ray.rllib.core.learner.learner.Learner`
        using :py:meth:`~ray.rllib.core.learner.learner.Learner.set_state`
        and :py:meth:`~ray.rllib.core.learner.learner.Learner.get_state` .
        For getting only the RLModule's weights (without optimizer states), use
        the `components=COMPONENT_RL_MODULE` arg in :py:meth:`~ray.rllib.core.learner.learner.Learner.get_state`
        (see code above).
        For setting only the RLModule's weights (without touching the optimizer states), use
        :py:meth:`~ray.rllib.core.learner.learner.Learner.get_state` and pass in a dict:
        `{COMPONENT_RL_MODULE: [RLModule's state]}` (see code above).


.. testcode::
    :hide:

    import tempfile

    LEARNER_CKPT_DIR = tempfile.mkdtemp()
    LEARNER_GROUP_CKPT_DIR = tempfile.mkdtemp()


Checkpointing
-------------

.. tab-set::

    .. tab-item:: Checkpointing a LearnerGroup

        .. testcode::

            learner_group.save_to_path(LEARNER_GROUP_CKPT_DIR)
            learner_group.restore_from_path(LEARNER_GROUP_CKPT_DIR)

        Checkpoint the state of all learners in the :py:class:`~ray.rllib.core.learner.learner_group.LearnerGroup`
        through :py:meth:`~ray.rllib.core.learner.learner_group.LearnerGroup.save_to_path` and restore
        the state of a saved :py:class:`~ray.rllib.core.learner.learner_group.LearnerGroup` through :py:meth:`~ray.rllib.core.learner.learner_group.LearnerGroup.restore_from_path`.
        A LearnerGroup's state includes the neural network weights and all optimizer states.
        Note that since the state of all of the :py:class:`~ray.rllib.core.learner.learner.Learner` instances is identical,
        only the states from the first :py:class:`~ray.rllib.core.learner.learner.Learner` are saved.

    .. tab-item:: Checkpointing a Learner

        .. testcode::

            learner.save_to_path(LEARNER_CKPT_DIR)
            learner.restore_from_path(LEARNER_CKPT_DIR)

        Checkpoint the state of a :py:class:`~ray.rllib.core.learner.learner.Learner`
        through :py:meth:`~ray.rllib.core.learner.learner.Learner.save_to_path` and restore the state
        of a saved :py:class:`~ray.rllib.core.learner.learner.Learner` through :py:meth:`~ray.rllib.core.learner.learner.Learner.restore_from_path`.
        A Learner's state includes the neural network weights and all optimizer states.


Implementation
==============
:py:class:`~ray.rllib.core.learner.learner.Learner` has many APIs for flexible implementation, however the core ones that you need to implement are:

.. list-table::
   :widths: 60 60
   :header-rows: 1

   * - Method
     - Description
   * - :py:meth:`~ray.rllib.core.learner.learner.Learner.configure_optimizers_for_module()`
     - set up any optimizers for a RLModule.
   * - :py:meth:`~ray.rllib.core.learner.learner.Learner.compute_loss_for_module()`
     - calculate the loss for gradient based update to a module.
   * - :py:meth:`~ray.rllib.core.learner.learner.Learner.before_gradient_based_update()`
     - do any non-gradient based updates to a RLModule before(!) the gradient based ones, e.g. add noise to your network.
   * - :py:meth:`~ray.rllib.core.learner.learner.Learner.after_gradient_based_update()`
     - do any non-gradient based updates to a RLModule after(!) the gradient based ones, e.g. update a loss coefficient based on some schedule.

Starter Example
---------------

A :py:class:`~ray.rllib.core.learner.learner.Learner` that implements behavior cloning could look like the following:

.. testcode::
    :hide:

    from typing import Any, Dict, DefaultDict

    from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
    from ray.rllib.core.learner.learner import Learner
    from ray.rllib.core.learner.torch.torch_learner import TorchLearner
    from ray.rllib.policy.sample_batch import SampleBatch
    from ray.rllib.utils.annotations import override
    from ray.rllib.utils.numpy import convert_to_numpy
    from ray.rllib.utils.typing import ModuleID, TensorType

.. testcode::

    class BCTorchLearner(TorchLearner):

        @override(Learner)
        def compute_loss_for_module(
            self,
            *,
            module_id: ModuleID,
            config: AlgorithmConfig = None,
            batch: Dict[str, Any],
            fwd_out: Dict[str, TensorType],
        ) -> TensorType:

            # standard behavior cloning loss
            action_dist_inputs = fwd_out[SampleBatch.ACTION_DIST_INPUTS]
            action_dist_class = self._module[module_id].get_train_action_dist_cls()
            action_dist = action_dist_class.from_logits(action_dist_inputs)
            loss = -torch.mean(action_dist.logp(batch[SampleBatch.ACTIONS]))

            return loss
