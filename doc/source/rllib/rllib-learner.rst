.. include:: /_includes/rllib/announcement.rst

.. include:: /_includes/rllib/we_are_hiring.rst

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
`num_gpus_per_learner_worker`, `num_cpus_per_learner_worker`, and `num_learner_workers`
arguments in the :py:class:`~ray.rllib.algorithms.algorithm_config.AlgorithmConfig`.

.. testcode::
	:hide:
    :skipif: True

    from ray.rllib.algorithms.ppo.ppo import PPOConfig

.. testcode::
    :skipif: True

    config = (
        PPOConfig()
        .resources(
            num_gpus_per_learner_worker=0,  # Set this to 1 to enable GPU training.
            num_cpus_per_learner_worker=1,
            num_learner_workers=0  # Set this to greater than 0 to allow for DDP style 
                               # updates.
        )
        .training(_enable_learner_api=True)
        .rl_module(_enable_rl_module_api=True)
    )

.. testcode::
	:hide:
    :skipif: True

	from gymnasium.envs.classic_control.cartpole import CartPoleEnv

	config = config.environment(env=CartPoleEnv)
	config.build()  # test that the algorithm can be built with the given resources


.. note::
    
    This features is in alpha. If you migrate to this algorithm, enable the feature by 
    setting `_enable_learner_api` and `_enable_rl_module_api` flags in the 
    `AlgorithmConfig`.

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
    :skipif: True

    # imports for the examples

    import numpy as np
    import gymnasium as gym
    import ray
    from ray.rllib.algorithms.ppo.torch.ppo_torch_rl_module import PPOTorchRLModule
    from ray.rllib.algorithms.ppo.torch.ppo_torch_learner import PPOTorchLearner
    from ray.rllib.algorithms.ppo.ppo_catalog import PPOCatalog
    from ray.rllib.algorithms.ppo.ppo_learner import PPOLearnerHyperparameters
    from ray.rllib.core.rl_module.rl_module import SingleAgentRLModuleSpec
    from ray.rllib.core.learner.learner import FrameworkHyperparameters, LearnerSpec
    from ray.rllib.core.learner.learner_group import LearnerGroup
    from ray.rllib.core.learner.scaling_config import LearnerGroupScalingConfig


.. tab-set::
    
    .. tab-item:: Contstructing a LearnerGroup


        .. testcode::
            :skipif: True

            env = gym.make("CartPole-v1")

            module_spec = SingleAgentRLModuleSpec(
                            module_class=PPOTorchRLModule,
                            observation_space=env.observation_space,
                            action_space=env.action_space,
                            model_config_dict={},
                            catalog_class=PPOCatalog
                        )

            hparams = PPOLearnerHyperparameters(
                use_kl_loss=True, 
                kl_coeff=0.01,
                kl_target=0.05, 
                clip_param=0.2, 
                vf_clip_param=0.2, 
                entropy_coeff=0.05,
                vf_loss_coeff=0.5
            )

            scaling_config = LearnerGroupScalingConfig(num_workers=1)

            learner_spec = LearnerSpec(
                                learner_class=PPOTorchLearner,
                                module_spec=module_spec,
                                learner_group_scaling_config=scaling_config,
                                learner_hyperparameters=hparams,
                                framework_hyperparameters=FrameworkHyperparameters(),
                            )

            learner_group = LearnerGroup(learner_spec)

    .. tab-item:: Constructing a Learner

        .. testcode::
            :skipif: True

            env = gym.make("CartPole-v1")

            module_spec = SingleAgentRLModuleSpec(
                            module_class=PPOTorchRLModule,
                            observation_space=env.observation_space,
                            action_space=env.action_space,
                            model_config_dict={},
                            catalog_class=PPOCatalog
                        )

            hparams = PPOLearnerHyperparameters(
                use_kl_loss=True, 
                kl_coeff=0.01,
                kl_target=0.05, 
                clip_param=0.2, 
                vf_clip_param=0.2, 
                entropy_coeff=0.05,
                vf_loss_coeff=0.5
            )

            learner = PPOTorchLearner(
                module_spec=module_spec, 
                learner_hyperparameters=hparams,
                framework_hyperparameters=FrameworkHyperparameters()
            )

Updates
-------

.. testcode::
    :hide:
    :skipif: True

    from ray.rllib.policy.sample_batch import (DEFAULT_POLICY_ID, SampleBatch, 
        MultiAgentBatch)
    from ray.rllib.evaluation.postprocessing import Postprocessing

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
    ADDITIONAL_UPDATE_KWARGS = {"timestep": 0, "sampled_kl_values": {DEFAULT_POLICY_ID: 1e-4}}

    learner.build() # needs to be called on the learner before calling any functions


.. tab-set::
        
    .. tab-item:: Updating a LearnerGroup

        .. testcode::
            :skipif: True

            # This is a blocking update
            results = learner_group.update(DUMMY_BATCH)

            # This is a non-blocking update. The results are returned in a future
            # call to `async_update`
            results = learner_group.async_update(DUMMY_BATCH)

            # This is an additional non-gradient based update.
            learner_group.additional_update(**ADDITIONAL_UPDATE_KWARGS)

        When updating a :py:class:`~ray.rllib.core.learner.learner_group.LearnerGroup` you can perform blocking or async updates on batches of data. Async updates are necessary for implementing async algorithms such as APPO/IMPALA.
        You can perform non-gradient based updates using :py:meth:`~ray.rllib.core.learner.learner_group.LearnerGroup.additional_update`.

    .. tab-item:: Updating a Learner

        .. testcode::
            :skipif: True

            # This is a blocking update.
            result = learner.update(DUMMY_BATCH)

            # This is an additional non-gradient based update.
            learner_group.additional_update(**ADDITIONAL_UPDATE_KWARGS)

        When updating a :py:class:`~ray.rllib.core.learner.learner.Learner` you can only perform blocking updates on batches of data.
        You can perform non-gradient based updates using :py:meth:`~ray.rllib.core.learner.learner.Learner.additional_update`.
    

Getting and setting state
-------------------------

.. tab-set::
    
    .. tab-item:: Getting and Setting State for a LearnerGroup

        .. testcode::
            :skipif: True

            # module weights and optimizer states
            state = learner_group.get_state()
            learner_group.set_state(state)

            # just module weights
            weights = learner_group.get_weights()
            learner_group.set_weights(weights)

        Set/get the state dict of all learners through learner_group via 
        :py:meth:`~ray.rllib.core.learner.learner_group.LearnerGroup.set_state` or 
        :py:meth:`~ray.rllib.core.learner.learner_group.LearnerGroup.get_state`. 
        This includes all states including both neural network weights, 
        and optimizer states on each learner. You can set and get the weights of 
        the RLModule of all learners through learner_group via 
        :py:meth:`~ray.rllib.core.learner.learner_group.LearnerGroup.set_weights` or
        :py:meth:`~ray.rllib.core.learner.learner_group.LearnerGroup.get_weights`. 
        This does not include optimizer states.
    
    .. tab-item:: Getting and Setting State for a Learner

        .. testcode::
            :skipif: True

            # module weights and optimizer states
            state = learner.get_state()
            learner.set_state(state)

            # just module state
            module_state = learner.get_module_state()
            learner.module.set_module_state(module_state)

        You can set and get the weights of a :py:class:`~ray.rllib.core.learner.learner.Learner` 
        using :py:meth:`~ray.rllib.core.learner.learner.Learner.set_state` 
        and :py:meth:`~ray.rllib.core.learner.learner.Learner.get_state` .
        For setting or getting only RLModule weights (without optimizer states), use 
        :py:meth:`~ray.rllib.core.learner.learner.Learner.set_module_state` 
        or :py:meth:`~ray.rllib.core.learner.learner.Learner.get_module_state` API.


.. testcode::
	:hide:
    :skipif: True

	import shutil
	import tempfile

	LEARNER_CKPT_DIR = str(tempfile.TemporaryDirectory())
	LEARNER_GROUP_CKPT_DIR = str(tempfile.TemporaryDirectory())


Checkpointing
-------------

.. tab-set::
    
    .. tab-item:: Checkpointing a LearnerGroup

        .. testcode::
            :skipif: True

            learner_group.save_state(LEARNER_GROUP_CKPT_DIR)
            learner_group.load_state(LEARNER_GROUP_CKPT_DIR)
        
        Checkpoint the state of all learners in the :py:class:`~ray.rllib.core.learner.learner_group.LearnerGroup` via :py:meth:`~ray.rllib.core.learner.learner_group.LearnerGroup.save_state` and
        :py:meth:`~ray.rllib.core.learner.learner_group.LearnerGroup.load_state`. This includes all states including neural network weights and any
        optimizer states. Note that since the state of all of the :py:class:`~ray.rllib.core.learner.learner.Learner` instances is identical,
        only the states from the first :py:class:`~ray.rllib.core.learner.learner.Learner` need to be saved.

    .. tab-item:: Checkpointing a Learner

        .. testcode::
            :skipif: True

            learner.save_state(LEARNER_CKPT_DIR)
            learner.load_state(LEARNER_CKPT_DIR)

        Checkpoint the state of a :py:class:`~ray.rllib.core.learner.learner.Learner` 
        via :py:meth:`~ray.rllib.core.learner.learner.Learner.save_state` and 
        :py:meth:`~ray.rllib.core.learner.learner.Learner.load_state`. This 
        includes all states including neural network weights and any optimizer states.


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
   * - :py:meth:`~ray.rllib.core.learner.learner.Learner.additional_update_for_module()`
     - do any non gradient based updates to a RLModule, e.g. target network updates.
   * - :py:meth:`~ray.rllib.core.learner.learner.Learner.compile_results()`
     - compute training statistics and format them for downstream use.
     
Starter Example
---------------

A :py:class:`~ray.rllib.core.learner.learner.Learner` that implements behavior cloning could look like the following:

.. testcode::
    :hide:
    :skipif: True

    from typing import Any, Dict, DefaultDict, Mapping

    from ray.rllib.core.learner.learner import LearnerHyperparameters, Learner
    from ray.rllib.core.learner.torch.torch_learner import TorchLearner
    from ray.rllib.core.rl_module.rl_module import ModuleID
    from ray.rllib.policy.sample_batch import SampleBatch
    from ray.rllib.utils.annotations import override
    from ray.rllib.utils.nested_dict import NestedDict
    from ray.rllib.utils.typing import TensorType

.. testcode::
    :skipif: True

    class BCTorchLearner(TorchLearner):

        @override(Learner)
        def compute_loss_for_module(
            self,
            *,
            module_id: ModuleID,
            hps: LearnerHyperparameters,
            batch: NestedDict,
            fwd_out: Mapping[str, TensorType],
        ) -> Mapping[str, Any]:

            # standard behavior cloning loss 
            action_dist_inputs = fwd_out[SampleBatch.ACTION_DIST_INPUTS]
            action_dist_class = self._module[module_id].get_train_action_dist_cls()
            action_dist = action_dist_class.from_logits(action_dist_inputs)
            loss = -torch.mean(action_dist.logp(batch[SampleBatch.ACTIONS]))

            return loss


        @override(Learner)
        def compile_results(
            self,
            *,
            batch: NestedDict,
            fwd_out: Mapping[str, Any],
            loss_per_module: Mapping[str, TensorType],
            metrics_per_module: DefaultDict[ModuleID, Dict[str, Any]],
        ) -> Mapping[str, Any]:

            results = super().compile_results(
                batch=batch,
                fwd_out=fwd_out,
                loss_per_module=loss_per_module,
                metrics_per_module=metrics_per_module,
            )
            # report the mean weight of each 
            mean_ws = {}
            for module_id in self.module.keys():
                m = self.module[module_id]
                parameters = convert_to_numpy(self.get_parameters(m))
                mean_ws[module_id] = np.mean([w.mean() for w in parameters])
                results[module_id]["mean_weight"] = mean_ws[module_id]

            return results


