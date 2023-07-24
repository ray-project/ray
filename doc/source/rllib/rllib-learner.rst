.. include:: /_includes/rllib/announcement.rst

.. include:: /_includes/rllib/we_are_hiring.rst

.. |tensorflow| image:: images/tensorflow.png
    :class: inline-figure
    :width: 16

.. |pytorch| image:: images/pytorch.png
    :class: inline-figure
    :width: 16


Learner (Alpha)
==================

:py:class:`~ray.rllib.core.learner.learner.Learner` allows you to abstract the training 
logic of RLModules. It supports both gradient-based and non-gradient-based updates (e.g.
polyak averaging, etc.) The API is designed so that it can be distributed using data-
distributed parallel (DDP). The Learner achieves the following:


(1) Facilitating gradient-based updates on `RLModule`.
(2) Providing abstractions for non-gradient based updates such as polyak averaging, etc.
(3) Reporting training statistics.
(4) Checkpointing the modules and optimizer states for durable training.

The :py:class:`~ray.rllib.core.learner.learner.Learner` class supports data-distributed-
parallel style training via the 
:py:class:`~ray.rllib.core.learner.learner_group.LearnerGroup` API. Under this paradigm, 
the :py:class:`~ray.rllib.core.learner.learner_group.LearnerGroup` maintains multiple 
copies of the same :py:class:`~ray.rllib.core.learner.learner.Learner` with identical 
parameters and hyperparameters. Each of these
:py:class:`~ray.rllib.core.learner.learner.Learner` s computes the loss and gradients on a
shard of a sample batch and then accumulates the gradients across the 
:py:class:`~ray.rllib.core.learner.learner.Learner` s. Learn more about data-distributed 
parallel learning in 
`this article. <https://pytorch.org/tutorials/intermediate/ddp_tutorial.html>`_

:py:class:`~ray.rllib.core.learner.learner_group.LearnerGroup` also allows for 
asynchronous training and (distributed) checkpointing for durability during training.

How To Enable Learner API In RLlib Experiments
==============================================

You can adjust the amount of resources for training using the 
`num_gpus_per_learner_worker`, `num_cpus_per_learner_worker`, and `num_learner_workers`
arguments in the `AlgorithmConfig`.

.. testcode::
	:hide:
	from ray.rllib.algorithms.ppo.ppo import PPOConfig

.. testcode::

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

	config = config.environment("CartPole-v1")
	config.build()  # test that the algorithm can be built with the given resources


.. note::
    
    This features is in beta. If you migrate to this algorithm, enable the feature by 
    setting `_enable_learner_api` and `_enable_rl_module_api` flags in the 
    `AlgorithmConfig`.

    The following algorithms support `Learner` out of the box. Implement
    an algorithm with a custom `Learner` to leverage this API for other Algorithms.

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


Basic Usage
===========

Use the `LearnerGroup` utility to interact with multiple learners. 

Construction
------------

If you enable the `RLModule`
and `Learner` APIs via the RLlib algorithm config, then `Algorithm` will construct a `LearnerGroup` for you, but if you’re using these APIs standalone, you can construct the `LearnerGroup` as follows.

.. testcode::
    :hide:
    # imports for the examples

    import gymnasium as gym
    import ray
    from ray.rllib.algorithms.ppo.torch.ppo_torch_rl_module import PPOTorchRLModule
    from ray.rllib.algorithms.ppo.torch.ppo_torch_learner import PPOTorchLearner
    from ray.rllib.algorithms.ppo.ppo_learner import PPOLearnerHyperparameters
    from ray.rllib.core.rl_module.rl_module import SingleAgentRLModuleSpec
    from ray.rllib.core.learner.learner import FrameworkHyperparameters, LearnerSpec
    from ray.rllib.core.learner.learner_group import LearnerGroup
    from ray.rllib.core.learner.scaling_config import LearnerGroupScalingConfig
    ray.init()


.. tab-set::
    
    .. tab-item:: Contstructing a `LearnerGroup`


        .. testcode::

            env = gym.make("CartPole-v1")

            module_spec = SingleAgentRLModuleSpec(
                            module_class=PPOTorchRLModule,
                            observation_space=env.observation_space,
                            action_space=env.action_space,
                        )

            hparams = PPOLearnerHyperparameters(use_kl_loss=True, 
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

    .. tab-item:: Constructing a `Learner`

        .. testcode::

            env = gym.make("CartPole-v1")

            module_spec = SingleAgentRLModuleSpec(
                            module_class=PPOTorchRLModule,
                            observation_space=env.observation_space,
                            action_space=env.action_space,
                        )

            hparams = PPOLearnerHyperparameters(use_kl_loss=True, 
                                                kl_coeff=0.01,
                                                kl_target=0.05, 
                                                clip_param=0.2, 
                                                vf_clip_param=0.2, 
                                                entropy_coeff=0.05,
                                                vf_loss_coeff=0.5
                        )

            learner = PPOTorchLearner(module_spec=module_spec, 
                                      learner_hyperparameters=hparams,
                                      framework_hyperparameters=FrameworkHyperparameters()
                                      )

Updates
-------

.. testcode::
    :hide:

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
        SampleBatch.AGENT_INDEX: np.array([0, 0, 0])
    }
    DUMMY_BATCH = SampleBatch(DUMMY_BATCH)


.. tab-set::
        
    .. tab-item:: Updating a `LearnerGroup`

        .. testcode::

            # this is a blocking update
            results = learner_group.update(DUMMY_BATCH)

            # This is a non-blocking update. The results will be returned in a future
            # call to `async_update`
            results = learner_group.async_update(DUMMY_BATCH)

            # This is an additional non-gradient based update
            learner_group.additional_update()

        When updating a `LearnerGroup` you can perform blocking or async updates on batches of data. Async updates are necessary for implementing async algorithms such as APPO/IMPALA.
        You can perform non-gradient based updates using `additional_update`.

    .. tab-item:: Updating a `Learner`

        .. testcode::

            # this is a blocking update
            result = learner.update(DUMMY_BATCH)

            # This is an additional non-gradient based update
            learner_group.additional_update()

        When updating a `Learner` you can only perform blocking updates on batches of data.
        You can perform non-gradient based updates using `additional_update`.