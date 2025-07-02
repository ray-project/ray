TODO (sven): weave this in here from env-to-module. It should go here as it requires a learner connector equivalent piece.

Stacking the N most recent observations
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If you would like to write a custom env-to-module connector that stacks the `N` most recent observations and feeds
this stack of observations into your RLModule (for example in an attention/transformer architecture), you can
achieve this, too, by subclassing `ConnectorV2` and overriding the `__call__` method.

However, in this case, the implementation shouldn't write back the stacked observations into the episode
(as updated observation), because doing so would make the next call to the same ConnectorV2 piece to look back onto
an already stacked previous observation. Instead, you should manipulate the `batch` directly, as in this example:


.. testcode::

    import gymnasium as gym
    import numpy as np
    from ray.rllib.connectors.connector_v2 import ConnectorV2
    from ray.rllib.core.columns import Columns


    class StackLast10Observations(ConnectorV2):

        def recompute_output_observation_space(self, in_obs_space, in_act_space):
            # Assume the input observation space is a Box of shape (N,).
            assert (
                isinstance(input_observation_space, gym.spaces.Box)
                and len(input_observation_space.shape) == 1
            )

            # This connector concatenates the last 10 observations at axis=0, so the
            # output space has a shape of (10*N,).
            return gym.spaces.Box(
                low=input_observation_space.low,
                high=input_observation_space.high,
                shape=(input_observation_space.shape[0] * 10,),
                dtype=input_observation_space.dtype,
            )

        def __call__(self, *, rl_module, batch, episodes, **kwargs):
            # Assume that the input `batch` is empty. Note that this may not be the case
            # if you have other custom connector pieces before this one.
            assert not batch

            # Loop through all (single-agent) episodes.
            for single_agent_episode in self.single_agent_episode_iterator(episodes):
                # Get the 10 most recent observations from the episodes.
                last_10_obs = single_agent_episode.get_observations(
                    indices=[-10, -9, -8, -7, -6, -5, -4, -3, -2, -1], fill=0.0
                )
                # Concatenate the two observations.
                new_obs = np.concatenate(last_10_obs, axis=0)

                # Add the new observation to the `batch` using the
                # `ConnectorV2.add_batch_item()` utility.
                self.add_batch_item(
                    batch=batch,
                    column=Columns.OBS,
                    item_to_add=new_obs,
                    single_agent_episode=single_agent_episode,
                )

                # Note that we do not write the stacked observations back into the episode
                # as this would interfere with the next call of this same connector (it
                # would try to stack already stacked observations and thus produce a shape error).

            # Return batch (with stacked observations).
            return batch


Since the returned `batch` in the preceding env-to-module piece is discarded after the model forward pass
and not stored in the episodes, you have to make sure to perform the framestacking again on the Learner
side of things.


.. tip::
    There are already off-the-shelf ConnectorV2 pieces available to you. These perform the task of
    stacking the last `N` observations in both the env-to-module and Learner pipelines:

    .. code-block:: python

        from ray.rllib.connectors.common.frame_stacking import FrameStacking

        # Framestacking on the EnvRunner side.
        config.env_runners(
            env_to_module_connector=lambda env, spaces, device: FrameStacking(num_frames=N),
        )
        # Then again on the Learner side.
        config.training(
            learner_connector=lambda obs_space, act_space: FrameStacking(num_frames=N, as_learner_conector=True),
        )


END (sven)









.. _learner-pipeline-docs:

Learner Pipeline
++++++++++++++++

One learner pipeline is located on each :py:class:`~ray.rllib.core.learner.learner.Learner` worker (see figure below) and is responsible for
compiling the train batch for the :py:class:`~ray.rllib.core.rl_module.rl_module.RLModule` from a list of episodes (trajectory data).


.. figure:: images/connector_v2/learner_connector_pipeline.svg
    :width: 1000
    :align: left

    **Learner ConnectorV2 Pipelines**: A learner connector pipeline sits between the input training data into the
    :py:class:`~ray.rllib.core.learner.learner.Learner` worker and its :py:class:`~ray.rllib.core.rl_module.rl_module.RLModule`.
    It translates the input data, lists of episodes, into a train batch, tensor data, readable by the RLModule's
    :py:meth:`~ray.rllib.core.rl_module.rl_module.RLModule.forward_train` method.

When calling the Learner connector pipeline, a translation from a list of :ref:`Episode objects <single-agent-episode-docs>` to an
RLModule-readable tensor batch (the "train batch") takes place and the output of the pipeline is directly sent into the
:py:meth:`~ray.rllib.core.rl_module.rl_module.RLModule.forward_train` method of the :py:class:`~ray.rllib.core.rl_module.rl_module.RLModule`.

.. _default-learner-pipeline:

**Default Learner Pipeline Behavior:** By default (if you don't configure anything else), a Learner connector pipeline is
populated with the following built-in connector pieces, which perform the following tasks:

* :py:class:`~ray.rllib.connectors.common.add_observations_from_episodes_to_batch.AddObservationsFromEpisodesToBatch`: Places all observations from the incoming episodes into the batch. For example, if you have 2 incoming episodes of length 10 and 20, your resulting train batch size is 30 (10 + 20).
* :py:class:`~ray.rllib.connectors.learner.add_columns_from_episodes_to_batch.AddColumnsFromEpisodesToBatch`: Places all other columns (rewards, actions, terminated flags, etc..) from the incoming episodes into the batch.
* *For stateful models only:* :py:class:`~ray.rllib.connectors.common.add_states_from_episodes_to_batch.AddStatesFromEpisodesToBatch`: Adds a time-dimension of size `max_seq_len` at axis=1 to all data in the batch and (right) zero-pads in cases where episodes end at timesteps non-dividable by `max_seq_len`. You can change `max_seq_len` through your RLModule's `model_config_dict` (call `config.rl_module(model_config_dict={'max_seq_len': ...})` on your :py:class:`~ray.rllib.algorithms.algorithm_config.AlgorithmConfig` object). Also places every `max_seq_len`th state output of your module from the incoming episodes into the train batch (as new state inputs).
* *For multi-agent only:* :py:class:`~ray.rllib.connectors.common.agent_to_module_mapping.AgentToModuleMapping`: Maps per-agent data to the respective per-module data depending on the already determined agent-to-module mapping stored in each (multi-agent) episode.
* :py:class:`~ray.rllib.connectors.common.batch_individual_items.BatchIndividualItems`: Now that all data has been placed in the batch, convert the individual batch items into batched data structures (lists of individual items are converted to NumPy arrays).
* :py:class:`~ray.rllib.connectors.common.numpy_to_tensor.NumpyToTensor`: Converts all NumPy arrays in the batch into actual framework specific tensors and moves these to the GPU if required.

It's discussed further below :ref:`how you can customize the behavior of the Learner pipeline <customizing-connector-v2-pipelines>` by adding any number of `ConnectorV2` pieces to it.


Adding custom Learner connectors
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To add custom Learner connector pieces, you need to call the
:py:meth:`~ray.rllib.algorithms.algorithm_config.AlgorithmConfig.learners` method of the algorithm config:

.. testcode::
    :skipif: True

    # Add a Learner connector piece to the default Learner pipeline.
    # Note that the lambda takes the input observation- and input action spaces as
    # arguments.
    config.learners(
        learner_connector=lambda obs_space, act_space: MyLearnerConnector(..),
    )
    # Return a list of Learner instances from the `lambda`, if you would like to add more
    # than one connector piece to the custom pipeline.


Adding past rewards to the Model's input
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If you want to add the most recent reward to be part of the next observation going into your
:py:class:`~ray.rllib.core.rl_module.rl_module.RLModule`, you can write a custom
env-to-module :py:class:`~ray.rllib.connectors.connector_v2.ConnectorV2` piece like so:


.. testcode::

    import gymnasium as gym
    import numpy as np
    from ray.rllib.connectors.connector_v2 import ConnectorV2


    class AddLastRewardToObservations(ConnectorV2):

        # Define how the observation space will change because of this connector piece.
        def recompute_output_observation_space(self, in_obs_space, in_act_space):
            # For simplicity, assert input obs space is a 1D Box
            assert isinstance(in_obs_space, gym.spaces.Box) and len(in_obs_space.shape) == 1
            return gym.spaces.Box(
                low=list(in_obs_space.low) + [float("-inf")],
                high=list(in_obs_space.high) + [float("inf")],
                shape=(in_obs_space.shape[0] + 1,),
                dtype=in_obs_space.dtype,
            )

        # Define the actual transformation.
        def __call__(self, *, rl_module, batch, episodes, **kwargs):
            # Loop through all `episodes`.
            for single_agent_episode in self.single_agent_episode_iterator(episodes):
                # Get last reward (or 0.0 if right after `env.reset`).
                reward = single_agent_episode.get_reward(-1, fill=0.0)
                # Get last observation.
                obs = single_agent_episode.get_observation(-1)
                # Append reward to obs array.
                new_obs = np.append(obs, reward)

                # Write new observation back into the episode.
                single_agent_episode.set_observation(new_value=new_obs, at_index=-1)

            return batch


.. tip::
    There is already an off-the-shelf ConnectorV2 piece available for you, which performs the task of
    adding the `N` most recent rewards and/or `M` most recent actions to the observations:

    .. code-block:: python

        from ray.rllib.connectors.env_to_module.prev_actions_prev_rewards import PrevActionsPrevRewards

        config.env_runners(
            env_to_module_connector=lambda env: PrevActionsPrevRewards(n_prev_rewards=N, n_prev_actions=M),
        )


If you plug in this custom :py:class:`~ray.rllib.connectors.connector_v2.ConnectorV2` class into your algorithm config
(`config.env_runners(env_to_module_connector=lambda env: AddLastRewardToObservations())`),
your model should receive observations that have the most recent reward "attached" to it.

Notice that in the example here, the transformed observations were written right back into the given episodes
and not placed into the `batch`. This strategy of writing back those data that was pulled from episodes right back
into the same episodes makes sure that from this point on, only the changed data is visible to all subsequent components (for example
other ConnectorV2 pieces in the same pipeline or other ConnectorV2 pipelines). It does not touch the `batch`.
However, knowing that one of the subsequent :ref:`default env-to-module pieces <default-env-to-module-pipeline>`
is going to do exactly that, we can defer this task (of populating the batch with your changed data) to these default pieces.

The next example, however, demonstrates a specific case - observation stacking - where this strategy fails and in
which you should instead manipulate the `batch` directly.
