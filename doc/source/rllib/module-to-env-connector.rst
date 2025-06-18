
.. _module-to-env-pipeline-docs:

Module-to-env Pipeline
++++++++++++++++++++++

A single module-to-env pipeline is located on each :py:class:`~ray.rllib.env.env_runner.EnvRunner` (see preceding figure) and responsible for connecting the
EnvRunner's :py:class:`~ray.rllib.core.rl_module.rl_module.RLModule` with the `gymnasium.Env <https://gymnasium.farama.org/api/env/>`__.
When calling the module-to-env pipeline, a translation takes place from the model's output, possibly containing action logits or distribution parameters,
to actions readable by the environment. Note that a model-to-env connector has access to the same list of ongoing :ref:`Episode objects <single-agent-episode-docs>`
that the env-to-module connector already saw, however, there is usually no need to access them (or write to them) in this pipeline.

The output of the module-to-env pipeline is directly sent to the RL environment (`gymnasium.Env <https://gymnasium.farama.org/api/env/>`__) for its next `step()` call.

.. _default-module-to-env-pipeline:

**Default Module-to-Env Behavior:** By default, if you don't specifically say otherwise in your config, a module-to-env pipeline is populated with these
built-in connector pieces performing the following tasks:

* :py:class:`~ray.rllib.connectors.module_to_env.get_actions.GetActions`: Checks, whether the "actions" key is already part of the RLModule's output and if not, samples actions using the obligatory "action_dist_inputs" key.
* :py:class:`~ray.rllib.connectors.common.tensor_to_numpy.TensorToNumpy`: Converts all framework specific tensors in the batch to NumPy arrays.
* :py:class:`~ray.rllib.connectors.module_to_env.unbatch_to_individual_items.UnBatchToIndividualItems`: Un-batches all data, meaning converts from NumPy arrays with a batch axis=0 to lists of individual batch items w/o such batch axis.
* *For multi-agent only:* :py:class:`~ray.rllib.connectors.common.module_to_agent_unmapping.ModuleToAgentUnmapping`: Maps per-module data back to the respective per-agent data depending on the previously performed agent-to-module mapping.
* *For stateful models only:* :py:class:`~ray.rllib.connectors.module_to_env.remove_single_ts_time_rank_from_batch.RemoveSingleTsTimeRankFromBatch`: Removes a previously added 1-timestep second axis (axis=1) from all data. This means it reverts the transformation by the :py:class:`~ray.rllib.connectors.common.add_states_from_episodes_to_batch.AddStatesFromEpisodesToBatch` piece in the env-to-module pipeline.
* :py:class:`~ray.rllib.connectors.module_to_env.normalize_and_clip_actions.NormalizeAndClipActions`: Translates the computed/sampled actions from your neural-network form (assumed to be somewhat within a small range) to the `gymnasium.Env <https://gymnasium.farama.org/api/env/>`__'s action space or - alternatively - clips the computed/sampled actions to the env's action ranges. Note that this step is only relevant for non-Discrete action spaces.
* :py:class:`~ray.rllib.connectors.module_to_env.listify_data_for_vector_env.ListifyDataForVectorEnv`: Converts data from the connector pipeline specific format into plain lists, matching the `gymnasium.Env <https://gymnasium.farama.org/api/env/>`__ vector in size.

It's discussed further below :ref:`how you can customize the behavior of the module-to-env pipeline <customizing-connector-v2-pipelines>` by adding any number of `ConnectorV2` pieces to it.

.. hint::

    You can disable the default connector pieces by setting `config.env_runners(add_default_connectors_to_module_to_env_pipeline=False)`
    in your :ref:`algorithm config <rllib-algo-configuration-docs>`.



Adding custom module-to-env connectors
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Similarly, you can add a custom module-to-env piece to your pipeline (or several
by returning a list from your lambda).

.. testcode::
    :skipif: True

    # Add a module-to-env connector piece to the default module-to-env pipeline.
    # Note that the lambda takes the `gymnasium.Env` as only argument.
    config.env_runners(
        module_to_env_connector=lambda env: MyModuleToEnvConnector(..),
    )
    # Return a list of module-to-env connector instances from the `lambda`, if you would like to add more
    # than one connector piece to the custom pipeline.

