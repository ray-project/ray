.. include:: /_includes/rllib/we_are_hiring.rst

.. include:: /_includes/rllib/new_api_stack.rst

.. include:: /_includes/rllib/new_api_stack_component.rst


.. _connector-v2-docs:


Connector (V2) and Connector Pipelines
======================================

As explained in the :ref:`section on Episodes <single-agent-episode-docs>`, RLlib stores and
transports all trajectory data in the form of `Episodes` (single- or multi-agent) and only translates
this data into NN-readable tensor batches right before the model forward pass.

The components that perform such translations (from episodes to batches or from model outputs to
action tensors) are called `connector pipelines`. There are three different types of `connector pipelines`
in RLlib, 1) env-to-module connector pipelines, 2) module-to-env connector pipelines,
and 3) Learner connector pipelines.

.. figure:: images/connector_v2/usage_of_connector_pipelines.svg
    :width: 750
    :align: left

    **Connector(V2) Pipelines**: Connector pipelines convert episode data into batched data
    for processing by a neural network or convert model outputs to actions to be sent to an RL environment.
    An env-to-module pipeline (on an :py:class:`~ray.rllib.env.env_runner.EnvRunner`) takes a list of
    episodes as input and outputs the forward batch used in the :py:class:`~ray.rllib.core.rl_module.rl_module.RLModule`
    on the :py:class:`~ray.rllib.env.env_runner.EnvRunner` to compute the next action. A module-to-env pipeline takes
    an RLModule's output and converts it into actions for the next `step()` call on the RL environment. Lastly,
    a Learner connector pipeline takes a list of episodes as input and outputs the train batch(es) for the
    :py:class:`~ray.rllib.core.rl_module.rl_module.RLModule` sitting on a :py:class:`~ray.rllib.core.learner.learner.Learner`
    worker.


.. hint::

    RLlib is currently in a transition state from old- to new API stack.
    This page only covers the new API stack's :py:class:`~ray.rllib.connectors.connector_v2.ConnectorV2`
    and :py:class:`~ray.rllib.connectors.connector_pipeline_v2.ConnectorPipelineV2` classes. Thus the term "connector" here
    refers to this `ConnectorV2` class and should not be confused with the old API stack's `Connector` and `ConnectorPipeline` classes.


We talk in detail about each of these three types further below, however, all three pipelines have the following things in common:

* All connector pipelines are sequences of one or more :py:class:`~ray.rllib.connectors.connector_v2.ConnectorV2` pieces, some of which may be another pipeline (nesting is supported).
* All connector pieces (and also any pipeline) are callable overriding the python `__call__` method. When a pipeline is called, the individual pieces therein receive input data and hand their output data to the subsequent piece in the pipeline.
* All connector pipelines are callable and they are called with the same set of arguments, the main ones being a list of episodes, a batch or action tensor to-be-built, and an RLModule instance. See the :py:meth:`~ray.rllib.connectors.connector_v2.ConnectorV2.__call__` method for more details.
* All connector pipelines can read from and write to all the provided episodes and thereby manipulate these episodes as required. The visibility on those episodes is "complete", meaning the connector can access any data inside the episode (observations, actions, rewards, RNN-states, etc..) at any timestep.


Three ConnectorV2 Pipeline Types
================================

In the following, the three pipeline types (env-to-module, module-to-env, and learner) are described in more detail.

Env-to-module Pipeline
----------------------

One env-to-module pipeline is located on each :py:class:`~ray.rllib.env.env_runner.EnvRunner` (see figure below) and is responsible for connecting the `gymnasium.Env` with
the :py:class:`~ray.rllib.core.rl_module.rl_module.RLModule`.
When calling the env-to-module pipeline, a translation from a list of ongoing :ref:`Episode objects <single-agent-episode-docs>` to an
RLModule-readable tensor batch takes place and the output of the pipeline is directly sent into the RLModule's
`forward_inference` or `forward_exploration` method (depending on the user's exploration settings).

.. hint::

    Set `config.exploration(explore=True)` in your `AlgorithmConfig` to make sure your RLModule's
    :py:meth:`~ray.rllib.core.rl_module.rl_module.RLModule.forward_exploration` method is called with the connector's output.
    Otherwise, the EnvRunner calls :py:meth:`~ray.rllib.core.rl_module.rl_module.RLModule.forward_inference`.
    Note also that usually these two methods only differ in that actions are sampled when `explore=True` and
    greedily picked when `explore=False`. the exact behavior depends on your :ref:`RLModule's implementation <rlmodule-guide>`.


.. figure:: images/connector_v2/env_runner_connector_pipelines.svg
    :width: 750
    :align: left

    **EnvRunner Connector(V2) Pipelines**: The env-to-module pipeline sits between the RL environment (gymnasium.Env) and the RLModule,
    translating ongoing episodes into RLModule-readable batches (for the model's `forward_...()` methods).
    The module-to-env pipeline serves the other direction converting the RLModule's outputs (i.e. action logits,
    action distribution parameters, etc..) to actual actions understandable by the `gymnasium.Env` (for the next `step()` call).


Default Env-to-module Behavior
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

By default (if the user doesn't configure anything else), an env-to-module pipeline is populated with the following
built-in connector pieces, which perform the following tasks:


* :py:class:`~ray.rllib.connectors.common.add_observations_from_episodes_to_batch.AddObservationsFromEpisodesToBatch`: Places the most recent observation from each ongoing episode into the batch. Note that if you have a vector of `N` environments per `EnvRunner`, your batch size (number of observations) will also be `N`.
* *For stateful models only:* :py:class:`~ray.rllib.connectors.common.add_states_from_episodes_to_batch.AddStatesFromEpisodesToBatch`: Places the most recent state outputs of your module (as new state inputs) into the batch and adds a 1 timestep second axis (axis=1) to all data (to make it sequential).
* *For multi-agent only:* :py:class:`~ray.rllib.connectors.common.agent_to_module_mapping.AgentToModuleMapping`
* :py:class:`~ray.rllib.connectors.common.batch_individual_items.BatchIndividualItems`: Now that all data has been placed in the batch, convert the individual batch items into batched data structures.
* :py:class:`~ray.rllib.connectors.common.numpy_to_tensor.NumpyToTensor`: Converts all numpy arrays in the batch into actual framework specific tensors and moves these to the GPU if required.


Module-to-env Pipeline
----------------------
One module-to-env pipeline is located on each :py:class:`~ray.rllib.env.env_runner.EnvRunner` (see preceding figure) and is responsible for connecting the
:py:class:`~ray.rllib.core.rl_module.rl_module.RLModule` with the `gymnasium.Env`.
When calling the module-to-env pipeline, a translation from a model output dict - possibly containing action logits or distribution parameters -
to actual env-readable actions takes place. Note that a model-to-env connector also has access to the same list of ongoing :ref:`Episode objects <single-agent-episode-docs>`
that the env-to-module connector already saw, however, there is usually no need to access them (or write to them) in this pipeline.

The output of the module-to-env pipeline is directly sent to the RL environment (`gymnasium.Env`) for its next `step()` call.


Learner Pipeline
----------------
One learner pipeline is located on each :py:class:`~ray.rllib.core.learner.learner.Learner` worker (see figure below) and is reponsible for
compiling the train batch for the RLModule from a list of episode (trajectory) data.




Default- and Custom ConnectorPipelines
======================================

Each of the three mentioned connector pipeline types are stacked
