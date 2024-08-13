.. include:: /_includes/rllib/we_are_hiring.rst

.. include:: /_includes/rllib/new_api_stack.rst

.. include:: /_includes/rllib/new_api_stack_component.rst


.. _connector-v2-docs:


Connector (V2) and Connector Pipelines
======================================

As explained in the :ref:`section on Episodes <single-agent-episode-docs>`, RLlib stores and
transports all trajectory data in the form of `Episodes` (single- or multi-agent) and only translates
this data into NN-readable tensor batches right before the model forward pass.

The components that perform this translation (from episodes to batches) are called `connector pipelines`.
There are three different types of `connector pipelines` in RLlib, 1) env-to-module connector pipelines, 2)
module-to-env connector pipelines, and 3) Learner connector pipelines.

.. figure:: images/connector_v2/usage_of_connector_pipelines.svg
    :width: 750
    :align: left

    **Connector(V2) Pipelines**: Connector pipelines convert episode (trajectory) data into forward batches
    for neural networks or model outputs to actions (which are then written back into the episodes).
    An env-to-module pipeline (on an :py:class:`~ray.rllib.env.env_runner.EnvRunner`) takes a list of
    episodes as input and outputs the forward batch used in the :py:class:`~ray.rllib.core.rl_module.rl_module.RLModule`
    on the :py:class:`~ray.rllib.env.env_runner.EnvRunner` to compute the next action. A module-to-env pipeline takes
    the module's output and converts it into actions for the next `step()` of the RL environment. Lastly,
    a Learner connector pipeline takes a list of episodes as input and outputs the train batch used in the
    :py:class:`~ray.rllib.core.rl_module.rl_module.RLModule` on the :py:class:`~ray.rllib.core.learner.learner.Learner`
    to eventually compute the loss.


We will talk in detail about each of these three types further below, however, all three have the following things in common:



* All connector pipelines are sequences of one or more :py:class:`~ray.rllib.connectors.connector_v2.ConnectorV2` pieces, each of which may be another pipeline. The individual pieces receive input data and hand their output data to the subsequent piece in the pipeline.
* All connector pipelines are callable and they are called with the same set of arguments, the main ones being a list of episodes, a batch (data dict), and an RLModule instance.
* All connector pipelines can read from and write to all provided episodes and thereby manipulate the episodes as required. The visibility when reading is "complete", meaning the connector can
* Every connector pipeline has access to a list of input episodes. These are either the ongoing, currently collected episodes (trajectories) on an EnvRunner  connector pipeline is responsible for translating a list of episodes and a data dict (which may be empty initially) into a
only immediately before a neural network forward pass by so called "connector pipelines".

