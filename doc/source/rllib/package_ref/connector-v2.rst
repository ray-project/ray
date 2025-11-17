.. include:: /_includes/rllib/we_are_hiring.rst


.. _connector-v2-reference-docs:

ConnectorV2 API
===============

.. include:: /_includes/rllib/new_api_stack.rst

.. currentmodule:: ray.rllib.connectors.connector_v2

rllib.connectors.connector_v2.ConnectorV2
-----------------------------------------

.. autoclass:: ray.rllib.connectors.connector_v2.ConnectorV2
    :special-members: __call__
    :members:


rllib.connectors.connector_pipeline_v2.ConnectorPipelineV2
----------------------------------------------------------

.. autoclass:: ray.rllib.connectors.connector_pipeline_v2.ConnectorPipelineV2
    :members:


Observation preprocessors
=========================

.. currentmodule:: ray.rllib.connectors.env_to_module.observation_preprocessor

rllib.connectors.env_to_module.observation_preprocessor.SingleAgentObservationPreprocessor
------------------------------------------------------------------------------------------

.. autoclass:: ray.rllib.connectors.env_to_module.observation_preprocessor.SingleAgentObservationPreprocessor

    .. automethod:: recompute_output_observation_space
    .. automethod:: preprocess


rllib.connectors.env_to_module.observation_preprocessor.MultiAgentObservationPreprocessor
-----------------------------------------------------------------------------------------

.. autoclass:: ray.rllib.connectors.env_to_module.observation_preprocessor.MultiAgentObservationPreprocessor

    .. automethod:: recompute_output_observation_space
    .. automethod:: preprocess
