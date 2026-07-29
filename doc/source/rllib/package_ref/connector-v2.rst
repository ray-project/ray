.. _connector-v2-reference-docs:

ConnectorV2 API
===============

.. include:: /_includes/rllib/new_api_stack.rst

.. currentmodule:: ray.rllib.connectors.connector_v2

rllib.connectors.connector_v2.ConnectorV2
-----------------------------------------

.. autoclass:: ConnectorV2
    :special-members: __call__
    :members:


rllib.connectors.connector_pipeline_v2.ConnectorPipelineV2
----------------------------------------------------------

.. currentmodule:: ray.rllib.connectors.connector_pipeline_v2

.. autoclass:: ConnectorPipelineV2
    :members:


Observation preprocessors
=========================

.. currentmodule:: ray.rllib.connectors.env_to_module.observation_preprocessor

rllib.connectors.env_to_module.observation_preprocessor.SingleAgentObservationPreprocessor
------------------------------------------------------------------------------------------

.. autoclass:: SingleAgentObservationPreprocessor

    .. automethod:: recompute_output_observation_space
    .. automethod:: preprocess


rllib.connectors.env_to_module.observation_preprocessor.MultiAgentObservationPreprocessor
-----------------------------------------------------------------------------------------

.. autoclass:: MultiAgentObservationPreprocessor

    .. automethod:: recompute_output_observation_space
    .. automethod:: preprocess
