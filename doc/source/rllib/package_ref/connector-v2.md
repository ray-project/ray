---
myst:
  html_meta:
    description: "API reference for RLlib's ConnectorV2, ConnectorPipelineV2, and observation preprocessor classes."
---

(connector-v2-reference-docs)=

# ConnectorV2 API

```{eval-rst}
.. currentmodule:: ray.rllib.connectors.connector_v2
```

## rllib.connectors.connector_v2.ConnectorV2

```{eval-rst}
.. autoclass:: ConnectorV2
    :special-members: __call__
    :members:
```

## rllib.connectors.connector_pipeline_v2.ConnectorPipelineV2

```{eval-rst}
.. currentmodule:: ray.rllib.connectors.connector_pipeline_v2
```

```{eval-rst}
.. autoclass:: ConnectorPipelineV2
    :members:
```

# Observation preprocessors

```{eval-rst}
.. currentmodule:: ray.rllib.connectors.env_to_module.observation_preprocessor
```

## rllib.connectors.env_to_module.observation_preprocessor.SingleAgentObservationPreprocessor

```{eval-rst}
.. autoclass:: SingleAgentObservationPreprocessor

    .. automethod:: recompute_output_observation_space
    .. automethod:: preprocess
```

## rllib.connectors.env_to_module.observation_preprocessor.MultiAgentObservationPreprocessor

```{eval-rst}
.. autoclass:: MultiAgentObservationPreprocessor

    .. automethod:: recompute_output_observation_space
    .. automethod:: preprocess
```
