---
myst:
  html_meta:
    description: "API reference for RLlib's offline RL APIs, covering offline data configuration, recording EnvRunners, OfflineData, and OfflinePreLearner."
---

(new-api-offline-reference-docs)=

# Offline RL API

## Configuring Offline RL

```{eval-rst}
.. currentmodule:: ray.rllib.algorithms.algorithm_config
```

```{eval-rst}
.. autosummary::
    :nosignatures:
    :toctree: doc/

    AlgorithmConfig.offline_data
    AlgorithmConfig.learners
```

## Configuring Offline Recording EnvRunners

```{eval-rst}
.. autosummary::
    :nosignatures:
    :toctree: doc/

    AlgorithmConfig.env_runners
```

## Constructing a Recording EnvRunner

```{eval-rst}
.. currentmodule:: ray.rllib.offline.offline_env_runner
```

```{eval-rst}
.. autosummary::
    :nosignatures:
    :toctree: doc/

    OfflineSingleAgentEnvRunner
```

## Constructing OfflineData

```{eval-rst}
.. currentmodule:: ray.rllib.offline.offline_data
```

```{eval-rst}
.. autosummary::
    :nosignatures:
    :toctree: doc/

    OfflineData
    OfflineData.__init__
```

## Sampling from Offline Data

```{eval-rst}
.. autosummary::
    :nosignatures:
    :toctree: doc/

    OfflineData.sample
    OfflineData.default_map_batches_kwargs
    OfflineData.default_iter_batches_kwargs
```

## Constructing an OfflinePreLearner

```{eval-rst}
.. currentmodule:: ray.rllib.offline.offline_prelearner
```

```{eval-rst}
.. autosummary::
    :nosignatures:
    :toctree: doc/

    OfflinePreLearner
```

## Transforming Data with an OfflinePreLearner

```{eval-rst}
.. autosummary::
    :nosignatures:
    :toctree: doc/

    SCHEMA
    OfflinePreLearner.__call__
    OfflinePreLearner._map_to_episodes
```
