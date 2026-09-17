---
myst:
  html_meta:
    description: "API reference for RLlib's AlgorithmConfig, covering constructor, builder, getter, and configuration methods for environments, training, learners, callbacks, and more."
---

(algorithm-config-reference-docs)=

# Algorithm Configuration API

```{eval-rst}
.. currentmodule:: ray.rllib.algorithms.algorithm_config
```

## Constructor

```{eval-rst}
.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~AlgorithmConfig
```

## Builder methods

```{eval-rst}
.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~AlgorithmConfig.build_algo
    ~AlgorithmConfig.build_learner_group
    ~AlgorithmConfig.build_learner
```

## Properties

```{eval-rst}
.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~AlgorithmConfig.is_multi_agent
    ~AlgorithmConfig.is_offline
    ~AlgorithmConfig.learner_class
    ~AlgorithmConfig.model_config
    ~AlgorithmConfig.rl_module_spec
    ~AlgorithmConfig.total_train_batch_size
```

## Getter methods

```{eval-rst}
.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~AlgorithmConfig.get_default_learner_class
    ~AlgorithmConfig.get_default_rl_module_spec
    ~AlgorithmConfig.get_evaluation_config_object
    ~AlgorithmConfig.get_multi_rl_module_spec
    ~AlgorithmConfig.get_multi_agent_setup
    ~AlgorithmConfig.get_rollout_fragment_length
```

## Public methods

```{eval-rst}
.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~AlgorithmConfig.copy
    ~AlgorithmConfig.validate
    ~AlgorithmConfig.freeze
```

(rllib-algorithm-config-methods)=

## Configuration methods

(rllib-config-env)=

### Configuring the RL Environment

```{eval-rst}
.. automethod:: ray.rllib.algorithms.algorithm_config.AlgorithmConfig.environment
    :noindex:
```

(rllib-config-training)=

### Configuring training behavior

```{eval-rst}
.. automethod:: ray.rllib.algorithms.algorithm_config.AlgorithmConfig.training
    :noindex:
```

(rllib-config-env-runners)=

### Configuring `EnvRunnerGroup` and `EnvRunner` actors

```{eval-rst}
.. automethod:: ray.rllib.algorithms.algorithm_config.AlgorithmConfig.env_runners
    :noindex:
```

(rllib-config-learners)=

### Configuring `LearnerGroup` and `Learner` actors

```{eval-rst}
.. automethod:: ray.rllib.algorithms.algorithm_config.AlgorithmConfig.learners
    :noindex:
```

(rllib-config-callbacks)=

### Configuring custom callbacks

```{eval-rst}
.. automethod:: ray.rllib.algorithms.algorithm_config.AlgorithmConfig.callbacks
    :noindex:
```

(rllib-config-multi_agent)=

### Configuring multi-agent specific settings

```{eval-rst}
.. automethod:: ray.rllib.algorithms.algorithm_config.AlgorithmConfig.multi_agent
    :noindex:
```

(rllib-config-offline_data)=

### Configuring offline RL specific settings

```{eval-rst}
.. automethod:: ray.rllib.algorithms.algorithm_config.AlgorithmConfig.offline_data
    :noindex:
```

(rllib-config-evaluation)=

### Configuring evaluation settings

```{eval-rst}
.. automethod:: ray.rllib.algorithms.algorithm_config.AlgorithmConfig.evaluation
    :noindex:
```

(rllib-config-framework)=

### Configuring deep learning framework settings

```{eval-rst}
.. automethod:: ray.rllib.algorithms.algorithm_config.AlgorithmConfig.framework
    :noindex:
```

(rllib-config-reporting)=

### Configuring reporting settings

```{eval-rst}
.. automethod:: ray.rllib.algorithms.algorithm_config.AlgorithmConfig.reporting
    :noindex:
```

(rllib-config-checkpointing)=

### Configuring checkpointing settings

```{eval-rst}
.. automethod:: ray.rllib.algorithms.algorithm_config.AlgorithmConfig.checkpointing
    :noindex:
```

(rllib-config-debugging)=

### Configuring debugging settings

```{eval-rst}
.. automethod:: ray.rllib.algorithms.algorithm_config.AlgorithmConfig.debugging
    :noindex:
```

(rllib-config-experimental)=

### Configuring experimental settings

```{eval-rst}
.. automethod:: ray.rllib.algorithms.algorithm_config.AlgorithmConfig.experimental
    :noindex:
```
