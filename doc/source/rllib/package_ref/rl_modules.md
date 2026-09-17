---
myst:
  html_meta:
    description: "API reference for RLlib's RLModule and MultiRLModule APIs, covering specs, forward methods, checkpointing, and the additional RLModule APIs."
---

(rlmodule-reference-docs)=

# RLModule APIs

## RLModule specifications and configurations

### Single RLModuleSpec

```{eval-rst}
.. currentmodule:: ray.rllib.core.rl_module.rl_module
```

```{eval-rst}
.. autosummary::
    :nosignatures:
    :toctree: doc/

    RLModuleSpec
    RLModuleSpec.build
```

```{eval-rst}
.. autoattribute:: ray.rllib.core.rl_module.rl_module.RLModuleSpec.module_class
    :no-index:

.. autoattribute:: ray.rllib.core.rl_module.rl_module.RLModuleSpec.observation_space
    :no-index:

.. autoattribute:: ray.rllib.core.rl_module.rl_module.RLModuleSpec.action_space
    :no-index:

.. autoattribute:: ray.rllib.core.rl_module.rl_module.RLModuleSpec.inference_only
    :no-index:

.. autoattribute:: ray.rllib.core.rl_module.rl_module.RLModuleSpec.learner_only
    :no-index:

.. autoattribute:: ray.rllib.core.rl_module.rl_module.RLModuleSpec.model_config
    :no-index:
```

### MultiRLModuleSpec

```{eval-rst}
.. currentmodule:: ray.rllib.core.rl_module.multi_rl_module
```

```{eval-rst}
.. autosummary::
    :nosignatures:
    :toctree: doc/

    MultiRLModuleSpec
    MultiRLModuleSpec.build
```

```{eval-rst}
.. autoattribute:: ray.rllib.core.rl_module.multi_rl_module.MultiRLModuleSpec.multi_rl_module_class
    :no-index:

.. autoattribute:: ray.rllib.core.rl_module.multi_rl_module.MultiRLModuleSpec.observation_space
    :no-index:

.. autoattribute:: ray.rllib.core.rl_module.multi_rl_module.MultiRLModuleSpec.action_space
    :no-index:

.. autoattribute:: ray.rllib.core.rl_module.multi_rl_module.MultiRLModuleSpec.inference_only
    :no-index:

.. autoattribute:: ray.rllib.core.rl_module.multi_rl_module.MultiRLModuleSpec.model_config
    :no-index:

.. autoattribute:: ray.rllib.core.rl_module.multi_rl_module.MultiRLModuleSpec.rl_module_specs
    :no-index:
```

### DefaultModelConfig

```{eval-rst}
.. currentmodule:: ray.rllib.core.rl_module.default_model_config
```

```{eval-rst}
.. autosummary::
    :nosignatures:
    :toctree: doc/

    DefaultModelConfig
```

## RLModule API

```{eval-rst}
.. currentmodule:: ray.rllib.core.rl_module.rl_module
```

### Construction and setup

```{eval-rst}
.. autosummary::
    :nosignatures:
    :toctree: doc/

    RLModule
    RLModule.observation_space
    RLModule.action_space
    RLModule.inference_only
    RLModule.model_config
    RLModule.setup
    RLModule.as_multi_rl_module
```

### Forward methods

Use the following three forward methods when you use RLModule from inside other classes
and components. However, do NOT override them and leave them as-is in your custom subclasses.
For defining your own forward behavior, override the private methods `_forward` (generic forward behavior for
all phases) or, for more granularity, use `_forward_exploration`, `_forward_inference`, and `_forward_train`.

```{eval-rst}
.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~RLModule.forward_exploration
    ~RLModule.forward_inference
    ~RLModule.forward_train
```

Override these private methods to define your custom model's forward behavior.

- `_forward`: generic forward behavior for all phases
- `_forward_exploration`: for training sample collection
- `_forward_inference`: for production deployments, greedy acting
- `_forward_train`: for computing loss function inputs

```{eval-rst}
.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~RLModule._forward
    ~RLModule._forward_exploration
    ~RLModule._forward_inference
    ~RLModule._forward_train
```

### Saving and restoring

```{eval-rst}
.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~RLModule.save_to_path
    ~RLModule.restore_from_path
    ~RLModule.from_checkpoint
    ~RLModule.get_state
    ~RLModule.set_state
```

## MultiRLModule API

```{eval-rst}
.. currentmodule:: ray.rllib.core.rl_module.multi_rl_module
```

### Constructor

```{eval-rst}
.. autosummary::
    :nosignatures:
    :toctree: doc/

    MultiRLModule
    MultiRLModule.setup
    MultiRLModule.as_multi_rl_module
```

### Modifying the underlying RLModules

```{eval-rst}
.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~MultiRLModule.add_module
    ~MultiRLModule.remove_module
```

### Saving and restoring

```{eval-rst}
.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~MultiRLModule.save_to_path
    ~MultiRLModule.restore_from_path
    ~MultiRLModule.from_checkpoint
    ~MultiRLModule.get_state
    ~MultiRLModule.set_state
```

## Additional RLModule APIs

### InferenceOnlyAPI

```{eval-rst}
.. currentmodule:: ray.rllib.core.rl_module.apis.inference_only_api
```

```{eval-rst}
.. autoclass:: InferenceOnlyAPI

    .. automethod:: get_non_inference_attributes
```

### QNetAPI

```{eval-rst}
.. currentmodule:: ray.rllib.core.rl_module.apis.q_net_api
```

```{eval-rst}
.. autoclass:: QNetAPI

    .. automethod:: compute_q_values
    .. automethod:: compute_advantage_distribution
```

### SelfSupervisedLossAPI

```{eval-rst}
.. currentmodule:: ray.rllib.core.rl_module.apis.self_supervised_loss_api
```

```{eval-rst}
.. autoclass:: SelfSupervisedLossAPI

    .. automethod:: compute_self_supervised_loss
```

### TargetNetworkAPI

```{eval-rst}
.. currentmodule:: ray.rllib.core.rl_module.apis.target_network_api
```

```{eval-rst}
.. autoclass:: TargetNetworkAPI

    .. automethod:: make_target_networks
    .. automethod:: get_target_network_pairs
    .. automethod:: forward_target
```

### ValueFunctionAPI

```{eval-rst}
.. currentmodule:: ray.rllib.core.rl_module.apis.value_function_api
```

```{eval-rst}
.. autoclass:: ValueFunctionAPI

    .. automethod:: compute_values
```
