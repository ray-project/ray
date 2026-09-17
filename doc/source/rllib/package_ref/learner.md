---
myst:
  html_meta:
    description: "API reference for RLlib's LearnerGroup and Learner APIs, covering construction, updates, loss computation, optimizers, gradients, and checkpointing."
---

(learner-reference-docs)=

# LearnerGroup API

## Configuring a LearnerGroup and Learner actors

```{eval-rst}
.. currentmodule:: ray.rllib.algorithms.algorithm_config
```

```{eval-rst}
.. autosummary::
    :nosignatures:
    :toctree: doc/

    AlgorithmConfig.learners
```

## Constructing a LearnerGroup

```{eval-rst}
.. autosummary::
    :nosignatures:
    :toctree: doc/

    AlgorithmConfig.build_learner_group
```

```{eval-rst}
.. currentmodule:: ray.rllib.core.learner.learner_group
```

```{eval-rst}
.. autosummary::
    :nosignatures:
    :toctree: doc/

    LearnerGroup
```

# Learner API

## Constructing a Learner

```{eval-rst}
.. currentmodule:: ray.rllib.algorithms.algorithm_config
```

```{eval-rst}
.. autosummary::
    :nosignatures:
    :toctree: doc/

    AlgorithmConfig.build_learner
```

```{eval-rst}
.. currentmodule:: ray.rllib.core.learner.learner
```

```{eval-rst}
.. autosummary::
    :nosignatures:
    :toctree: doc/

    Learner
    Learner.build
    Learner._make_module
```

## Implementing a custom RLModule to fit a Learner

```{eval-rst}
.. autosummary::
    :nosignatures:
    :toctree: doc/

    Learner.rl_module_required_apis
    Learner.rl_module_is_compatible
```

## Performing updates

```{eval-rst}
.. autosummary::
    :nosignatures:
    :toctree: doc/

    Learner.update
    Learner.before_gradient_based_update
    Learner.after_gradient_based_update
```

## Computing losses

```{eval-rst}
.. autosummary::
    :nosignatures:
    :toctree: doc/

    Learner.compute_losses
    Learner.compute_loss_for_module
```

## Configuring optimizers

```{eval-rst}
.. autosummary::
    :nosignatures:
    :toctree: doc/

    Learner.configure_optimizers_for_module
    Learner.configure_optimizers
    Learner.register_optimizer
    Learner.get_optimizers_for_module
    Learner.get_optimizer
    Learner.get_parameters
    Learner.get_param_ref
    Learner.filter_param_dict_for_optimizer
```

## Gradient computation

```{eval-rst}
.. autosummary::
    :nosignatures:
    :toctree: doc/

    Learner.compute_gradients
    Learner.postprocess_gradients
    Learner.postprocess_gradients_for_module
    Learner.apply_gradients
```

## Saving and restoring

```{eval-rst}
.. autosummary::
    :nosignatures:
    :toctree: doc/

    Learner.save_to_path
    Learner.restore_from_path
    Learner.from_checkpoint
    Learner.get_state
    Learner.set_state
```

## Adding and removing modules

```{eval-rst}
.. autosummary::
    :nosignatures:
    :toctree: doc/

    Learner.add_module
    Learner.remove_module
```
