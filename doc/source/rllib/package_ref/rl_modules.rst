.. include:: /_includes/rllib/we_are_hiring.rst

.. _rlmodule-reference-docs:

RLModule APIs
=============

.. include:: /_includes/rllib/new_api_stack.rst

RLModule specifications and configurations
-------------------------------------------

Single RLModuleSpec
+++++++++++++++++++

.. currentmodule:: ray.rllib.core.rl_module.rl_module

.. autosummary::
    :nosignatures:
    :toctree: doc/

    RLModuleSpec
    RLModuleSpec.build
    RLModuleSpec.module_class
    RLModuleSpec.observation_space
    RLModuleSpec.action_space
    RLModuleSpec.inference_only
    RLModuleSpec.learner_only
    RLModuleSpec.model_config

MultiRLModuleSpec
+++++++++++++++++

.. currentmodule:: ray.rllib.core.rl_module.multi_rl_module

.. autosummary::
    :nosignatures:
    :toctree: doc/

    MultiRLModuleSpec
    MultiRLModuleSpec.build

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


DefaultModelConfig
++++++++++++++++++

.. currentmodule:: ray.rllib.core.rl_module.default_model_config

.. autosummary::
    :nosignatures:
    :toctree: doc/

    DefaultModelConfig


RLModule API
------------

.. currentmodule:: ray.rllib.core.rl_module.rl_module

Construction and setup
++++++++++++++++++++++

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


Forward methods
+++++++++++++++

Use the following three forward methods when you use RLModule from inside other classes
and components. However, do NOT override them and leave them as-is in your custom subclasses.
For defining your own forward behavior, override the private methods ``_forward`` (generic forward behavior for
all phases) or, for more granularity, use ``_forward_exploration``, ``_forward_inference``, and ``_forward_train``.

.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~RLModule.forward_exploration
    ~RLModule.forward_inference
    ~RLModule.forward_train


Override these private methods to define your custom model's forward behavior.
- ``_forward``: generic forward behavior for all phases
- ``_forward_exploration``: for training sample collection
- ``_forward_inference``: for production deployments, greedy acting
- `_forward_train``: for computing loss function inputs

.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~RLModule._forward
    ~RLModule._forward_exploration
    ~RLModule._forward_inference
    ~RLModule._forward_train


Saving and restoring
++++++++++++++++++++

.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~RLModule.save_to_path
    ~RLModule.restore_from_path
    ~RLModule.from_checkpoint
    ~RLModule.get_state
    ~RLModule.set_state


MultiRLModule API
-----------------

.. currentmodule:: ray.rllib.core.rl_module.multi_rl_module

Constructor
+++++++++++

.. autosummary::
    :nosignatures:
    :toctree: doc/

    MultiRLModule
    MultiRLModule.setup
    MultiRLModule.as_multi_rl_module

Modifying the underlying RLModules
++++++++++++++++++++++++++++++++++

.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~MultiRLModule.add_module
    ~MultiRLModule.remove_module

Saving and restoring
++++++++++++++++++++

.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~MultiRLModule.save_to_path
    ~MultiRLModule.restore_from_path
    ~MultiRLModule.from_checkpoint
    ~MultiRLModule.get_state
    ~MultiRLModule.set_state


Additional RLModule APIs
------------------------

.. currentmodule:: ray.rllib.core.rl_module.apis

InferenceOnlyAPI
++++++++++++++++

.. autoclass:: ray.rllib.core.rl_module.apis.inference_only_api.InferenceOnlyAPI

    .. automethod:: get_non_inference_attributes

QNetAPI
+++++++

.. autoclass:: ray.rllib.core.rl_module.apis.q_net_api.QNetAPI

    .. automethod:: compute_q_values
    .. automethod:: compute_advantage_distribution

SelfSupervisedLossAPI
+++++++++++++++++++++

.. autoclass:: ray.rllib.core.rl_module.apis.self_supervised_loss_api.SelfSupervisedLossAPI

    .. automethod:: compute_self_supervised_loss

TargetNetworkAPI
++++++++++++++++

.. autoclass:: ray.rllib.core.rl_module.apis.target_network_api.TargetNetworkAPI

    .. automethod:: make_target_networks
    .. automethod:: get_target_network_pairs
    .. automethod:: forward_target

ValueFunctionAPI
++++++++++++++++

.. autoclass:: ray.rllib.core.rl_module.apis.value_function_api.ValueFunctionAPI

    .. automethod:: compute_values
