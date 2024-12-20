
.. include:: /_includes/rllib/we_are_hiring.rst

.. include:: /_includes/rllib/new_api_stack.rst

.. _rlmodule-reference-docs:

RLModule APIs
=============


RL Module specifications and configurations
-------------------------------------------

Single RLModule
+++++++++++++++

.. currentmodule:: ray.rllib.core.rl_module.rl_module

.. autosummary::
    :nosignatures:
    :toctree: doc/

    RLModuleSpec
    RLModuleSpec.build

MultiRLModule
+++++++++++++

.. currentmodule:: ray.rllib.core.rl_module.multi_rl_module

.. autosummary::
    :nosignatures:
    :toctree: doc/

    MultiRLModuleSpec
    MultiRLModuleSpec.build

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

Constructor
+++++++++++

.. autosummary::
    :nosignatures:
    :toctree: doc/

    RLModule
    RLModule.setup
    RLModule.as_multi_rl_module


Forward methods
+++++++++++++++

.. autosummary::
    :nosignatures:
    :toctree: doc/


    ~RLModule.forward_train
    ~RLModule.forward_exploration
    ~RLModule.forward_inference
    ~RLModule._forward_train
    ~RLModule._forward_exploration
    ~RLModule._forward_inference


Saving and Loading
++++++++++++++++++++++

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

Saving and Restoring
++++++++++++++++++++

.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~MultiRLModule.save_to_path
    ~MultiRLModule.restore_from_path
    ~MultiRLModule.from_checkpoint
    ~MultiRLModule.get_state
    ~MultiRLModule.set_state
