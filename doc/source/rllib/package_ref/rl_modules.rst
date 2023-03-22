

.. _rlmodule-reference-docs:

RLModule API
============


RL Module specifications and configuraitons
-------------------------------------------

Single Agent
++++++++++++

.. currentmodule:: ray.rllib.core.rl_module.rl_module

.. autosummary::
    :toctree: doc/
    :template: autosummary/class_with_autosummary.rst

    SingleAgentRLModuleSpec
    SingleAgentRLModuleSpec.build
    SingleAgentRLModuleSpec.get_rl_module_config

RLModule Configuration
+++++++++++++++++++++++

.. autosummary::
    :toctree: doc/
    :template: autosummary/class_with_autosummary.rst

    RLModuleConfig
    RLModuleConfig.to_dict
    RLModuleConfig.from_dict
    RLModuleConfig.get_catalog

Multi Agent
++++++++++++

.. currentmodule:: ray.rllib.core.rl_module.marl_module

.. autosummary::
    :toctree: doc/
    :template: autosummary/class_with_autosummary.rst

    MultiAgentRLModuleSpec
    MultiAgentRLModuleSpec.build
    MultiAgentRLModuleSpec.get_marl_config



RL Module API
-------------

.. currentmodule:: ray.rllib.core.rl_module.rl_module


Constructor
+++++++++++

.. autosummary::
    :toctree: doc/
    :template: autosummary/class_with_autosummary.rst

    RLModule
    RLModule.as_multi_agent


Forward methods
+++++++++++++++

.. autosummary::
    :toctree: doc/


    ~RLModule.forward_train
    ~RLModule.forward_exploration
    ~RLModule.forward_inference

IO specifications
+++++++++++++++++

.. autosummary::
    :toctree: doc/

    ~RLModule.input_specs_inference
    ~RLModule.input_specs_exploration
    ~RLModule.input_specs_train
    ~RLModule.output_specs_inference
    ~RLModule.output_specs_exploration
    ~RLModule.output_specs_train



Saving and Loading
++++++++++++++++++++++

.. autosummary::
    :toctree: doc/

    ~RLModule.get_state
    ~RLModule.set_state
    ~RLModule.save_state_to_file
    ~RLModule.load_state_from_file
    ~RLModule.save_to_checkpoint
    ~RLModule.from_checkpoint


Multi Agent RL Module API
-------------------------

.. currentmodule:: ray.rllib.core.rl_module.marl_module

Constructor
+++++++++++

.. autosummary::
    :toctree: doc/
    :template: autosummary/class_with_autosummary.rst

    MultiAgentRLModule
    MultiAgentRLModule.build
    MultiAgentRLModule.as_multi_agent

Modifying the underlying RL modules
++++++++++++++++++++++++++++++++++++

.. autosummary::
    :toctree: doc/

    ~MultiAgentRLModule.add_module
    ~MultiAgentRLModule.remove_module

Saving and Loading
++++++++++++++++++++++

.. autosummary::
    :toctree: doc/

    ~MultiAgentRLModule.load_state_from_dir
    ~MultiAgentRLModule.save_state_to_dir
