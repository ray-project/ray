
.. include:: /_includes/rllib/we_are_hiring.rst

.. include:: /_includes/rllib/new_api_stack.rst

.. include:: /_includes/rllib/new_api_stack_component.rst

.. _learner-reference-docs:


LearnerGroup API
================

Configuring a LearnerGroup and Learner Workers
----------------------------------------------

.. currentmodule:: ray.rllib.algorithms.algorithm_config

.. autosummary::
    :nosignatures:
    :toctree: doc/

    AlgorithmConfig.resources
    AlgorithmConfig.rl_module
    AlgorithmConfig.training


Constructing a LearnerGroup
---------------------------

.. autosummary::
    :nosignatures:
    :toctree: doc/

    AlgorithmConfig.build_learner_group


.. currentmodule:: ray.rllib.core.learner.learner_group

.. autosummary::
    :nosignatures:
    :toctree: doc/

    LearnerGroup



Learner API
===========


Constructing a Learner
----------------------

.. currentmodule:: ray.rllib.algorithms.algorithm_config

.. autosummary::
    :nosignatures:
    :toctree: doc/

    AlgorithmConfig.build_learner


.. currentmodule:: ray.rllib.core.learner.learner

.. autosummary::
    :nosignatures:
    :toctree: doc/

    Learner
    Learner.build
    Learner._check_is_built
    Learner._make_module

Performing Updates
------------------

.. autosummary::
    :nosignatures:
    :toctree: doc/

    Learner.update_from_batch
    Learner.update_from_episodes
    Learner.before_gradient_based_update
    Learner._update
    Learner.after_gradient_based_update


Computing Losses
----------------

.. autosummary::
    :nosignatures:
    :toctree: doc/

    Learner.compute_loss
    Learner.compute_loss_for_module
    Learner._is_module_compatible_with_learner
    Learner._get_tensor_variable


Configuring Optimizers
----------------------

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
    Learner._check_registered_optimizer
    Learner._set_optimizer_lr
    Learner._get_clip_function


Gradient Computation
--------------------

.. autosummary::
    :nosignatures:
    :toctree: doc/

    Learner.compute_gradients
    Learner.postprocess_gradients
    Learner.postprocess_gradients_for_module
    Learner.apply_gradients

Saving, Loading, Checkpointing, and Restoring States
----------------------------------------------------

.. autosummary::
    :nosignatures:
    :toctree: doc/

    Learner.get_state
    Learner.set_state
    Learner.save_to_path
    Learner.restore_from_path
    Learner.from_checkpoint
    Learner._get_optimizer_state
    Learner._set_optimizer_state

Adding and Removing Modules
---------------------------

.. autosummary::
    :nosignatures:
    :toctree: doc/

    Learner.add_module
    Learner.remove_module
