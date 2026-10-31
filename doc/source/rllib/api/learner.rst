.. _learner-reference-docs:

LearnerGroup API
================

.. include:: /_includes/rllib/new_api_stack.rst

Configuring a LearnerGroup and Learner actors
---------------------------------------------

.. currentmodule:: ray.rllib.algorithms.algorithm_config

.. autosummary::
    :nosignatures:

    AlgorithmConfig.learners


Constructing a LearnerGroup
---------------------------

.. autosummary::
    :nosignatures:

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

    AlgorithmConfig.build_learner


.. currentmodule:: ray.rllib.core.learner.learner

.. autosummary::
    :nosignatures:
    :toctree: doc/

    Learner

.. autosummary::
    :nosignatures:

    Learner.build

.. autosummary::
    :nosignatures:
    :toctree: doc/

    Learner._make_module


Implementing a custom RLModule to fit a Learner
----------------------------------------------------

.. autosummary::
    :nosignatures:

    Learner.rl_module_required_apis
    Learner.rl_module_is_compatible


Performing updates
------------------

.. autosummary::
    :nosignatures:

    Learner.update
    Learner.before_gradient_based_update
    Learner.after_gradient_based_update


Computing losses
----------------

.. autosummary::
    :nosignatures:

    Learner.compute_losses
    Learner.compute_loss_for_module


Configuring optimizers
----------------------

.. autosummary::
    :nosignatures:

    Learner.configure_optimizers_for_module
    Learner.configure_optimizers
    Learner.register_optimizer
    Learner.get_optimizers_for_module
    Learner.get_optimizer
    Learner.get_parameters
    Learner.get_param_ref
    Learner.filter_param_dict_for_optimizer


Gradient computation
--------------------

.. autosummary::
    :nosignatures:

    Learner.compute_gradients
    Learner.postprocess_gradients
    Learner.postprocess_gradients_for_module
    Learner.apply_gradients

Saving and restoring
--------------------

.. autosummary::
    :nosignatures:

    Learner.save_to_path
    Learner.restore_from_path
    Learner.from_checkpoint

.. autosummary::
    :nosignatures:
    :toctree: doc/

    Learner.get_state
    Learner.set_state

Adding and removing modules
---------------------------

.. autosummary::
    :nosignatures:

    Learner.add_module
    Learner.remove_module
