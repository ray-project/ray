.. include:: /_includes/rllib/we_are_hiring.rst

.. _learner-reference-docs:

LearnerGroup API
================

.. include:: /_includes/rllib/new_api_stack.rst

Configuring a LearnerGroup and Learner actors
---------------------------------------------

.. currentmodule:: ray.rllib.algorithms.algorithm_config

.. autosummary::
    :nosignatures:
    :toctree: doc/

    AlgorithmConfig.learners


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


Implementing a custom RLModule to fit a Learner
----------------------------------------------------

.. autosummary::
    :nosignatures:
    :toctree: doc/

    Learner.rl_module_required_apis
    Learner.rl_module_is_compatible


Performing updates
------------------

.. autosummary::
    :nosignatures:
    :toctree: doc/

    Learner.update
    Learner.before_gradient_based_update
    Learner.after_gradient_based_update


Computing losses
----------------

.. autosummary::
    :nosignatures:
    :toctree: doc/

    Learner.compute_losses
    Learner.compute_loss_for_module


Configuring optimizers
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


Gradient computation
--------------------

.. autosummary::
    :nosignatures:
    :toctree: doc/

    Learner.compute_gradients
    Learner.postprocess_gradients
    Learner.postprocess_gradients_for_module
    Learner.apply_gradients
    Learner._get_clip_function

Saving and restoring
--------------------

.. autosummary::
    :nosignatures:
    :toctree: doc/

    Learner.save_to_path
    Learner.restore_from_path
    Learner.from_checkpoint
    Learner.get_state
    Learner.set_state

Adding and removing modules
---------------------------

.. autosummary::
    :nosignatures:
    :toctree: doc/

    Learner.add_module
    Learner.remove_module
