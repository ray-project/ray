.. _learner-reference-docs:

Learner API
===========

Learner specifications and configurations
-----------------------------------------

.. currentmodule:: ray.rllib.core.learner.learner

.. autosummary::
    :nosignatures:
    :toctree: doc/

    FrameworkHyperparameters
    LearnerHyperparameters

TorchLearner configurations
+++++++++++++++++++++++++++

.. autosummary::
    :nosignatures:
    :toctree: doc/

    TorchCompileWhatToCompile

Constructor
-----------

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

    Learner.update
    Learner._update
    Learner.additional_update
    Learner.additional_update_for_module
    Learner._convert_batch_type


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

    Learner.save_state
    Learner.load_state
    Learner._save_optimizers
    Learner._load_optimizers
    Learner.get_state
    Learner.set_state
    Learner.get_optimizer_state
    Learner.set_optimizer_state
    Learner._get_metadata
    
Adding and Removing Modules
---------------------------

.. autosummary::
    :nosignatures:
    :toctree: doc/

    Learner.add_module
    Learner.remove_module

Managing Results
----------------

.. autosummary::
    :nosignatures:
    :toctree: doc/

    Learner.compile_results
    Learner.register_metric
    Learner.register_metrics
    Learner._check_result



LearnerGroup API
================

Configuring a LearnerGroup
--------------------------

.. autosummary::
    :nosignatures:
    :toctree: doc/

    LearnerSpec

.. currentmodule:: ray.rllib.core.learner.learner_group
    
.. autosummary::
    :nosignatures:
    :toctree: doc/

    LearnerGroup











