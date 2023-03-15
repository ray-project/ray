.. _policy-base-class-reference-docs:


Base Policy class (ray.rllib.policy.policy.Policy)
==================================================

.. currentmodule:: ray.rllib.policy

Constructor
--------------------
.. autosummary::
   :toctree: doc/
   :template: autosummary/class_with_autosummary.rst

    Policy

Inference
--------------------
.. autosummary::
    :toctree: doc/
    
    Policy.compute_actions
    Policy.compute_actions_from_input_dict
    Policy.compute_single_action


Computing and applying gradients
--------------------
.. autosummary::
    :toctree: doc/

    Policy.compute_gradients
    Policy.apply_gradients


Updating the neural networks
--------------------
.. autosummary::
    :toctree: doc/

    Policy.learn_on_batch
    Policy.load_batch_into_buffer
    Policy.learn_on_loaded_batch
    Policy.learn_on_batch_from_replay_buffer
    Policy.get_num_samples_loaded_into_buffer


Loss and trajectory processing
--------------------
.. autosummary::
    :toctree: doc/

    Policy.loss
    Policy.on_global_var_update
    Policy.postprocess_trajectory


Saving and restoring
--------------------
.. autosummary::
    :toctree: doc/

    Policy.from_checkpoint
    Policy.export_checkpoint
    Policy.from_state
    Policy.get_weights
    Policy.set_weights
    Policy.get_state
    Policy.set_state
    Policy.import_model_from_h5

Connectors
--------------------
.. autosummary::
    :toctree: doc/

    Policy.reset_connectors
    Policy.restore_connectors


Recurrent Policies
--------------------
.. autosummary::
    :toctree: doc/

    Policy.get_initial_state
    Policy.num_state_tensors
    Policy.is_recurrent

Miscellaneous
--------------------
.. autosummary::
    :toctree: doc/

    Policy.apply
    Policy.get_session
    Policy.init_view_requirements
    Policy.make_rl_module


