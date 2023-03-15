.. _policy-reference-docs:

Policies
========

The :py:class:`~ray.rllib.policy.policy.Policy` class contains functionality to compute
actions for decision making in an environment, as well as computing loss(es) and gradients,
updating a neural network model as well as postprocessing a collected environment trajectory.
One or more :py:class:`~ray.rllib.policy.policy.Policy` objects sit inside a
:py:class:`~ray.rllib.evaluation.RolloutWorker`'s :py:class:`~ray.rllib.policy.policy_map.PolicyMap` and
are - if more than one - are selected based on a multi-agent ``policy_mapping_fn``,
which maps agent IDs to a policy ID.

.. https://docs.google.com/drawings/d/1eFAVV1aU47xliR5XtGqzQcdvuYs2zlVj1Gb8Gg0gvnc/edit
.. figure:: ../images/policy_classes_overview.svg
    :align: left

    **RLlib's Policy class hierarchy:** Policies are deep-learning framework
    specific as they hold functionality to handle a computation graph (e.g. a
    TensorFlow 1.x graph in a session). You can define custom policy behavior
    by sub-classing either of the available, built-in classes, depending on your
    needs.


Policy base class
-----------------

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


Losses and trajectory processing
--------------------
.. autosummary::
    :toctree: doc/

    Policy.loss
    Policy.on_globar_var_update
    Policy.postprocess_trajectory


Saving and restoring
--------------------
.. autosummary::
    :toctree: doc/

    Policy.get_state
    Policy.set_state
    Policy.get_weights
    Policy.set_weights
    Policy.from_state
    Policy.from_checkpoint
    Policy.export_checkpoint
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


.. Policy API Reference
--------------------

..   policy/policy.rst
..   policy/tf_policies.rst
..   policy/torch_policy.rst
..   policy/custom_policies.rst

