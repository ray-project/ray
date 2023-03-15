.. _policy-reference-docs:

Policy API
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


..   policy/policy.rst
   policy/tf_policy.rst
   policy/torch_policy.rst
   policy/custom_policy.rst

.. currentmodule:: ray.rllib

Base Policy classes
-------------------

.. autosummary::
   :toctree: doc/
   :template: autosummary/class_with_autosummary.rst

    ~policy.policy.Policy
    ~policy.eager_tf_policy_v2.EagerTFPolicyV2
    ~policy.torch_policy_v2.TorchPolicyV2

.. --------------------------------------------

Inference
--------------------

Base Policy
~~~~~~~~~~~~~~~~~~~~
.. autosummary::
    :toctree: doc/
    
    ~policy.policy.Policy.compute_actions
    ~policy.policy.Policy.compute_actions_from_input_dict
    ~policy.policy.Policy.compute_single_action

Torch Policy
~~~~~~~~~~~~~~~~~~~~
.. autosummary::
    :toctree: doc/
    
    ~policy.torch_policy_v2.TorchPolicyV2.action_sampler_fn
    ~policy.torch_policy_v2.TorchPolicyV2.action_distribution_fn

Tf Policy
~~~~~~~~~~~~~~~~~~~~
.. autosummary::
    :toctree: doc/
    
    ~policy.eager_tf_policy_v2.EagerTFPolicyV2.action_sampler_fn
    ~policy.eager_tf_policy_v2.EagerTFPolicyV2.action_distribution_fn
    ~policy.eager_tf_policy_v2.EagerTFPolicyV2.extra_action_out_fn

.. --------------------------------------------

Computing and applying gradients
--------------------

Base Policy
~~~~~~~~~~~~~~~~~~~~
.. autosummary::
    :toctree: doc/

    ~Policy.compute_gradients
    ~Policy.apply_gradients

Torch Policy
~~~~~~~~~~~~~~~~~~~~
.. autosummary::
    :toctree: doc/


Tf Policy
~~~~~~~~~~~~~~~~~~~~
.. autosummary::
    :toctree: doc/

    ~policy.torch_policy_v2.TorchPolicyV2.grad_stats_fn
    ~policy.eager_tf_policy_v2.EagerTFPolicyV2.compute_gradients_fn
    ~policy.eager_tf_policy_v2.EagerTFPolicyV2.apply_gradients_fn
    ~policy.eager_tf_policy_v2.EagerTFPolicyV2.extra_learn_fetches_fn

    

.. --------------------------------------------

Updating the neural networks
--------------------


Base Policy
~~~~~~~~~~~~~~~~~~~~
.. autosummary::
    :toctree: doc/
    
    Policy.learn_on_batch
    Policy.load_batch_into_buffer
    Policy.learn_on_loaded_batch
    Policy.learn_on_batch_from_replay_buffer
    Policy.get_num_samples_loaded_into_buffer

Torch Policy
~~~~~~~~~~~~~~~~~~~~
.. autosummary::
    :toctree: doc/


Tf Policy
~~~~~~~~~~~~~~~~~~~~
.. autosummary::
    :toctree: doc/


.. --------------------------------------------

Logging and metrics
--------------------


Base Policy
~~~~~~~~~~~~~~~~~~~~
.. autosummary::
    :toctree: doc/



Torch Policy
~~~~~~~~~~~~~~~~~~~~
.. autosummary::
    :toctree: doc/


Tf Policy
~~~~~~~~~~~~~~~~~~~~
.. autosummary::
    :toctree: doc/

    ~policy.eager_tf_policy_v2.EagerTFPolicyV2.stats_fn


.. --------------------------------------------

Loss and trajectory processing
--------------------

Base Policy
~~~~~~~~~~~~~~~~~~~~
.. autosummary::
    :toctree: doc/

    Policy.loss
    Policy.on_global_var_update
    Policy.postprocess_trajectory



Torch Policy
~~~~~~~~~~~~~~~~~~~~
.. autosummary::
    :toctree: doc/


Tf Policy
~~~~~~~~~~~~~~~~~~~~
.. autosummary::
    :toctree: doc/


.. --------------------------------------------

Saving and restoring
--------------------

Base Policy
~~~~~~~~~~~~~~~~~~~~
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


Torch Policy
~~~~~~~~~~~~~~~~~~~~
.. autosummary::
    :toctree: doc/


Tf Policy
~~~~~~~~~~~~~~~~~~~~
.. autosummary::
    :toctree: doc/


.. --------------------------------------------

Connectors
--------------------

Base Policy
~~~~~~~~~~~~~~~~~~~~
.. autosummary::
    :toctree: doc/

    ~policy.Policy.reset_connectors
    ~policy.Policy.restore_connectors
    ~policy.Policy.get_connector_metrics


Torch Policy
~~~~~~~~~~~~~~~~~~~~
.. autosummary::
    :toctree: doc/


Tf Policy
~~~~~~~~~~~~~~~~~~~~
.. autosummary::
    :toctree: doc/


.. --------------------------------------------

Recurrent Policies
--------------------

Base Policy
~~~~~~~~~~~~~~~~~~~~
.. autosummary::
    :toctree: doc/

    Policy.get_initial_state
    Policy.num_state_tensors
    Policy.is_recurrent


Torch Policy
~~~~~~~~~~~~~~~~~~~~
.. autosummary::
    :toctree: doc/


Tf Policy
~~~~~~~~~~~~~~~~~~~~
.. autosummary::
    :toctree: doc/


.. --------------------------------------------

Miscellaneous
--------------------

Base Policy
~~~~~~~~~~~~~~~~~~~~
.. autosummary::
    :toctree: doc/

    ~policy.Policy.apply
    ~policy.Policy.get_session
    ~policy.Policy.init_view_requirements
    ~policy.Policy.get_host


Torch Policy
~~~~~~~~~~~~~~~~~~~~
.. autosummary::
    :toctree: doc/


Tf Policy
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. autosummary::
    :toctree: doc/

    ~policy.eager_tf_policy_v2.EagerTFPolicyV2.variables


.. --------------------------------------------

Making models
--------------------

Base Policy
~~~~~~~~~~~~~~~~~~~~
.. autosummary::
    :toctree: doc/

    ~policy.Policy.make_rl_module


Torch Policy
~~~~~~~~~~~~~~~~~~~~
.. autosummary::
    :toctree: doc/

    ~policy.torch_policy_v2.TorchPolicyV2.make_model


Tf Policy
~~~~~~~~~~~~~~~~~~~~
.. autosummary::
    :toctree: doc/

    ~policy.eager_tf_policy_v2.EagerTFPolicyV2.make_model