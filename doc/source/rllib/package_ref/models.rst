.. _model-reference-docs:

.. include:: /_includes/rllib/rlm_learner_migration_banner.rst

Model APIs
==========

.. currentmodule:: ray.rllib.models

Base Model classes
-------------------

.. autosummary::
   :toctree: doc/

    ~modelv2.ModelV2
    ~torch.torch_modelv2.TorchModelV2
    ~tf.tf_modelv2.TFModelV2

Feed Forward methods
---------------------
.. autosummary::
   :toctree: doc/

    ~modelv2.ModelV2.forward
    ~modelv2.ModelV2.value_function
    ~modelv2.ModelV2.last_output

Recurrent Models API
---------------------
.. autosummary::
   :toctree: doc/

    ~modelv2.ModelV2.get_initial_state
    ~modelv2.ModelV2.is_time_major

Acessing variables
---------------------
.. autosummary::
   :toctree: doc/

    ~modelv2.ModelV2.variables
    ~modelv2.ModelV2.trainable_variables

Customization
--------------
.. autosummary::
   :toctree: doc/

    ~modelv2.ModelV2.custom_loss
    ~modelv2.ModelV2.metrics
