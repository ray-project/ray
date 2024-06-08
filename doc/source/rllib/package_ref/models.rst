.. include:: /_includes/rllib/we_are_hiring.rst

.. include:: /_includes/rllib/new_api_stack.rst


.. _model-reference-docs:

Model APIs
==========

.. currentmodule:: ray.rllib.models

Base Model classes
-------------------

.. autosummary::
   :nosignatures:
   :toctree: doc/

    ~modelv2.ModelV2
    ~torch.torch_modelv2.TorchModelV2
    ~tf.tf_modelv2.TFModelV2

Feed Forward methods
---------------------
.. autosummary::
   :nosignatures:
   :toctree: doc/

    ~modelv2.ModelV2.forward
    ~modelv2.ModelV2.value_function
    ~modelv2.ModelV2.last_output

Recurrent Models API
---------------------
.. autosummary::
   :nosignatures:
   :toctree: doc/

    ~modelv2.ModelV2.get_initial_state
    ~modelv2.ModelV2.is_time_major

Acessing variables
---------------------
.. autosummary::
   :nosignatures:
   :toctree: doc/

    ~modelv2.ModelV2.variables
    ~modelv2.ModelV2.trainable_variables
    ~distributions.Distribution

Customization
--------------
.. autosummary::
   :nosignatures:
   :toctree: doc/

    ~modelv2.ModelV2.custom_loss
    ~modelv2.ModelV2.metrics
