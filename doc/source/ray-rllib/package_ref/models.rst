.. _model-reference-docs:

Model APIs
==========

ModelV2 API (rllib.env.models.modelv2.ModelV2)
++++++++++++++++++++++++++++++++++++++++++++++

All RLlib neural network models have to be provided as ModelV2 sub-classes.

.. autoclass:: ray.rllib.models.modelv2.ModelV2
    :members:


RLlib comes with two sub-classes for TF (keras) models and PyTorch models:

TFModelV2 (rllib.env.models.tf.tf_modelv2.TFModelV2)
++++++++++++++++++++++++++++++++++++++++++++++++++++

.. autoclass:: ray.rllib.models.tf.tf_modelv2.TFModelV2
    :members:

TorchModelV2 (rllib.env.models.torch.torch_modelv2.TorchModelV2)
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

.. autoclass:: ray.rllib.models.torch.torch_modelv2.TorchModelV2
    :members:
