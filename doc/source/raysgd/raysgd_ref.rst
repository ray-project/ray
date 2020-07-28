RaySGD API Documentation
========================

.. _ref-torch-trainer:

TorchTrainer
------------

.. autoclass:: ray.util.sgd.torch.TorchTrainer
    :members:

.. _ref-torch-operator:

PyTorch TrainingOperator
------------------------

.. autoclass:: ray.util.sgd.torch.TrainingOperator
    :members:

.. _BaseTorchTrainable-doc:

BaseTorchTrainable
------------------

.. autoclass:: ray.util.sgd.torch.BaseTorchTrainable
    :members:
    :private-members:


.. _tune-sgd-ddp-doc:

Ray Tune + RaySGD
-----------------

If not using the TorchTrainer API, Ray also offers lightweight integrations to distribute your model training on Ray Tune.


.. autofunction:: ray.util.sgd.torch.DistributedTrainableCreator

.. autofunction:: ray.util.sgd.torch.distributed_checkpoint_dir

.. autofunction:: ray.util.sgd.torch.is_distributed_trainable

TFTrainer
---------

.. autoclass:: ray.util.sgd.tf.TFTrainer
    :members:

    .. automethod:: __init__

Dataset
-------

.. autoclass:: ray.util.sgd.data.Dataset
    :members:

    .. automethod:: __init__

