RaySGD API Reference
====================

PyTorch
-------

.. _ref-torch-trainer:

TorchTrainer
~~~~~~~~~~~~

.. autoclass:: ray.util.sgd.torch.TorchTrainer
    :members:

.. _ref-torch-operator:

PyTorch TrainingOperator
~~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: ray.util.sgd.torch.TrainingOperator
    :members:

.. _ref-creator-operator:

CreatorOperator
~~~~~~~~~~~~~~~~

.. autoclass:: ray.util.sgd.torch.training_operator.CreatorOperator
    :members:
    :exclude-members: setup

.. _ref-lightning-operator:

Pytorch Lightning LightningOperator
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: ray.util.sgd.torch.lightning_operator.LightningOperator
    :members:
    :exclude-members: setup, train_epoch, train_batch, validate, validate_batch, state_dict, load_state_dict

.. _BaseTorchTrainable-doc:

BaseTorchTrainable
~~~~~~~~~~~~~~~~~~

.. autoclass:: ray.util.sgd.torch.BaseTorchTrainable
    :members:
    :private-members:

Tensorflow
----------

TFTrainer
~~~~~~~~~

.. autoclass:: ray.util.sgd.tf.TFTrainer
    :members:

    .. automethod:: __init__

RaySGD Dataset
---------------

Dataset
~~~~~~~

.. autoclass:: ray.util.sgd.data.Dataset
    :members:

    .. automethod:: __init__

RaySGD Utils
-------------
.. _ref-utils:

Utils
~~~~~

.. autoclass:: ray.util.sgd.utils.AverageMeter
    :members:

.. autoclass:: ray.util.sgd.utils.AverageMeterCollection
    :members:



