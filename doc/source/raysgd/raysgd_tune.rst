.. _raysgd-tune:

RaySGD Hyperparameter Tuning
============================

.. warning:: This is an older version of Ray SGD. A newer, more light-weight version of Ray SGD (named Ray Train) is in alpha as of Ray 1.7.
         See the documentation :ref:`here <train-docs>`. To migrate from v1 to v2 you can follow the :ref:`migration guide <sgd-migration>`.

RaySGD integrates with :ref:`Ray Tune <tune-60-seconds>` to easily run distributed hyperparameter tuning experiments with your RaySGD Trainer.

PyTorch
-------

.. tip:: If you want to leverage multi-node data parallel training with PyTorch while using RayTune *without* using RaySGD, check out the :ref:`Tune PyTorch user guide <tune-pytorch-cifar-ref>` and Tune's lightweight :ref:`distributed pytorch integrations <tune-ddp-doc>`.

``TorchTrainer`` naturally integrates with Tune via the ``BaseTorchTrainable`` interface. Without changing any arguments, you can call ``TorchTrainer.as_trainable(...)`` to create a Tune-compatible class.
Then, you can simply pass the returned Trainable class to ``tune.run``. The ``config`` used for each ``Trainable`` in tune will automatically be passed down to the ``TorchTrainer``.
Therefore, each trial will have its own ``TorchTrainable`` that holds an instance of the ``TorchTrainer`` with its own unique hyperparameter configuration.
See the documentation (:ref:`BaseTorchTrainable-doc`) for more info.

.. literalinclude:: ../../../python/ray/util/sgd/torch/examples/tune_example.py
   :language: python
   :start-after: __torch_tune_example__
   :end-before: __end_torch_tune_example__

By default the training step for the returned ``Trainable`` will run one epoch of training and one epoch of validation, and will report
the combined result dictionaries to Tune.

By combining RaySGD with Tune, each individual trial will be run in a distributed fashion with ``num_workers`` workers,
but there can be multiple trials running in parallel as well.

Custom Training Step
~~~~~~~~~~~~~~~~~~~~
Sometimes it is necessary to provide a custom training step, for example if you want to run more than one epoch of training for
each tune iteration, or you need to manually update the scheduler after validation. Custom training steps can easily be provided by passing
in a ``override_tune_step`` function to ``TorchTrainer.as_trainable(...)``.

.. literalinclude:: ../../../python/ray/util/sgd/torch/examples/tune_example.py
   :language: python
   :start-after: __torch_tune_manual_lr_example__
   :end-before: __end_torch_tune_manual_lr_example__

Your custom step function should take in two arguments: an instance of the ``TorchTrainer`` and an ``info`` dict containing other potentially
necessary information.

The info dict contains the following values:

.. code-block:: python

    # The current Tune iteration.
    # This may be different than the number of epochs trained if each tune step does more than one epoch of training.
    iteration

If you would like any other information to be available in the ``info`` dict please file a feature request on `Github Issues <https://github.com/ray-project/ray/issues>`_!

You can see the `Tune example script <https://github.com/ray-project/ray/blob/master/python/ray/util/sgd/torch/examples/tune_example.py>`_ for an end-to-end example.
