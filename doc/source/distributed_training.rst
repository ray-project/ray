Distributed Training (Experimental)
===================================


Ray includes abstractions for distributed model training that integrate with
deep learning frameworks, such as PyTorch.

Ray Train is built on top of the Ray task and actor abstractions to provide
seamless integration into existing Ray applications.

PyTorch Interface
-----------------

To use Ray Train with PyTorch, pass model and data creator functions to the
``ray.experimental.sgd.pytorch.PyTorchTrainer`` class.
To drive the distributed training, ``trainer.train()`` can be called
repeatedly.

.. code-block:: python

    model_creator = lambda config: YourPyTorchModel()
    data_creator = lambda config: YourTrainingSet(), YourValidationSet()

    trainer = PyTorchTrainer(
        model_creator,
        data_creator,
        optimizer_creator=utils.sgd_mse_optimizer,
        config={"lr": 1e-4},
        num_replicas=2,
        resources_per_replica=Resources(num_gpus=1),
        batch_size=16,
        backend="auto")

    for i in range(NUM_EPOCHS):
        trainer.train()

Under the hood, Ray Train will create *replicas* of your model
(controlled by ``num_replicas``) which are each managed by a worker.
Multiple devices (e.g. GPUs) can be managed by each replica (controlled by ``resources_per_replica``),
which allows training of lage models across multiple GPUs.
The ``PyTorchTrainer`` class coordinates the distributed computation and training to improve the model.

The full documentation for ``PyTorchTrainer`` is as follows:

.. autoclass:: ray.experimental.sgd.pytorch.PyTorchTrainer
    :members:

    .. automethod:: __init__
