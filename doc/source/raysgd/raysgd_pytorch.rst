RaySGD Pytorch
==============

.. warning:: This is still an experimental API and is subject to change in the near future.

.. tip:: Help us make RaySGD better; take this 1 minute `User Survey <https://forms.gle/26EMwdahdgm7Lscy9>`_!

The RaySGD``PyTorchTrainer`` simplifies distributed model training for PyTorch. The ``PyTorchTrainer`` is a wrapper around ``torch.distributed.launch`` with a Python API to easily incorporate distributed training into a larger Python application, as opposed to needing to execute training outside of Python.

Under the hood, ``PytorchTrainer`` will create *replicas* of your model (controlled by ``num_replicas``) which are each managed by a Ray actor.

Setting up a model for training
-------------------------------

The ``PyTorchTrainer`` can be constructed with functions that construct components of the training script. Specifically, it needs constructors for the Model, Data, Optimizer, and Loss. This is so that we can create replicated copies of the neural network/optimizer across different devices and machines.

For example:

.. code-block::python

    def model_creator(config):
        """Constructor function for the model(s) to be optimized.

        Args:
            config (dict): Configuration dictionary passed into ``PyTorchTrainer``.

        Returns:
            One or more torch.nn.Module objects.
        """
        return nn.Linear(1, 1)


    def optimizer_creator(model, config):
        """Constructor of the optimizer.

        Args:
            model: The return values from ``model_creator``. This can be one
                or more torch neural networks.
            config (dict): Configuration dictionary passed into ``PyTorchTrainer``.

        Returns:
            One or more Torch optimizer objects. You must return as many optimizers
            as you have models.
        """
      return torch.optim.SGD(model.parameters(), lr=config.get("lr", 1e-4))


    def data_creator(config):
      """Returns training dataset, validation dataset."""
      return LinearDataset(2, 5), LinearDataset(2, 5, size=400)


    trainer1 = PyTorchTrainer(
        model_creator,
        data_creator,
        optimizer_creator,
        loss_creator=nn.MSELoss,
        config={"lr": 0.001})

Training APIs
-------------



.. code-block:: python

    trainer1.train()


Save and Load
-------------

If you want to save or reload the training procedure, you can use ``trainer.save``
and ``trainer.load``, which wraps the relevant ``torch.save`` and ``torch.load`` calls. This should work across a distributed cluster even without a NFS because it takes advantage of Ray's distributed object store.

.. code-block::

    trainer1 = PyTorchTrainer(
        model_creator,
        data_creator,
        optimizer_creator,
        loss_creator=nn.MSELoss,
        num_replicas=num_replicas)
    trainer1.train()

    checkpoint_path = os.path.join(tempfile.mkdtemp(), "checkpoint")
    trainer1.save(checkpoint_path)

    trainer2 = PyTorchTrainer(
        model_creator,
        data_creator,
        optimizer_creator,
        loss_creator=lambda config: nn.MSELoss(),
        num_replicas=num_replicas)
    trainer2.restore(checkpoint_path)



Exporting a model for inference
-------------------------------

The trained torch model can be extracted for use within the same Python program with ``trainer.get_model()``. This will load the state dictionary of the model(s).

.. code-block::

    trainer.train()
    model = trainer.get_model()

Shutting down training
----------------------

After training, you may want to reappropriate the Ray cluster. Release Ray resources with ``trainer.shutdown()``.

.. code-block::




Distributed Multi-node Training
-------------------------------


You can start a Ray cluster `via autoscaler <autoscaling.html>`_ or `manually <using-ray-on-a-cluster.html>`_.

.. code-block:: bash

    ray up CLUSTER.yaml
    python train.py --address="auto"


Advanced: Fault Tolerance
-------------------------

For distributed deep learning, jobs are often run on infrastructure where nodes can be pre-empted frequently (i.e., spot instances in the cloud). To overcome this, RaySGD provides **fault tolerance** features that enable training to continue regardless of node failures.

.. code-block:: bash

    trainer.train(max_retries=N)


During each ``train`` method, each parallel worker iterates through the dataset, synchronizing gradients and parameters at each batch. These synchronization primitives can hang when one or more of the parallel workers becomes unresponsive (i.e., when a node is lost). To address this, we've implemented the following protocol.

  1. If any worker node is lost, Ray will mark the training task as complete (``ray.wait`` will return).
  2. Ray will throw ``RayActorException`` when fetching the result for any worker, so the Trainer class will call ``ray.get`` on the "finished" training task.
  3. Upon catching this exception, the Trainer class will kill all of its workers.
  4. The Trainer will then detect the quantity of available resources (either CPUs or GPUs). It will then restart as many workers as it can, each resuming from the last checkpoint. Note that this may result in fewer workers than initially specified.
  5. If there are no available resources, the Trainer will apply an exponential backoff before retrying to create workers.
  6. If there are available resources and the Trainer has fewer workers than initially specified, then it will scale up its worker pool until it reaches the initially specified ``num_workers``.

Note that we assume the Trainer itself is not on a pre-emptible node. It is currently not possible to recover from a Trainer node failure.

Users can set ``checkpoint="auto"`` to always checkpoint the current model before executing a pass over the training dataset.

.. code-block:: bash

    trainer.train(max_retries=N, checkpoint="auto")



Advanced: Hyperparameter Tuning
-------------------------------

``PyTorchTrainer`` naturally integrates with Tune via the ``PyTorchTrainable`` interface. The same arguments to ``PyTorchTrainer`` should be passed into the ``tune.run(config=...)`` as shown below.

.. literalinclude:: ../../../python/ray/experimental/sgd/pytorch/examples/tune_example.py
   :language: python
   :start-after: __torch_tune_example__

