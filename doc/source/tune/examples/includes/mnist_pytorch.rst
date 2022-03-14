:orphan:

MNIST PyTorch Example
~~~~~~~~~~~~~~~~~~~~~

.. literalinclude:: /../../python/ray/tune/examples/mnist_pytorch.py

.. TODO: test this code snippet below

.. _tune-torch-ddp:

Advanced: Distributed training with DistributedDataParallel
-----------------------------------------------------------

Some models require multiple nodes to train in a short amount of time.
Ray Tune allows you to easily do distributed data parallel training in addition to distributed hyperparameter tuning.

You can wrap your model in ``torch.nn.parallel.DistributedDataParallel`` to support distributed data parallel training:

.. code-block:: python

    from ray.util.sgd.torch import is_distributed_trainable
    from torch.nn.parallel import DistributedDataParallel

    def train_cifar(config, checkpoint_dir=None, data_dir=None):
        net = Net(config["l1"], config["l2"])

        device = "cpu"

        #### Using distributed data parallel training
        if is_distributed_trainable():
            net = DistributedDataParallel(net)

        if torch.cuda.is_available():
            device = "cuda"

        net.to(device)


If using checkpointing, be sure to use a :ref:`special checkpoint context manager <tune-ddp-doc>`,
``distributed_checkpoint_dir`` that avoids redundant checkpointing across multiple processes:

.. code-block:: python

    from ray.util.sgd.torch import distributed_checkpoint_dir

    #### Using distributed data parallel training
    # Inside `def train_cifar(...)`,
    # replace tune.checkpoint_dir() with the following
    # Avoids redundant checkpointing on different processes.
    with distributed_checkpoint_dir(step=epoch) as checkpoint_dir:
        path = os.path.join(checkpoint_dir, "checkpoint")
        torch.save((net.state_dict(), optimizer.state_dict()), path)


Finally, we need to tell Ray Tune to start multiple distributed processes at once by using
``ray.tune.integration.torch.DistributedTrainableCreator`` (:ref:`docs <tune-ddp-doc>`).
This is essentially equivalent to running ``torch.distributed.launch`` for each hyperparameter trial:

.. code-block:: python

    # You'll probably want to be running on a distributed Ray cluster.
    # ray.init(address="auto")

    from ray.util.sgd.integration.torch import DistributedTrainableCreator

    distributed_train_cifar = DistributedTrainableCreator(
      partial(train_cifar, data_dir=data_dir),
      use_gpu=True,
      num_workers=2,  # number of parallel workers to use
      num_cpus_per_worker=8
    )
    tune.run(
      distributed_train_cifar,
      resources_per_trial=None,
      config=config,
      num_samples=num_samples,
      ...
    )

See an :doc:`end-to-end example here </tune/examples/includes/ddp_mnist_torch>`.

If you consider switching to PyTorch Lightning to get rid of some of your boilerplate
training code, please know that we also have a walkthrough on :doc:`how to use Tune with
PyTorch Lightning models </tune/examples/tune-pytorch-lightning>`.