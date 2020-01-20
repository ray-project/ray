RaySGD: Distributed Deep Learning
=================================

.. image:: raysgdlogo.png
    :scale: 20%
    :align: center

RaySGD is a lightweight library for distributed deep learning, providing thin wrappers around framework-native modules for data parallel training.

.. tip:: Help us make RaySGD better; take this 1 minute `User Survey <https://forms.gle/26EMwdahdgm7Lscy9>`_!

The main features are:

  - Ease of use: Scale Pytorch's native ``DistributedDataParallel`` and TensorFlow's ``tf.distribute.MirroredStrategy`` without needing to monitor individual nodes.
  - Composibility: RaySGD is built on top of the Ray Actor API, enabling seamless integration with existing Ray applications such as RLlib, Tune, and Ray.Serve.
  - Scale up and down: Start on single CPU. Scale up to multi-node, multi-gpu by changing 2 lines of code.

Getting Started
---------------

You can start a PyTorch Trainer with the following:

.. code-block:: python

    ray.init(args.address)

    trainer1 = PyTorchTrainer(
        model_creator,
        data_creator,
        optimizer_creator,
        loss_creator,
        num_replicas=<NUM_GPUS_YOU_HAVE> * <NUM_NODES>,
        use_gpu=True,
        batch_size=512,
        backend="nccl")

    stats = trainer1.train()
    print(stats)
    trainer1.shutdown()
    print("success!")


.. toctree::

   raysgd_pytorch.rst
   raysgd_tensorflow.rst
   raysgd_ft.rst
