Distributed Training (Experimental)
===================================

Ray's ``PyTorchTrainer`` simplifies distributed model training for PyTorch. The ``PyTorchTrainer`` is a wrapper around ``torch.distributed.launch`` with a Python API to easily incorporate distributed training into a larger Python application, as opposed to needing to execute training outside of Python.

----------

**With Ray**:

Wrap your training with this:

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



Then, start a Ray cluster `via autoscaler <autoscaling.html>`_ or `manually <using-ray-on-a-cluster.html>`_.

.. code-block:: bash

    ray up CLUSTER.yaml
    python train.py --address="localhost:<PORT>"


----------

**Before, with Pytorch**:

In your training program, insert the following:

.. code-block:: python

    torch.distributed.init_process_group(backend='YOUR BACKEND',
                                         init_method='env://')

    model = torch.nn.parallel.DistributedDataParallel(model,
                                                      device_ids=[arg.local_rank],
                                                      output_device=arg.local_rank)

Then, separately, on each machine:

.. code-block:: bash

    # Node 1: *(IP: 192.168.1.1, and has a free port: 1234)*
    $ python -m torch.distributed.launch --nproc_per_node=NUM_GPUS_YOU_HAVE
               --nnodes=4 --node_rank=0 --master_addr="192.168.1.1"
               --master_port=1234 YOUR_TRAINING_SCRIPT.py (--arg1 --arg2 --arg3
               and all other arguments of your training script)
    # Node 2:
    $ python -m torch.distributed.launch --nproc_per_node=NUM_GPUS_YOU_HAVE
               --nnodes=4 --node_rank=1 --master_addr="192.168.1.1"
               --master_port=1234 YOUR_TRAINING_SCRIPT.py (--arg1 --arg2 --arg3
               and all other arguments of your training script)
    # Node 3:
    $ python -m torch.distributed.launch --nproc_per_node=NUM_GPUS_YOU_HAVE
               --nnodes=4 --node_rank=2 --master_addr="192.168.1.1"
               --master_port=1234 YOUR_TRAINING_SCRIPT.py (--arg1 --arg2 --arg3
               and all other arguments of your training script)
    # Node 4:
    $ python -m torch.distributed.launch --nproc_per_node=NUM_GPUS_YOU_HAVE
               --nnodes=4 --node_rank=3 --master_addr="192.168.1.1"
               --master_port=1234 YOUR_TRAINING_SCRIPT.py (--arg1 --arg2 --arg3
               and all other arguments of your training script)


PyTorchTrainer Example
----------------------

Below is an example of using Ray's PyTorchTrainer. Under the hood, ``PytorchTrainer`` will create *replicas* of your model (controlled by ``num_replicas``) which are each managed by a worker.

.. literalinclude:: ../../python/ray/experimental/sgd/examples/train_example.py
   :language: python
   :start-after: __torch_train_example__


Hyperparameter Optimization on Distributed Pytorch
--------------------------------------------------

``PyTorchTrainer`` naturally integrates with Tune via the ``PyTorchTrainable`` interface. The same arguments to ``PyTorchTrainer`` should be passed into the ``tune.run(config=...)`` as shown below.

.. literalinclude:: ../../python/ray/experimental/sgd/examples/tune_example.py
   :language: python
   :start-after: __torch_tune_example__


Package Reference
-----------------

.. autoclass:: ray.experimental.sgd.pytorch.PyTorchTrainer
    :members:

    .. automethod:: __init__


.. autoclass:: ray.experimental.sgd.pytorch.PyTorchTrainable
    :members:
