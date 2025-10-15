.. _train-local-mode:

Local Mode
==========

.. important::
    This user guide shows how to use local mode for Ray Train V2 only.
    **This user guide assumes that you're using Ray Train V2 with the environment variable ``RAY_TRAIN_V2_ENABLED=1`` enabled.**

Local mode in Ray Train allows you to run your training function directly in the local process.
This is useful for testing, development, and quick iterations.

When to use local mode
----------------------

Local mode is particularly useful in the following scenarios:

* **Isolated testing**: Test your training function without the Ray Train framework overhead, making it easier to locate issues in your training logic.
* **Unit testing**: Test your training logic quickly without the overhead of launching a Ray cluster.
* **Development**: Iterate rapidly on your training function before scaling to distributed training.
* **Multi-worker training with torchrun**: Launch multi-GPU training jobs using torchrun directly, making it easier to transfer from other frameworks.

.. note::
    In local mode, Ray Train doesn't launch Ray Train worker actors, but your code may still launch
    other Ray actors if needed. Local mode works with both single-process and multi-process training
    (through torchrun).

Enabling local mode
-------------------

To enable local mode, set ``num_workers=0`` in your :class:`~ray.train.ScalingConfig`:

.. code-block:: python

    from ray.train import ScalingConfig
    from ray.train.torch import TorchTrainer

    def train_func(config):
        # Your training logic
        pass

    trainer = TorchTrainer(
        train_loop_per_worker=train_func,
        scaling_config=ScalingConfig(num_workers=0),
    )
    result = trainer.fit()

This runs your training function in the main process without launching Ray Train worker actors.

Single-process local mode
-------------------------

The following example demonstrates single-process local mode with PyTorch:

.. code-block:: python

    import torch
    from torch import nn
    import ray
    from ray.train import ScalingConfig
    from ray.train.torch import TorchTrainer

    def train_func(config):
        model = nn.Linear(10, 1)
        optimizer = torch.optim.SGD(model.parameters(), lr=config["lr"])

        for epoch in range(config["epochs"]):
            # Training loop
            loss = model(torch.randn(32, 10)).sum()
            loss.backward()
            optimizer.step()

            # Report metrics
            ray.train.report({"loss": loss.item()})

    trainer = TorchTrainer(
        train_loop_per_worker=train_func,
        train_loop_config={"lr": 0.01, "epochs": 3},
        scaling_config=ScalingConfig(num_workers=0),
    )
    result = trainer.fit()
    print(f"Final loss: {result.metrics['loss']}")

.. note::
    Local mode works with all Ray Train framework integrations, including PyTorch Lightning,
    Hugging Face Transformers, LightGBM, XGBoost, TensorFlow, and others.

Multi-process local mode with torchrun
---------------------------------------

Local mode supports multi-GPU training through torchrun. This allows you to launch distributed training
across multiple GPUs or nodes while still using Ray but not the Ray Train framework, isolating your
training logic from the framework overhead.

Single-node multi-GPU training
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The following example shows how to use torchrun with local mode for multi-GPU training on a single node.

First, create your training script (``train_script.py``):

.. code-block:: python

    import os
    import tempfile
    import torch
    from torch import nn
    import ray
    from ray.train import Checkpoint, ScalingConfig
    from ray.train.torch import TorchTrainer

    def train_func(config):
        # Prepare model for distributed training
        model = nn.Linear(10, 1)
        model = ray.train.torch.prepare_model(model)

        optimizer = torch.optim.SGD(model.parameters(), lr=config["lr"])

        for epoch in range(config["epochs"]):
            # Training loop
            loss = model(torch.randn(32, 10)).sum()
            loss.backward()
            optimizer.step()

            # Report metrics and checkpoint
            with tempfile.TemporaryDirectory() as temp_dir:
                torch.save(model.state_dict(), os.path.join(temp_dir, "model.pt"))
                ray.train.report(
                    {"loss": loss.item()},
                    checkpoint=Checkpoint.from_directory(temp_dir)
                )

    # Configure trainer for local mode
    trainer = TorchTrainer(
        train_loop_per_worker=train_func,
        train_loop_config={"lr": 0.01, "epochs": 10},
        scaling_config=ScalingConfig(num_workers=0, use_gpu=True),
    )
    result = trainer.fit()

Then, launch the training with torchrun:

.. code-block:: bash

    # Train on 4 GPUs on a single node
    RAY_TRAIN_V2_ENABLED=1 torchrun --nproc-per-node=4 train_script.py

Ray Train automatically detects the torchrun environment variables and configures the distributed
training accordingly. You can access distributed training information through :func:`ray.train.get_context()`:

.. code-block:: python

    from ray.train import get_context

    context = get_context()
    print(f"World size: {context.get_world_size()}")
    print(f"World rank: {context.get_world_rank()}")
    print(f"Local rank: {context.get_local_rank()}")

Multi-node multi-GPU training
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can also use torchrun to launch multi-node training with local mode. The following example shows
how to launch training across 2 nodes with 4 GPUs each:

On the master node (``192.168.1.1``):

.. code-block:: bash

    RAY_TRAIN_V2_ENABLED=1 torchrun \
        --nnodes=2 \
        --nproc-per-node=4 \
        --node_rank=0 \
        --rdzv_backend=c10d \
        --rdzv_endpoint=192.168.1.1:29500 \
        --rdzv_id=job_id \
        train_script.py

On the worker node:

.. code-block:: bash

    RAY_TRAIN_V2_ENABLED=1 torchrun \
        --nnodes=2 \
        --nproc-per-node=4 \
        --node_rank=1 \
        --rdzv_backend=c10d \
        --rdzv_endpoint=192.168.1.1:29500 \
        --rdzv_id=job_id \
        train_script.py

.. note::
    When using torchrun with local mode, Ray Data isn't supported. The training relies on standard
    PyTorch data loading mechanisms.

Using local mode with Ray Data
-------------------------------

Single-process local mode works seamlessly with Ray Data for data loading and preprocessing.
The data is processed and provided to your training function without distributed workers.

The following example shows how to use Ray Data with single-process local mode:

.. code-block:: python

    import ray
    from ray.train import ScalingConfig
    from ray.train.torch import TorchTrainer

    def train_func(config):
        # Get the dataset shard
        train_dataset = ray.train.get_dataset_shard("train")

        # Iterate over batches
        for batch in train_dataset.iter_batches(batch_size=32):
            # Training logic
            pass

    # Create a Ray Dataset
    dataset = ray.data.read_csv("s3://bucket/data.csv")

    trainer = TorchTrainer(
        train_loop_per_worker=train_func,
        scaling_config=ScalingConfig(num_workers=0),
        datasets={"train": dataset},
    )
    result = trainer.fit()

.. note::
    Ray Data isn't supported when using torchrun with local mode for multi-process training.

Testing with local mode
-----------------------

Local mode is excellent for unit testing your training logic. The following example shows how to write
a unit test with local mode:

.. code-block:: python

    import unittest
    import ray
    from ray.train import ScalingConfig
    from ray.train.torch import TorchTrainer

    class TestTraining(unittest.TestCase):
        def test_training_runs(self):
            def train_func(config):
                # Minimal training logic
                ray.train.report({"loss": 0.5})

            trainer = TorchTrainer(
                train_loop_per_worker=train_func,
                scaling_config=ScalingConfig(num_workers=0),
            )
            result = trainer.fit()

            self.assertIsNone(result.error)
            self.assertEqual(result.metrics["loss"], 0.5)

    if __name__ == "__main__":
        unittest.main()

Limitations
-----------

Local mode has the following limitations:

* **No Ray Train fault tolerance**: Worker-level fault tolerance doesn't apply in local mode.
* **Ray Data limitations**: Ray Data isn't supported when using torchrun with local mode for multi-process training.
* **Different behavior**: Some framework-specific behaviors might differ slightly from Ray Train's distributed mode.

Transitioning from local mode to distributed training
-----------------------------------------------------

When you're ready to scale from local mode to distributed training, simply change ``num_workers``
to a value greater than 0:

.. code-block:: diff

     trainer = TorchTrainer(
         train_loop_per_worker=train_func,
         train_loop_config=config,
    -    scaling_config=ScalingConfig(num_workers=0),
    +    scaling_config=ScalingConfig(num_workers=4, use_gpu=True),
     )

Your training function code remains the same, and Ray Train handles the distributed coordination automatically.
