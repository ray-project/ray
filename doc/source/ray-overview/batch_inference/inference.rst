.. _batch_inference:

Model Inferencing
=================

Model inferencing involves applying a :meth:`ds.map_batches() <ray.data.Dataset.map_batches>` to our transformed dataset with a pre-trained model as a UDF.

Stateful UDFs
-------------

One of the key value adds of Ray over other distributed systems is the support for distributed stateful operations. These stateful operations are especially useful for inferencing since the model only needs to be initialized once, instead of per batch.

First, define a callable class:

1. The ``__init__`` method  will load the model
2. The ``__call__`` method defines the logic for inferencing on a single batch.

.. code-block:: python

    from torchvision.models import resnet18

    class TorchModel:
        def __init__(self):
            self.model = resnet18(pretrained=True)
            self.model.eval()

        def __call__(self, batch: List[torch.Tensor]):
            torch_batch = torch.stack(batch)
            with torch.inference_mode():
                prediction = self.model(torch_batch)
                return {"class": prediction.argmax(dim=1).detach().numpy()}

Then, call :meth:`ds.map_batches() <ray.data.Dataset.map_batches>`, except making sure to specify a `ActorPoolStrategy <ray.data.compute.ActorPoolStrategy>` which defines how many workers to use for inferencing.

.. code-block:: python

    predictions = dataset.map_batches(
        TorchModel,
        compute=ray.data.ActorPoolStrategy(size=2),
    )

Using GPUs
----------

Do the following to use GPUs for inference:

1. Update the callable class implementation to move the model and data to and from Cuda device.

.. code-block:: diff

    from torchvision.models import resnet18

    class TorchModel:
        def __init__(self):
            self.model = resnet18(pretrained=True)
    +       self.model = self.model.cuda()
            self.model.eval()

        def __call__(self, batch: List[torch.Tensor]):
            torch_batch = torch.stack(batch)
    +       torch_batch = torch_batch.cuda()
            with torch.inference_mode():
                prediction = self.model(torch_batch)
    -           return {"class": prediction.argmax(dim=1).detach().numpy()}
    +           return {"class": prediction.argmax(dim=1).detach().cpu().numpy()}


2. Specify ``num_gpus=1`` in :meth:`ds.map_batches() <ray.data.Dataset.map_batches>` to indiciate that each inference worker should use 1 GPU.

.. code-block:: diff

    predictions = dataset.map_batches(
        TorchModel,
        compute=ray.data.ActorPoolStrategy(size=2),
    +   num_gpus=1
    )



Full Example: Resnet-18 prediction with PyTorch
-----------------------------------------------

.. literalinclude:: doc_code/torch_image_batch_pretrained.py
        :language: python


Configuration & Troubleshooting
-------------------------------

**How should I configure num_cpus and num_gpus for my model?**

By default, Ray will assign 1 CPU per task or actor. For example, on a machine with 16 CPUs, this will result in 16 tasks or actors running concurrently for inference. To change this, you can specify num_cpus=N, which will tell Ray to reserve more CPUs for the task or actor, or num_gpus=N, which will tell Ray to reserve/assign GPUs (GPUs will be assigned via CUDA_VISIBLE_DEVICES env var).

.. code-block:: python

    # Use 16 actors, each of which is assigned 1 GPU (16 GPUs total).
    ds = ds.map_batches(
        MyFn,
        compute=ActorPoolStrategy(size=16),
        num_gpus=1
    )

    # Use 16 actors, each of which is reserved 8 CPUs (128 CPUs total).
    ds = ds.map_batches(
        MyFn,
        compute=ActorPoolStrategy(size=16),
        num_cpus=8)


**How should I deal with OOM errors due to heavy model memory usage?**

It's common for models to consume a large amount of heap memory. For example, if a model uses 5GB of RAM when created / run, and a machine has 16GB of RAM total, then no more than three of these models can be run at the same time. The default resource assignments of 1 CPU per task/actor will likely lead to OutOfMemoryErrors from Ray in this situation.

Let's suppose our machine has 16GiB of RAM and 8 GPUs. To tell Ray to construct at most 3 of these actors per node, we can override the CPU or memory:

.. code-block:: python

    # Require 5 CPUs per actor (so at most 3 can fit per 16 CPU node).
    ds = ds.map_batches(MyFn,
    compute=ActorPoolStrategy(size=16), num_cpus=5)
