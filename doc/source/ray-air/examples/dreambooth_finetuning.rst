Fine-tuning DreamBooth with Ray AIR
===================================

.. include:: ../../../../python/ray/air/examples/dreambooth/README.rst
  :start-after: section_intro
  :end-before: How it works

How it works
------------

This example leverages Ray Data for data loading and Ray Train for distributed training.

Data loading
^^^^^^^^^^^^

.. note::
    You can find the latest version of the code here: `dataset.py <https://github.com/ray-project/ray/blob/master/python/ray/air/examples/dreambooth/dataset.py>`_

    The latest version might differ slightly from the code presented here.


We use Ray Data for data loading. The code has three interesting parts.

First, we load two datasets using :func:`ray.data.read_images`:

.. literalinclude:: ../../../../python/ray/air/examples/dreambooth/dataset.py
  :language: python
  :start-at: instance_dataset = read
  :end-at: class_dataset = read
  :dedent: 4

Then, we tokenize the prompt that generated these images:

.. literalinclude:: ../../../../python/ray/air/examples/dreambooth/dataset.py
  :language: python
  :start-at: tokenizer = AutoTokenizer
  :end-at: instance_prompt_ids = _tokenize
  :dedent: 4


And lastly, we apply a ``torchvision`` preprocessing pipeline to the images:

.. literalinclude:: ../../../../python/ray/air/examples/dreambooth/dataset.py
  :language: python
  :start-at: transform = transforms.Compose
  :end-at: preprocessor = TorchVisionPreprocessor
  :dedent: 4

We apply all of this in final step:


.. literalinclude:: ../../../../python/ray/air/examples/dreambooth/dataset.py
  :language: python
  :start-at: instance_dataset = preprocessor
  :end-before: ---
  :dedent: 4



Distributed training
^^^^^^^^^^^^^^^^^^^^


.. note::
    You can find the latest version of the code here: `train.py <https://github.com/ray-project/ray/blob/master/python/ray/air/examples/dreambooth/train.py>`_

    The latest version might differ slightly from the code presented here.


The central part of the training code is the *training function*. This function accepts a configuration dict that contains the hyperparameters. It then defines a regular PyTorch training loop.

There are only a few locations where we interact with the Ray AIR API. We marked them with in-line comments in the snippet below.

Remember that we want to do data-parallel training for all our models.


#. We load the data shard for each worker with session.get_dataset_shard("train")
#. We iterate over the dataset with train_dataset.iter_torch_batches()
#. We report results to Ray AIR with session.report(results)

The code was compacted for brevity. The `full code <https://github.com/ray-project/ray/blob/master/python/ray/air/examples/dreambooth/train.py>`_ is more thoroughly annotated.


.. literalinclude:: ../../../../python/ray/air/examples/dreambooth/train.py
  :language: python
  :start-at: def train_fn(config)
  :end-at: session.report(results)

We can then run this training loop with Ray AIR's TorchTrainer:


.. literalinclude:: ../../../../python/ray/air/examples/dreambooth/train.py
  :language: python
  :start-at: args = train_arguments
  :end-at: trainer.fit()
  :dedent: 4

Configuring the scale
^^^^^^^^^^^^^^^^^^^^^

In the TorchTrainer, we can easily configure our scale. 
The above example uses the ``num_workers`` argument to specify the number
of workers. This defaults to 2 workers with 2 GPUs each - so 4 GPUs in total.

To run the example on 8 GPUs, just set the number of workers to 4 using ``--num-workers=4``!
Or you can change the scaling config directly:

.. code-block:: diff

     scaling_config=ScalingConfig(
         use_gpu=True,
    -    num_workers=args.num_workers,
    +    num_workers=4,
         resources_per_worker={
             "GPU": 2,
         },
     )

If you're running multi-node training, you should make sure that all nodes have access to a shared
storage (e.g. via NFS or EFS). In the example script below, you can adjust this location with the
``DATA_PREFIX`` environment variable.

Training throughput
~~~~~~~~~~~~~~~~~~~

We ran training using 1, 2, 4, and 8 workers (and 2, 4, 8, and 16 GPUs, respectively) to compare throughput.

Setup:

* 2 x g5.12xlarge nodes with 4 A10G GPUs each
* Model as configured below
* Data from this example
* 200 regularization images
* Training for 4 epochs (800 steps)
* Use a mounted External File System to share data between nodes
* 3 runs per configuration

Because network storage can be slow, we excluded the time it takes to save the final model from the training time.

We expect that the training time should benefit from scale and decreases when running with
more workers and GPUs.


.. image:: images/dreambooth_training.png
   :target: images/dreambooth_training.png
   :alt: DreamBooth training times


.. list-table::
   :header-rows: 1

   * - Number of workers
     - Number of GPUs
     - Training time
   * - 1
     - 2
     - 458.16 (3.82)
   * - 2
     - 4
     - 364.61 (1.65)
   * - 4
     - 8
     - 252.37 (3.18)
   * - 8
     - 16
     - 160.97 (1.36)


While the training time decreases linearly with the amount of workers/GPUs, we observe some penalty.
Specifically, with double the amount of workers we don't get half of the training time.

This is most likely due to additional communication between processes and the transfer of large model
weights. We are also only training with a batch size of one because our GPU memory is limited. On larger
GPUs with higher batch sizes we would expect a greater benefit from scaling out.


Run the example
---------------

.. include:: ../../../../python/ray/air/examples/dreambooth/README.rst
  :start-after: section_run_example


