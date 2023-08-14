:orphan:

Fine-tuning DreamBooth with Ray Train
=====================================

This example shows how to do DreamBooth fine-tuning of a Stable Diffusion model using Ray AIR.
See the original `DreamBooth project homepage <https://dreambooth.github.io/>`_ for more details on what this fine-tuning method achieves.

.. image:: https://dreambooth.github.io/DreamBooth_files/high_level.png
  :target: https://dreambooth.github.io
  :alt: DreamBooth fine-tuning overview

This example is built on top of `this HuggingFace 🤗 tutorial <https://huggingface.co/docs/diffusers/training/dreambooth>`_.
See the HuggingFace tutorial for useful explanations and suggestions on hyperparameters.
**Adapting this example to Ray Train allows you to easily scale up the fine-tuning to an arbitrary number of distributed training workers.**

**Compute requirements:**

* Because of the large model sizes, you'll need a machine with at least 1 A10G GPU.
* Each training worker uses 1 GPU. You can use multiple GPUs/workers to leverage data-parallel training to speed up training time.

This example fine-tunes both the ``text_encoder`` and ``unet`` models used in the Stable Diffusion process, with respect to a prior preserving loss.


.. image:: /templates/05_dreambooth_finetuning/dreambooth/images/dreambooth_example.png
   :alt: DreamBooth overview

The full code repository can be found here: `https://github.com/ray-project/ray/tree/master/doc/source/templates/05_dreambooth_finetuning <https://github.com/ray-project/ray/tree/master/doc/source/templates/05_dreambooth_finetuning>`_


How it works
------------

This example leverages Ray Data for data loading and Ray Train for distributed training.

Data loading
^^^^^^^^^^^^

.. note::
    You can find the latest version of the code here: `dataset.py <https://github.com/ray-project/ray/tree/master/doc/source/templates/05_dreambooth_finetuning/dreambooth/dataset.py>`_

    The latest version might differ slightly from the code presented here.


We use Ray Data for data loading. The code has three interesting parts.

First, we load two datasets using :func:`ray.data.read_images`:

.. literalinclude:: /templates/05_dreambooth_finetuning/dreambooth/dataset.py
  :language: python
  :start-at: instance_dataset = read
  :end-at: class_dataset = read
  :dedent: 4

Then, we tokenize the prompt that generated these images:

.. literalinclude:: /templates/05_dreambooth_finetuning/dreambooth/dataset.py
  :language: python
  :start-at: tokenizer = AutoTokenizer
  :end-at: instance_prompt_ids = _tokenize
  :dedent: 4


And lastly, we apply a ``torchvision`` preprocessing pipeline to the images:

.. literalinclude:: /templates/05_dreambooth_finetuning/dreambooth/dataset.py
  :language: python
  :start-after: START: image preprocessing
  :end-before: END: image preprocessing
  :dedent: 4

We apply all of this in final step:


.. literalinclude:: /templates/05_dreambooth_finetuning/dreambooth/dataset.py
  :language: python
  :start-after: START: Apply preprocessing
  :end-before: END: Apply preprocessing
  :dedent: 4



Distributed training
^^^^^^^^^^^^^^^^^^^^


.. note::
    You can find the latest version of the code here: `train.py <https://github.com/ray-project/ray/tree/master/doc/source/templates/05_dreambooth_finetuning/dreambooth/train.py>`_

    The latest version might differ slightly from the code presented here.


The central part of the training code is the *training function*. This function accepts a configuration dict that contains the hyperparameters. It then defines a regular PyTorch training loop.

There are only a few locations where we interact with the Ray Train API. We marked them with in-line comments in the snippet below.

Remember that we want to do data-parallel training for all our models.


#. We load the data shard for each worker with session.get_dataset_shard("train")
#. We iterate over the dataset with train_dataset.iter_torch_batches()
#. We report results to Ray Train with session.report(results)

The code was compacted for brevity. The `full code <https://github.com/ray-project/ray/tree/master/doc/source/templates/05_dreambooth_finetuning/dreambooth/train.py>`_ is more thoroughly annotated.


.. literalinclude:: /templates/05_dreambooth_finetuning/dreambooth/train.py
  :language: python
  :start-at: def train_fn(config)
  :end-before: END: Training loop

We can then run this training loop with Ray AIR's TorchTrainer:


.. literalinclude:: /templates/05_dreambooth_finetuning/dreambooth/train.py
  :language: python
  :start-at: args = train_arguments
  :end-at: trainer.fit()
  :dedent: 4

Configuring the scale
^^^^^^^^^^^^^^^^^^^^^

In the TorchTrainer, we can easily configure our scale.
The above example uses the ``num_workers`` argument to specify the number
of workers. This defaults to 2 workers with 1 GPU each - so 2 GPUs in total.

To run the example on 4 GPUs, just set the number of workers to 4 using ``--num-workers=4``!
Or you can change the scaling config directly:

.. code-block:: diff

     scaling_config=ScalingConfig(
         use_gpu=True,
    -    num_workers=args.num_workers,
    +    num_workers=4,
     )

If you're running multi-node training, you should make sure that all nodes have access to a shared
storage (e.g. via NFS or EFS). In the example script below, you can adjust this location with the
``DATA_PREFIX`` environment variable.

Training throughput
~~~~~~~~~~~~~~~~~~~

We ran training using 2, and 4 workers/GPUs to compare throughput.

Setup:

* One g5.12xlarge nodes with 4 A10G GPUs
* Model as configured below
* Data from this example
* 200 regularization images
* Training for 4 epochs (800 steps)
* 3 runs per configuration

We expect that the training time should benefit from scale and decreases when running with
more workers and GPUs.

.. image:: /templates/05_dreambooth_finetuning/dreambooth/images/dreambooth_training.png
   :alt: DreamBooth training times

.. list-table::
   :header-rows: 1

   * - Number of workers/GPUs
     - Training time (seconds)
   * - 1
     - 802.14
   * - 2
     - 487.82
   * - 4
     - 313.25


While the training time decreases linearly with the amount of workers/GPUs, we observe some penalty.
Specifically, with double the amount of workers we don't get half of the training time.

This is most likely due to additional communication between processes and the transfer of large model
weights. We are also only training with a batch size of one because our GPU memory is limited. On larger
GPUs with higher batch sizes we would expect a greater benefit from scaling out.


Run the example
---------------

First, we download the pre-trained stable diffusion model as a starting point.

We will then train this model with a few images of our subject.

To achieve this, we choose a non-word as an identifier, e.g. ``unqtkn``. When fine-tuning the model with our subject, we will teach it that the prompt is ``A photo of a unqtkn <class>``.

After fine-tuning we can run inference with this specific prompt.
For instance: ``A photo of a unqtkn <class>`` will create an image of our subject.
Similarly, ``A photo of a unqtkn <class> at the beach`` will create an image of our subject at the beach.

Step 0: Preparation
^^^^^^^^^^^^^^^^^^^

Clone the Ray repository, go to the example directory, and install dependencies.

.. code-block:: bash

   git clone https://github.com/ray-project/ray.git
   cd doc/source/templates/05_dreambooth_finetuning
   pip install -Ur dreambooth/requirements.txt

Prepare some directories and environment variables.

.. literalinclude:: /templates/05_dreambooth_finetuning/dreambooth_run.sh
  :language: bash
  :start-after: __preparation_start__
  :end-before: __preparation_end__


Step 1: Download the pre-trained model
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Download and cache a pre-trained Stable-Diffusion model locally.

.. literalinclude:: /templates/05_dreambooth_finetuning/dreambooth_run.sh
  :language: bash
  :start-after: __cache_model_start__
  :end-before: __cache_model_end__

You can access the downloaded model checkpoint at the ``$ORIG_MODEL_PATH``.

Step 2: Supply images of your subject
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Use one of the sample datasets (dog, lego car), or provide your own directory
of images, and specify the directory with the ``$INSTANCE_DIR`` environment variable.

Then, we copy these images to ``$IMAGES_OWN_DIR``.

.. literalinclude:: /templates/05_dreambooth_finetuning/dreambooth_run.sh
  :language: bash
  :start-after: __supply_own_images_start__
  :end-before: __supply_own_images_end__

The ``$CLASS_NAME`` should be the general category of your subject.
The images produced by the prompt ``photo of a unqtkn <class>`` should be diverse images
that are different enough from the subject in order for generated images to clearly
show the effect of fine-tuning.

Step 3: Create the regularization images
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Create a regularization image set for a class of subjects using the pre-trained
Stable Diffusion model. This is used to regularize the fine-tuning by ensuring that
the model still produces decent images for random images of the same class,
rather than just optimize for producing good images of the subject.

.. literalinclude:: /templates/05_dreambooth_finetuning/dreambooth_run.sh
  :language: bash
  :start-after: Step 3: START
  :end-before: Step 3: END

We use Ray Data to do batch inference with 4 workers, so more images can be generated in parallel.

Step 4: Fine-tune the model
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Save a few (4 to 5) images of the subject being fine-tuned
in a local directory. Then launch the training job with:

.. literalinclude:: /templates/05_dreambooth_finetuning/dreambooth_run.sh
  :language: bash
  :start-after: Step 4: START
  :end-before: Step 4: END

Step 5: Generate images of our subject
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Try your model with the same command line as Step 2, but point
to your own model this time!

.. literalinclude:: /templates/05_dreambooth_finetuning/dreambooth_run.sh
  :language: bash
  :start-after: Step 5: START
  :end-before: Step 5: END

Next, try replacing the prompt with something more interesting!

For example, for the dog subject, you can try:

- "photo of a unqtkn dog in a bucket"
- "photo of a unqtkn dog sleeping"
- "photo of a unqtkn dog in a doghouse"