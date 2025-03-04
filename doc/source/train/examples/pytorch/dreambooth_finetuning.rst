:orphan:

Fine-tune of Stable Diffusion with DreamBooth and Ray Train
===========================================================

.. raw:: html

    <a id="try-anyscale-quickstart-dreambooth_finetuning" target="_blank" href="https://www.anyscale.com/ray-on-anyscale?utm_source=ray_docs&utm_medium=docs&utm_campaign=dreambooth_finetuning">
      <img src="../../../_static/img/run-on-anyscale.svg" alt="Run on Anyscale" />
      <br/><br/>
    </a>

This is an intermediate example that shows how to do DreamBooth fine-tuning of a Stable Diffusion model using Ray Train.
It demonstrates how to use :ref:`Ray Data <data>` with PyTorch Lightning in Ray Train.


See the original `DreamBooth project homepage <https://dreambooth.github.io/>`_ for more details on what this fine-tuning method achieves.

.. image:: https://dreambooth.github.io/DreamBooth_files/high_level.png
  :target: https://dreambooth.github.io
  :alt: DreamBooth fine-tuning overview

This example builds on `this Hugging Face 🤗 tutorial <https://huggingface.co/docs/diffusers/training/dreambooth>`_.
See the Hugging Face tutorial for useful explanations and suggestions on hyperparameters.
**Adapting this example to Ray Train allows you to easily scale up the fine-tuning to an arbitrary number of distributed training workers.**

**Compute requirements:**

* Because of the large model sizes, you need a machine with at least 1 A10G GPU.
* Each training worker uses 1 GPU. You can use multiple GPUs or workers to leverage data-parallel training to speed up training time.

This example fine-tunes both the ``text_encoder`` and ``unet`` models used in the stable diffusion process, with respect to a prior preserving loss.


.. image:: /templates/05_dreambooth_finetuning/dreambooth/images/dreambooth_example.png
   :alt: DreamBooth overview

Find the full code repository at `https://github.com/ray-project/ray/tree/master/doc/source/templates/05_dreambooth_finetuning <https://github.com/ray-project/ray/tree/master/doc/source/templates/05_dreambooth_finetuning>`_


How it works
------------

This example uses Ray Data for data loading and Ray Train for distributed training.

Data loading
^^^^^^^^^^^^

.. note::
    Find the latest version of the code at `dataset.py <https://github.com/ray-project/ray/tree/master/doc/source/templates/05_dreambooth_finetuning/dreambooth/dataset.py>`_

    The latest version might differ slightly from the code presented here.


Use Ray Data for data loading. The code has three interesting parts.

First, load two datasets using :func:`ray.data.read_images`:

.. literalinclude:: /templates/05_dreambooth_finetuning/dreambooth/dataset.py
  :language: python
  :start-at: instance_dataset = read
  :end-at: class_dataset = read
  :dedent: 4

Then, tokenize the prompt that generated these images:

.. literalinclude:: /templates/05_dreambooth_finetuning/dreambooth/dataset.py
  :language: python
  :start-at: tokenizer = AutoTokenizer
  :end-at: instance_prompt_ids = _tokenize
  :dedent: 4


And lastly, apply a ``torchvision`` preprocessing pipeline to the images:

.. literalinclude:: /templates/05_dreambooth_finetuning/dreambooth/dataset.py
  :language: python
  :start-after: START: image preprocessing
  :end-before: END: image preprocessing
  :dedent: 4

Apply all three parts in a final step:

.. literalinclude:: /templates/05_dreambooth_finetuning/dreambooth/dataset.py
  :language: python
  :start-after: START: Apply preprocessing
  :end-before: END: Apply preprocessing
  :dedent: 4


Distributed training
^^^^^^^^^^^^^^^^^^^^


.. note::
    Find the latest version of the code at `train.py <https://github.com/ray-project/ray/tree/master/doc/source/templates/05_dreambooth_finetuning/dreambooth/train.py>`_

    The latest version might differ slightly from the code presented here.


The central part of the training code is the :ref:`training function <train-overview-training-function>`. This function accepts a configuration dict that contains the hyperparameters. It then defines a regular PyTorch training loop.

You interact with the Ray Train API in only a few locations, which follow in-line comments in the snippet below.

Remember that you want to do data-parallel training for all the models.


#. Load the data shard for each worker with `session.get_dataset_shard("train")``
#. Iterate over the dataset with `train_dataset.iter_torch_batches()``
#. Report results to Ray Train with `session.report(results)``

The code is compacted for brevity. The `full code <https://github.com/ray-project/ray/tree/master/doc/source/templates/05_dreambooth_finetuning/dreambooth/train.py>`_ is more thoroughly annotated.


.. literalinclude:: /templates/05_dreambooth_finetuning/dreambooth/train.py
  :language: python
  :start-at: def train_fn(config)
  :end-before: END: Training loop

You can then run this training function with Ray Train's TorchTrainer:


.. literalinclude:: /templates/05_dreambooth_finetuning/dreambooth/train.py
  :language: python
  :start-at: args = train_arguments
  :end-at: trainer.fit()
  :dedent: 4

Configure the scale
^^^^^^^^^^^^^^^^^^^

In the TorchTrainer, you can easily configure the scale.
The preceding example uses the ``num_workers`` argument to specify the number
of workers. This argument defaults to 2 workers with 1 GPU each, totalling to 2 GPUs.

To run the example on 4 GPUs, set the number of workers to 4 using ``--num-workers=4``.
Or you can change the scaling config directly:

.. code-block:: diff

     scaling_config=ScalingConfig(
         use_gpu=True,
    -    num_workers=args.num_workers,
    +    num_workers=4,
     )

If you're running multi-node training, make sure that all nodes have access to a shared
storage like NFS or EFS. In the following example script, you can adjust the location with the
``DATA_PREFIX`` environment variable.

Training throughput
~~~~~~~~~~~~~~~~~~~

Compare throughput of the preceding training runs that used 1,  2, and 4 workers or GPUs.

Consider the following setup:

* 1 GCE g2-standard-48-nvidia-l4-4 instance with 4 GPUs
* Model as configured below
* Data from this example
* 200 regularization images
* Training for 4 epochs (local batch size = 2)
* 3 runs per configuration

You expect that the training time should benefit from scale and decreases when running with
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


While the training time decreases linearly with the amount of workers/GPUs, you can observe some penalty.
Specifically, with double the amount of workers you don't get half of the training time.

This penalty is most likely due to additional communication between processes and the transfer of large model
weights. You are also only training with a batch size of one because of the GPU memory limitation. On larger
GPUs with higher batch sizes you would expect a greater benefit from scaling out.


Run the example
---------------

First, download the pre-trained Stable Diffusion model as a starting point.

Then train this model with a few images of a subject.

To achieve this, choose a non-word as an identifier, such as ``unqtkn``. When fine-tuning the model with this subject, you teach the model that the prompt is ``A photo of a unqtkn <class>``.

After fine-tuning you can run inference with this specific prompt.
For instance: ``A photo of a unqtkn <class>`` creates an image of the subject.
Similarly, ``A photo of a unqtkn <class> at the beach`` creates an image of the subject at the beach.

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

Download and cache a pre-trained Stable Diffusion model locally.

.. literalinclude:: /templates/05_dreambooth_finetuning/dreambooth_run.sh
  :language: bash
  :start-after: __cache_model_start__
  :end-before: __cache_model_end__

You can access the downloaded model checkpoint at the ``$ORIG_MODEL_PATH``.

Step 2: Supply images of your subject
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Use one of the sample datasets, like `dog` or `lego car`, or provide your own directory
of images, and specify the directory with the ``$INSTANCE_DIR`` environment variable.

Then, copy these images to ``$IMAGES_OWN_DIR``.

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
Stable Diffusion model. This regularization set ensures that
the model still produces decent images for random images of the same class,
rather than just optimize for producing good images of the subject.

.. literalinclude:: /templates/05_dreambooth_finetuning/dreambooth_run.sh
  :language: bash
  :start-after: Step 3: START
  :end-before: Step 3: END

Use Ray Data to do batch inference with 4 workers, to generate more images in parallel.

Step 4: Fine-tune the model
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Save a few, like 4 to 5, images of the subject being fine-tuned
in a local directory. Then launch the training job with:

.. literalinclude:: /templates/05_dreambooth_finetuning/dreambooth_run.sh
  :language: bash
  :start-after: Step 4: START
  :end-before: Step 4: END

Step 5: Generate images of the subject
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Try your model with the same command line as Step 2, but point
to your own model this time.

.. literalinclude:: /templates/05_dreambooth_finetuning/dreambooth_run.sh
  :language: bash
  :start-after: Step 5: START
  :end-before: Step 5: END

Next, try replacing the prompt with something more interesting.

For example, for the dog subject, you can try:

- "photo of a unqtkn dog in a bucket"
- "photo of a unqtkn dog sleeping"
- "photo of a unqtkn dog in a doghouse"

See also
--------

* :doc:`Ray Train Examples <../../examples>` for more use cases

* :ref:`Ray Train User Guides <train-user-guides>` for how-to guides
