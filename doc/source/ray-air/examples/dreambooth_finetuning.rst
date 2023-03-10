..
    ATTN: This should be kept in sync with release/air_examples/dreambooth/dreambooth_run.sh
    ATTN: This should be kept in sync with python/ray/air/examples/dreambooth/README.md


Fine-tuning DreamBooth with Ray AIR
===================================

This example shows how to fine-tune a DreamBooth model using Ray AIR.

Because of the large model sizes, you'll need 2 A10G GPUs per worker.

The example can leverage data-parallel training to speed up training time. Of course, this will
require more GPUs.

The demo tunes both the text_encoder and unet parts of Stable Diffusion, and utilizes the prior preserving loss function.


.. image:: images/dreambooth_example.png
   :target: images/dreambooth_example.png
   :alt: DreamBooth example


The full code repository can be found here:
`https://github.com/ray-project/ray/blob/master/python/ray/air/examples/dreambooth/ <https://github.com/ray-project/ray/blob/master/python/ray/air/examples/dreambooth/>`_

How it works
------------

This example leverages Ray Data for data loading and Ray Train for distributed training.

Data loading
^^^^^^^^^^^^

Link to full code: `dataset.py <https://github.com/ray-project/ray/blob/master/python/ray/air/examples/dreambooth/dataset.py>`_

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

Link to full code: `train.py <https://github.com/ray-project/ray/blob/master/python/ray/air/examples/dreambooth/train.py>`_

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
The above example runs training on 2 workers with 2 GPUs each - i.e. on 4 GPUs. 

To run the example on 8 GPUs, just set the number of workers to 4!

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

First, we download the pre-trained stable diffusion model as a starting point.

We will then train this model with a few images of our subject.

To achieve this, we choose a non-word as an identifier, e.g. ``unqtkn``. When fine-tuning the model with our subject, we will teach it that the prompt is ``A photo of a unqtkn <class>``.

After fine-tuning we can run inference with this specific prompt. For instance: ``A photo of a unqtkn <class>`` will create an image of our subject.

Step 0: Preparation
^^^^^^^^^^^^^^^^^^^

Clone the Ray repository, go to the example directory, and install dependencies.

.. code-block:: bash

   git clone https://github.com/ray-project/ray.git
   cd ray/python/ray/air/examples/dreambooth
   pip install -Ur requirements.txt

Prepare some directories and environment variables.

.. code-block:: bash

   export DATA_PREFIX="./"
   export ORIG_MODEL_NAME="CompVis/stable-diffusion-v1-4"
   export ORIG_MODEL_HASH="249dd2d739844dea6a0bc7fc27b3c1d014720b28"
   export ORIG_MODEL_DIR="$DATA_PREFIX/model-orig"
   export ORIG_MODEL_PATH="$ORIG_MODEL_DIR/models--${ORIG_MODEL_NAME/\//--}/snapshots/$ORIG_MODEL_HASH"
   export TUNED_MODEL_DIR="$DATA_PREFIX/model-tuned"
   export IMAGES_REG_DIR="$DATA_PREFIX/images-reg"
   export IMAGES_OWN_DIR="$DATA_PREFIX/images-own"
   export IMAGES_NEW_DIR="$DATA_PREFIX/images-new"

   export CLASS_NAME="toy car"

   mkdir -p $ORIG_MODEL_DIR $TUNED_MODEL_DIR $IMAGES_REG_DIR $IMAGES_OWN_DIR $IMAGES_NEW_DIR

Copy some images for fine-tuning into ``$IMAGES_OWN_DIR``.

Step 1: Download the pre-trained model
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Download and cache a pre-trained Stable-Diffusion model locally.
Default model and version are ``CompVis/stable-diffusion-v1-4``
at git hash ``3857c45b7d4e78b3ba0f39d4d7f50a2a05aa23d4``.

.. code-block::

   python cache_model.py --model_dir=$ORIG_MODEL_DIR --model_name=$ORIG_MODEL_NAME --revision=$ORIG_MODEL_HASH

Note that actual model files will be downloaded into
``\<model_dir>\snapshots\<git_hash>\`` directory.

Step 2: Create the regularization images
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Create a regularization image set for a class of subjects:

.. code-block::

   python run_model.py \
     --model_dir=$ORIG_MODEL_PATH \
     --output_dir=$IMAGES_REG_DIR \
     --prompts="photo of a $CLASS_NAME" \
     --num_samples_per_prompt=200

Step 3: Fine-tune the model
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Save a few (4 to 5) images of the subject being fine-tuned
in a local directory. Then launch the training job with:

.. code-block::

   python train.py \
     --model_dir=$ORIG_MODEL_PATH \
     --output_dir=$TUNED_MODEL_DIR \
     --instance_images_dir=$IMAGES_OWN_DIR \
     --instance_prompt="a photo of unqtkn $CLASS_NAME" \
     --class_images_dir=$IMAGES_REG_DIR \
     --class_prompt="a photo of a $CLASS_NAME"

Step 4: Generate images of our subject
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Try your model with the same commandline as Step 2, but point
to your own model this time!

.. code-block::

   python run_model.py \
     --model_dir=$TUNED_MODEL_DIR \
     --output_dir=$IMAGES_NEW_DIR \
     --prompts="photo of a unqtkn $CLASS_NAME" \
     --num_samples_per_prompt=20
