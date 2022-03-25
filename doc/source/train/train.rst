.. include:: /_includes/train/announcement.rst

.. _train-docs:

Ray Train: Distributed Deep Learning
====================================

.. _`issue on GitHub`: https://github.com/ray-project/ray/issues

.. tip:: Get in touch with us if you're using or considering using `Ray Train <https://forms.gle/PXFcJmHwszCwQhqX7>`_!

Ray Train is a lightweight library for distributed deep learning, allowing you
to scale up and speed up training for your deep learning models.

The main features are:

- **Ease of use**: Scale your single process training code to a cluster in just a couple lines of code.
- **Composability**: Ray Train interoperates with :ref:`Ray Tune <tune-main>` to tune your distributed model and :ref:`Ray Datasets <datasets>` to train on large amounts of data.
- **Interactivity**: Ray Train fits in your workflow with support to run from any environment, including seamless Jupyter notebook support.

.. note::

  This API is in its Beta release (as of Ray 1.9) and may be revised in
  future Ray releases. If you encounter any bugs, please file an
  `issue on GitHub`_.
  If you are looking for the previous API documentation, see :ref:`sgd-index`.

Intro to Ray Train
------------------

Ray Train is a library that aims to simplify distributed deep learning.

**Frameworks**: Ray Train is built to abstract away the coordination/configuration setup of distributed deep learning frameworks such as Pytorch Distributed and Tensorflow Distributed, allowing users to only focus on implementing training logic.

* For Pytorch, Ray Train automatically handles the construction of the distributed process group.
* For Tensorflow, Ray Train automatically handles the coordination of the ``TF_CONFIG``. The current implementation assumes that the user will use a MultiWorkerMirroredStrategy, but this will change in the near future.
* For Horovod, Ray Train automatically handles the construction of the Horovod runtime and Rendezvous server.

**Built for data scientists/ML practitioners**: Ray Train has support for standard ML tools and features that practitioners love:

* Callbacks for early stopping
* Checkpointing
* Integration with TensorBoard, Weights/Biases, and MLflow
* Jupyter notebooks

**Integration with Ray Ecosystem**: Distributed deep learning often comes with a lot of complexity.


* Use :ref:`Ray Datasets <datasets>` with Ray Train to handle and train on large amounts of data.
* Use :ref:`Ray Tune <tune-main>` with Ray Train to leverage cutting edge hyperparameter techniques and distribute both your training and tuning.
* You can leverage the :ref:`Ray cluster launcher <cluster-cloud>` to launch autoscaling or spot instance clusters to train your model at scale on any cloud.


Quick Start
-----------

Ray Train abstracts away the complexity of setting up a distributed training
system. Let's take following simple examples:

.. tabbed:: PyTorch

    This example shows how you can use Ray Train with PyTorch.

    First, set up your dataset and model.

    .. literalinclude:: /../../python/ray/train/examples/torch_quick_start.py
       :language: python
       :start-after: __torch_setup_begin__
       :end-before: __torch_setup_end__


    Now define your single-worker PyTorch training function.

    .. literalinclude:: /../../python/ray/train/examples/torch_quick_start.py
       :language: python
       :start-after: __torch_single_begin__
       :end-before: __torch_single_end__

    This training function can be executed with:

    .. literalinclude:: /../../python/ray/train/examples/torch_quick_start.py
       :language: python
       :start-after: __torch_single_run_begin__
       :end-before: __torch_single_run_end__

    Now let's convert this to a distributed multi-worker training function!

    All you have to do is use the ``ray.train.torch.prepare_model`` and
    ``ray.train.torch.prepare_data_loader`` utility functions to
    easily setup your model & data for distributed training.
    This will automatically wrap your model with ``DistributedDataParallel``
    and place it on the right device, and add ``DistributedSampler`` to your DataLoaders.

    .. literalinclude:: /../../python/ray/train/examples/torch_quick_start.py
       :language: python
       :start-after: __torch_distributed_begin__
       :end-before: __torch_distributed_end__

    Then, instantiate a ``Trainer`` that uses a ``"torch"`` backend
    with 4 workers, and use it to run the new training function!

    .. literalinclude:: /../../python/ray/train/examples/torch_quick_start.py
       :language: python
       :start-after: __torch_trainer_begin__
       :end-before: __torch_trainer_end__

    See :ref:`train-porting-code` for a more comprehensive example.

.. tabbed:: TensorFlow

    This example shows how you can use Ray Train to set up `Multi-worker training
    with Keras <https://www.tensorflow.org/tutorials/distribute/multi_worker_with_keras>`_.

    First, set up your dataset and model.

    .. literalinclude:: /../../python/ray/train/examples/tensorflow_quick_start.py
       :language: python
       :start-after: __tf_setup_begin__
       :end-before: __tf_setup_end__

    Now define your single-worker TensorFlow training function.

    .. literalinclude:: /../../python/ray/train/examples/tensorflow_quick_start.py
           :language: python
           :start-after: __tf_single_begin__
           :end-before: __tf_single_end__

    This training function can be executed with:

    .. literalinclude:: /../../python/ray/train/examples/tensorflow_quick_start.py
       :language: python
       :start-after: __tf_single_run_begin__
       :end-before: __tf_single_run_end__

    Now let's convert this to a distributed multi-worker training function!
    All you need to do is:

    1. Set the *global* batch size - each worker will process the same size
       batch as in the single-worker code.
    2. Choose your TensorFlow distributed training strategy. In this example
       we use the ``MultiWorkerMirroredStrategy``.

    .. literalinclude:: /../../python/ray/train/examples/tensorflow_quick_start.py
       :language: python
       :start-after: __tf_distributed_begin__
       :end-before: __tf_distributed_end__

    Then, instantiate a ``Trainer`` that uses a ``"tensorflow"`` backend
    with 4 workers, and use it to run the new training function!

    .. literalinclude:: /../../python/ray/train/examples/tensorflow_quick_start.py
       :language: python
       :start-after: __tf_trainer_begin__
       :end-before: __tf_trainer_end__

    See :ref:`train-porting-code` for a more comprehensive example.


**Next steps:** Check out the :ref:`User Guide <train-user-guide>`!

.. include:: /_includes/train/announcement_bottom.rst
