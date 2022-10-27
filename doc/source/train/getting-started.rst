.. _train-getting-started:

Getting Started
===============

Ray Train offers multiple ``Trainers`` which implement scalable model training for different machine learning frameworks.
Here are examples for some of the commonly used trainers:

.. tabbed:: XGBoost

    In this example we will train a model using distributed XGBoost.

    First, we load the dataset from S3 using Ray Datasets and split it into a
    train and validation dataset.

    .. literalinclude:: doc_code/gbdt_user_guide.py
       :language: python
       :start-after: __xgb_detail_intro_start__
       :end-before: __xgb_detail_intro_end__

    In the :class:`ScalingConfig <ray.air.config.ScalingConfig>`,
    we configure the number of workers to use:

    .. literalinclude:: doc_code/gbdt_user_guide.py
       :language: python
       :start-after: __xgb_detail_scaling_start__
       :end-before: __xgb_detail_scaling_end__

    We then instantiate our XGBoostTrainer by passing in:

    - The aforementioned ``ScalingConfig``.
    - The ``label_column`` refers to the column name containing the labels in the Ray Dataset
    - The ``params`` are `XGBoost training parameters <https://xgboost.readthedocs.io/en/stable/parameter.html>`__

    .. literalinclude:: doc_code/gbdt_user_guide.py
        :language: python
        :start-after: __xgb_detail_training_start__
        :end-before: __xgb_detail_training_end__

    Lastly, we call ``trainer.fit()`` to kick off training and obtain the results.

    .. literalinclude:: doc_code/gbdt_user_guide.py
        :language: python
        :start-after: __xgb_detail_fit_start__
        :end-before: __xgb_detail_fit_end__

.. tabbed:: LightGBM

    In this example we will train a model using distributed LightGBM.

    First, we load the dataset from S3 using Ray Datasets and split it into a
    train and validation dataset.

    .. literalinclude:: doc_code/gbdt_user_guide.py
        :language: python
        :start-after: __lgbm_detail_intro_start__
        :end-before: __lgbm_detail_intro_end__

    In the :class:`ScalingConfig <ray.air.config.ScalingConfig>`,
    we configure the number of workers to use:

    .. literalinclude:: doc_code/gbdt_user_guide.py
        :language: python
        :start-after: __xgb_detail_scaling_start__
        :end-before: __xgb_detail_scaling_end__

    We then instantiate our LightGBMTrainer by passing in:

    - The aforementioned ``ScalingConfig``
    - The ``label_column`` refers to the column name containing the labels in the Ray Dataset
    - The ``params`` are core `LightGBM training parameters <https://lightgbm.readthedocs.io/en/latest/Parameters.html>`__

    .. literalinclude:: doc_code/gbdt_user_guide.py
        :language: python
        :start-after: __lgbm_detail_training_start__
        :end-before: __lgbm_detail_training_end__

    And lastly we call ``trainer.fit()`` to kick off training and obtain the results.

    .. literalinclude:: doc_code/gbdt_user_guide.py
        :language: python
        :start-after: __lgbm_detail_fit_start__
        :end-before: __lgbm_detail_fit_end__

.. tabbed:: PyTorch

    This example shows how you can use Ray Train with PyTorch.

    First, set up your dataset and model.

    .. literalinclude:: /../../python/ray/train/examples/pytorch/torch_quick_start.py
        :language: python
        :start-after: __torch_setup_begin__
        :end-before: __torch_setup_end__


    Now define your single-worker PyTorch training function.

    .. literalinclude:: /../../python/ray/train/examples/pytorch/torch_quick_start.py
        :language: python
        :start-after: __torch_single_begin__
        :end-before: __torch_single_end__

    This training function can be executed with:

    .. literalinclude:: /../../python/ray/train/examples/pytorch/torch_quick_start.py
        :language: python
        :start-after: __torch_single_run_begin__
        :end-before: __torch_single_run_end__

    Now let's convert this to a distributed multi-worker training function!

    All you have to do is use the ``ray.train.torch.prepare_model`` and
    ``ray.train.torch.prepare_data_loader`` utility functions to
    easily setup your model & data for distributed training.
    This will automatically wrap your model with ``DistributedDataParallel``
    and place it on the right device, and add ``DistributedSampler`` to your DataLoaders.

    .. literalinclude:: /../../python/ray/train/examples/pytorch/torch_quick_start.py
        :language: python
        :start-after: __torch_distributed_begin__
        :end-before: __torch_distributed_end__

    Then, instantiate a ``TorchTrainer``
    with 4 workers, and use it to run the new training function!

    .. literalinclude:: /../../python/ray/train/examples/pytorch/torch_quick_start.py
        :language: python
        :start-after: __torch_trainer_begin__
        :end-before: __torch_trainer_end__

    See :ref:`train-porting-code` for a more comprehensive example.

.. tabbed:: TensorFlow

    This example shows how you can use Ray Train to set up `Multi-worker training
    with Keras <https://www.tensorflow.org/tutorials/distribute/multi_worker_with_keras>`_.

    First, set up your dataset and model.

    .. literalinclude:: /../../python/ray/train/examples/tf/tensorflow_quick_start.py
        :language: python
        :start-after: __tf_setup_begin__
        :end-before: __tf_setup_end__

    Now define your single-worker TensorFlow training function.

    .. literalinclude:: /../../python/ray/train/examples/tf/tensorflow_quick_start.py
        :language: python
        :start-after: __tf_single_begin__
        :end-before: __tf_single_end__

    This training function can be executed with:

    .. literalinclude:: /../../python/ray/train/examples/tf/tensorflow_quick_start.py
        :language: python
        :start-after: __tf_single_run_begin__
        :end-before: __tf_single_run_end__

    Now let's convert this to a distributed multi-worker training function!
    All you need to do is:

    1. Set the per-worker batch size - each worker will process the same size
       batch as in the single-worker code.
    2. Choose your TensorFlow distributed training strategy. In this example
       we use the ``MultiWorkerMirroredStrategy``.

    .. literalinclude:: /../../python/ray/train/examples/tf/tensorflow_quick_start.py
        :language: python
        :start-after: __tf_distributed_begin__
        :end-before: __tf_distributed_end__

    Then, instantiate a ``TensorflowTrainer`` with 4 workers,
    and use it to run the new training function!

    .. literalinclude:: /../../python/ray/train/examples/tf/tensorflow_quick_start.py
        :language: python
        :start-after: __tf_trainer_begin__
        :end-before: __tf_trainer_end__

    See :ref:`train-porting-code` for a more comprehensive example.
