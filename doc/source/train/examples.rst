.. _train-examples:

Ray Train Examples
==================

.. Example .rst files should be organized in the same manner as the
   .py files in ray/python/ray/train/examples.

Below are examples for using Ray Train with a variety of models, frameworks, 
and use cases. You can filter these examples by the following categories:


.. raw:: html

    <div>
        <div id="allButton" type="button" class="tag btn btn-primary">All</div>

        <!--Frameworks-->
        <div type="button" class="tag btn btn-outline-primary">PyTorch</div>
        <div type="button" class="tag btn btn-outline-primary">TensorFlow</div>
        <div type="button" class="tag btn btn-outline-primary">HuggingFace</div>
        <div type="button" class="tag btn btn-outline-primary">Horovod</div>
        <div type="button" class="tag btn btn-outline-primary">MLflow</div>

        <!--Workload-->
        <div type="button" class="tag btn btn-outline-primary">Training</div>
        <div type="button" class="tag btn btn-outline-primary">Tuning</div>
    </div>


Distributed Training Examples using Ray Train
---------------------------------------------

.. grid:: 1 2 3 3
    :gutter: 1
    :class-container: container pb-4

    .. grid-item-card::
        :img-top: /images/pytorch_logo.png
        :class-img-top: pt-2 w-75 d-block mx-auto fixed-height-img

        .. button-ref:: torch_fashion_mnist_ex

            PyTorch Fashion MNIST Training Example

    .. grid-item-card::
        :img-top: images/hugging.png
        :class-img-top: pt-2 w-75 d-block mx-auto fixed-height-img

        .. button-ref:: train_transformers_example

            Transformers with PyTorch Training Example

    .. grid-item-card::
        :img-top: /images/tf_logo.png
        :class-img-top: pt-2 w-75 d-block mx-auto fixed-height-img

        .. button-ref:: tensorflow_mnist_example

            TensorFlow MNIST Training Example

    .. grid-item-card::
        :img-top: /images/horovod.png
        :class-img-top: pt-2 w-75 d-block mx-auto fixed-height-img

        .. button-ref:: horovod_example

            End-to-end Horovod Training Example

    .. grid-item-card::
        :img-top: /images/pytorch_lightning_small.png
        :class-img-top: pt-2 w-75 d-block mx-auto fixed-height-img

        .. button-ref:: lightning_mnist_example

            End-to-end PyTorch Lightning Training Example

    .. grid-item-card::
        :img-top: /images/pytorch_lightning_small.png
        :class-img-top: pt-2 w-75 d-block mx-auto fixed-height-img

        .. button-ref:: lightning_advanced_example

            Use LightningTrainer with Ray Data and Batch Predictor
    

Ray Train Examples Using Loggers & Callbacks
--------------------------------------------


.. grid:: 1 2 3 4
    :gutter: 1
    :class-container: container pb-3

    .. grid-item-card::
        :img-top: /images/mlflow.png
        :class-img-top: pt-2 w-75 d-block mx-auto fixed-height-img

        .. button-ref:: train_mlflow_example

            Logging Training Runs with MLflow


Ray Train & Tune Integration Examples
-------------------------------------

.. grid:: 1 2 3 4
    :gutter: 1
    :class-container: container pb-3

    .. grid-item-card::
        :img-top: /images/tune.png
        :class-img-top: pt-2 w-75 d-block mx-auto fixed-height-img

        .. button-ref:: tune_train_tf_example

            End-to-end Example for Tuning a TensorFlow Model

    .. grid-item-card::
        :img-top: /images/tune.png
        :class-img-top: pt-2 w-75 d-block mx-auto fixed-height-img

        .. button-ref:: tune_train_torch_example

            End-to-end Example for Tuning a PyTorch Model with PBT

..
    TODO implement these examples!

    Features
    --------

    * Example for using a custom callback
    * End-to-end example for running on an elastic cluster (elastic training)

    Models
    ------

    * Example training on Vision model.

Ray Train Benchmarks
--------------------


.. grid:: 1 2 3 4
    :gutter: 1
    :class-container: container pb-3

    .. grid-item-card::
        :img-top: /ray-overview/images/ray_svg_logo.svg
        :class-img-top: pt-2 w-75 d-block mx-auto fixed-height-img

        .. button-ref:: train_benchmark

            Benchmark example for the PyTorch data transfer auto pipeline
