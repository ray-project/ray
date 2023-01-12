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

.. panels::
    :container: container pb-4 full-width
    :column: col-md-4 px-2 py-2
    :img-top-cls: pt-5 w-75 d-block mx-auto

    ---
    :img-top: /images/pytorch_logo.png

    +++
    .. link-button:: torch_fashion_mnist_ex
        :type: ref
        :text: PyTorch Fashion MNIST Training Example
        :classes: btn-link btn-block stretched-link trainTorchFashionMnist

    ---
    :img-top: /images/pytorch_logo.png

    +++
    .. link-button:: train_transformers_example
        :type: ref
        :text: Transformers with PyTorch Training Example
        :classes: btn-link btn-block stretched-link trainTransformers

    ---
    :img-top: /images/tf_logo.png

    +++
    .. link-button:: tensorflow_mnist_example
        :type: ref
        :text: TensorFlow MNIST Training Example
        :classes: btn-link btn-block stretched-link trainTensorflowMnist

    ---
    :img-top: /images/horovod.png

    +++
    .. link-button:: horovod_example
        :type: ref
        :text: End-to-end Horovod Training Example
        :classes: btn-link btn-block stretched-link trainHorovod


Ray Train Examples Using Loggers & Callbacks
--------------------------------------------

.. panels::
    :container: container pb-4 full-width
    :column: col-md-4 px-2 py-2
    :img-top-cls: pt-5 w-75 d-block mx-auto

    ---
    :img-top: /images/mlflow.png

    +++
    .. link-button:: train_mlflow_example
        :type: ref
        :text: Logging Training Runs with MLflow
        :classes: btn-link btn-block stretched-link trainMlflow


Ray Train & Tune Integration Examples
-------------------------------------

.. panels::
    :container: container pb-4 full-width
    :column: col-md-4 px-2 py-2
    :img-top-cls: pt-5 w-75 d-block mx-auto

    ---
    :img-top: /images/tune.png

    +++
    .. link-button:: tune_train_tf_example
        :type: ref
        :text: End-to-end Example for Tuning a TensorFlow Model
        :classes: btn-link btn-block stretched-link trainTuneTensorflow

    ---
    :img-top: /images/tune.png

    +++
    .. link-button:: tune_train_torch_example
        :type: ref
        :text: End-to-end Example for Tuning a PyTorch Model with PBT
        :classes: btn-link btn-block stretched-link trainTunePyTorch

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

.. panels::
    :container: container pb-4 full-width
    :column: col-md-4 px-2 py-2
    :img-top-cls: pt-5 w-75 d-block mx-auto

    ---
    :img-top: /ray-overview/images/ray_svg_logo.svg

    +++
    .. link-button:: train_benchmark
        :type: ref
        :text: Benchmark example for the PyTorch data transfer auto pipeline
        :classes: btn-link btn-block stretched-link trainBenchmark
