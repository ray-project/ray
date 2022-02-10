Distributed TensorFlow
======================

.. warning:: This is an older version of Ray SGD. A newer, more light-weight version of Ray SGD (named Ray Train) is in alpha as of Ray 1.7.
         See the documentation :ref:`here <train-docs>`. To migrate from v1 to v2 you can follow the :ref:`migration guide <sgd-migration>`.

RaySGD's ``TFTrainer`` simplifies distributed model training for Tensorflow. The ``TFTrainer`` is a wrapper around ``MultiWorkerMirroredStrategy`` with a Python API to easily incorporate distributed training into a larger Python application, as opposed to write custom logic of setting environments and starting separate processes.

Under the hood, ``TFTrainer`` will create *replicas* of your model (controlled by ``num_replicas``), each of which is managed by a Ray actor.

.. image:: raysgd-actors.svg
    :align: center

.. tip:: Get in touch with us if you're using or considering using `RaySGD <https://forms.gle/PXFcJmHwszCwQhqX7>`_!

----------

**With Ray**:

Wrap your training with this:

.. code-block:: python

    ray.init(args.address)

    trainer = TFTrainer(
        model_creator=model_creator,
        data_creator=data_creator,
        num_replicas=4,
        use_gpu=True,
        verbose=True,
        config={
            "fit_config": {
                "steps_per_epoch": num_train_steps,
            },
            "evaluate_config": {
                "steps": num_eval_steps,
            }
        })


Then, start a :ref:`Ray cluster <cluster-index>`.

.. code-block:: bash

    ray up CLUSTER.yaml
    python train.py --address="localhost:<PORT>"


----------

**Before, with Tensorflow**:

In your training program, insert the following, and **customize** for each worker:

.. code-block:: python

    os.environ['TF_CONFIG'] = json.dumps({
        'cluster': {
            'worker': ["localhost:12345", "localhost:23456"]
        },
        'task': {'type': 'worker', 'index': 0}
    })

    ...
    strategy = tf.distribute.experimental.MultiWorkerMirroredStrategy()
    with strategy.scope():
      multi_worker_model = model_creator()

And on each machine, launch a separate process that contains the index of the worker and information about all other nodes of the cluster.

.. _ray-train-tftrainer-example:

TFTrainer Example
-----------------

Below is an example of using Ray's TFTrainer. Under the hood, ``TFTrainer`` will create *replicas* of your model (controlled by ``num_replicas``) which are each managed by a worker.

.. literalinclude:: ../../../python/ray/util/sgd/tf/examples/tensorflow_train_example.py
   :language: python

