.. _train-experiment-tracking-logger-callback:

Use Ray Train Report API and Logger Callbacks for Experiment Tracking
=====================================================================

.. warning::

    For DataParallelTrainer (TorchTrainer or TensorflowTrainer), you should probably 
    use native experiment tracking libraries by customizing your tracking logic 
    inside ``train_loop_per_worker`` function. This provides a simple integration 
    that doesn't require changes to your training code. 
    
    See :ref:`train-experiment-tracking-native` for the best practice there.

Ray Train also exposes logging callbacks that automatically report results to
experiment tracking services. This will use the results reported via the
:func:`~ray.train.report` API.

Example: Logging to MLflow and TensorBoard
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Step 1: Install the necessary packages**

.. code-block:: bash

    $ pip install mlflow
    $ pip install tensorboardX

**Step 2: Run the following training script**

.. literalinclude:: /../../python/ray/train/examples/mlflow_simple_example.py
   :language: python
