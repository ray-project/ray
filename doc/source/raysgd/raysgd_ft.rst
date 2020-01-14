RaySGD Fault Tolerance
======================

.. note:: Fault tolerance is currently only enabled for the PyTorchTrainer.

For distributed deep learning, jobs are often run on infrastructure where nodes can be pre-empted frequently (i.e., spot instances in the cloud). To overcome this, RaySGD provides **fault handling** features that enable training to continue regardless of node failures.

.. code-block:: bash

    trainer.train(max_retries=N)


How does it work?
-----------------

During each ``train`` method, each parallel worker iterates through the dataset, synchronizing gradients and parameters at each batch. These synchronization primitives can hang when one or more of the parallel workers becomes unresponsive (i.e., when a node is lost). To address this, we've implemented the following protocol.

  1. If any worker node is lost, Ray will mark the training task as complete (``ray.wait`` will return).
  2. Ray will throw ``RayActorException`` when fetching the result for any worker, so we will call ``ray.get`` on the "finished" training task.
  3. Upon catching this exception, the Trainer class will kill all of its workers.
  4. The Trainer will then detect the quantity of available resources, K. It will then restart K workers, each resuming from the last checkpoint.
  5. If there are no available resources, the Trainer will apply an exponential backoff before retrying to create workers.

Users can set ``checkpoint="auto"`` to always checkpoint the current model before executing a pass over the training dataset.

.. code-block:: bash

    trainer.train(max_retries=N, checkpoint="auto")



