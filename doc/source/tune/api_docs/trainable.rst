.. _trainable-docs:

Training (tune.Trainable, tune.track)
=====================================

Training can be done with either a **Class API** (``tune.Trainable``) or **function-based API** (``track.log``).

You can use the **function-based API** for fast prototyping. On the other hand, the ``tune.Trainable`` interface supports checkpoint/restore functionality and provides more control for advanced algorithms.

For the sake of example, let's maximize this objective function:

.. code-block:: python

    def objective(x, a, b):
        return a * (x ** 0.5) + b

.. _tune-function-api:

Function-based API
------------------

.. code-block:: python

    def trainable(config):
        # config (dict): A dict of hyperparameters.

        for x in range(20):
            score = objective(x, config["a"], config["b"])

            tune.track.log(score=score)  # This sends the score to Tune.

    analysis = tune.run(
        trainable,
        config={
            "a": 2,
            "b": 4
        })

    print("best config: ", analysis.get_best_config(metric="score", mode="max"))

.. tip:: Do not use ``tune.track.log`` within a ``Trainable`` class.

Tune will run this function on a separate thread in a Ray actor process. Note that this API is not checkpointable, since the thread will never return control back to its caller.

.. note:: If you want to pass in a Python lambda, you will need to first register the function: ``tune.register_trainable("lambda_id", lambda x: ...)``. You can then use ``lambda_id`` in place of ``my_trainable``.

.. _tune-class-api:

Trainable Class API
-------------------

.. caution:: Do not use ``tune.track.log`` within a ``Trainable`` class.

The Trainable **class API** will require users to subclass ``ray.tune.Trainable``. Here's a naive example of this API:

.. code-block:: python

    from ray import tune

    class Trainable(tune.Trainable):
        def _setup(self, config):
            # config (dict): A dict of hyperparameters
            self.x = 0
            self.a = config["a"]
            self.b = config["b"]

        def _train(self):  # This is called iteratively.
            score = objective(self.x, self.a, self.b)
            self.x += 1
            return {"score": score}

    analysis = tune.run(
        Trainable,
        stop={"training_iteration": 20},
        config={
            "a": 2,
            "b": 4
        })

    print('best config: ', analysis.get_best_config(metric="score", mode="max"))

As a subclass of ``tune.Trainable``, Tune will create a ``Trainable`` object on a separate process (using the :ref:`Ray Actor API <actor-guide>`).

  1. ``_setup`` function is invoked once training starts.
  2. ``_train`` is invoked **multiple times**. Each time, the Trainable object executes one logical iteration of training in the tuning process, which may include one or more iterations of actual training.
  3. ``_stop`` is invoked when training is finished.

.. tip:: As a rule of thumb, the execution time of ``_train`` should be large enough to avoid overheads (i.e. more than a few seconds), but short enough to report progress periodically (i.e. at most a few minutes).

In this example, we only implemented the ``_setup`` and ``_train`` methods for simplification. Next, we'll implement ``_save`` and ``_restore`` for checkpoint and fault tolerance.

.. _tune-trainable-save-restore:

Save and Restore
~~~~~~~~~~~~~~~~

Many Tune features rely on ``_save``, and ``_restore``, including the usage of certain Trial Schedulers, fault tolerance, and checkpointing.

.. code-block:: python

    class MyTrainableClass(Trainable):
        def _save(self, tmp_checkpoint_dir):
            checkpoint_path = os.path.join(tmp_checkpoint_dir, "model.pth")
            torch.save(self.model.state_dict(), checkpoint_path)
            return tmp_checkpoint_dir

        def _restore(self, tmp_checkpoint_dir):
            checkpoint_path = os.path.join(tmp_checkpoint_dir, "model.pth")
            self.model.load_state_dict(torch.load(checkpoint_path))

Checkpoints will be saved by training iteration to ``local_dir/exp_name/trial_name/checkpoint_<iter>``. You can restore a single trial checkpoint by using ``tune.run(restore=<checkpoint_dir>)``.

Tune also generates temporary checkpoints for pausing and switching between trials. For this purpose, it is important not to depend on absolute paths in the implementation of ``save``.

Use ``validate_save_restore`` to catch ``_save``/``_restore`` errors before execution.

.. code-block:: python

    from ray.tune.utils import validate_save_restore

    # both of these should return
    validate_save_restore(MyTrainableClass)
    validate_save_restore(MyTrainableClass, use_object_store=True)


Advanced Resource Allocation
----------------------------

Trainables can themselves be distributed. If your trainable function / class creates further Ray actors or tasks that also consume CPU / GPU resources, you will want to set ``extra_cpu`` or ``extra_gpu`` inside ``tune.run`` to reserve extra resource slots. For example, if a trainable class requires 1 GPU itself, but also launches 4 actors, each using another GPU, then you should set ``"gpu": 1, "extra_gpu": 4``.

.. code-block:: python
   :emphasize-lines: 4-8

    tune.run(
        my_trainable,
        name="my_trainable",
        resources_per_trial={
            "cpu": 1,
            "gpu": 1,
            "extra_gpu": 4
        }
    )

The ``Trainable`` also provides the ``default_resource_requests`` interface to automatically declare the ``resources_per_trial`` based on the given configuration.


Advanced: Reusing Actors
~~~~~~~~~~~~~~~~~~~~~~~~

Your Trainable can often take a long time to start. To avoid this, you can do ``tune.run(reuse_actors=True)`` to reuse the same Trainable Python process and object for multiple hyperparameters.

This requires you to implement ``Trainable.reset_config``, which provides a new set of hyperparameters. It is up to the user to correctly update the hyperparameters of your trainable.

.. code-block:: python

    class PytorchTrainble(tune.Trainable):
        """Train a Pytorch ConvNet."""

        def _setup(self, config):
            self.train_loader, self.test_loader = get_data_loaders()
            self.model = ConvNet()
            self.optimizer = optim.SGD(
                self.model.parameters(),
                lr=config.get("lr", 0.01),
                momentum=config.get("momentum", 0.9))

        def reset_config(self, new_config):
            for param_group in self.optimizer.param_groups:
                if "lr" in new_config:
                    param_group["lr"] = new_config["lr"]
                if "momentum" in new_config:
                    param_group["momentum"] = new_config["momentum"]

            self.model = ConvNet()
            self.config = new_config
            return True


tune.Trainable
--------------


.. autoclass:: ray.tune.Trainable
    :member-order: groupwise
    :private-members:
    :members:

tune.DurableTrainable
---------------------

.. autoclass:: ray.tune.DurableTrainable

.. _track-docstring:

tune.track
----------

.. automodule:: ray.tune.track
    :members:
    :exclude-members: init,

KerasCallback
-------------

.. automodule:: ray.tune.integration.keras
    :members:


StatusReporter
--------------

.. autoclass:: ray.tune.function_runner.StatusReporter
    :members: __call__, logdir
