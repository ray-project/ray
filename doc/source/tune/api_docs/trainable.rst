.. _trainable-docs:

Training (tune.Trainable, tune.track)
=====================================

Training can be done with either a **Class API** (``tune.Trainable``) or **function-based API** (``track.log``).

You can use the **function-based API** for fast prototyping. On the other hand, the ``tune.Trainable`` interface supports checkpoint/restore functionality and provides more control for advanced algorithms.

Function-based API
------------------

.. code-block:: python

    def trainable(config):
        """
        Args:
            config (dict): Parameters provided from the search algorithm
                or variant generation.
        """

        while True:
            # ...
            tune.track.log(**kwargs)

.. tip:: Do not use ``tune.track.log`` within a ``Trainable`` class.

Tune will run this function on a separate thread in a Ray actor process. Note that this API is not checkpointable, since the thread will never return control back to its caller.

.. note:: If you have a lambda function that you want to train, you will need to first register the function: ``tune.register_trainable("lambda_id", lambda x: ...)``. You can then use ``lambda_id`` in place of ``my_trainable``.

Trainable API
-------------

.. caution:: Do not use ``tune.track.log`` within a ``Trainable`` class.

The Trainable **class API** will require users to subclass ``ray.tune.Trainable``. Here's a naive example of this API:

.. code-block:: python

    from ray import tune

    class Guesser(tune.Trainable):
        """Randomly picks a number from [1, 10000) to find the password."""

        def _setup(self, config):
            self.guess = config["guess"]
            self.iter = 0
            self.password = 1024

        def _train(self):
            """Execute one step of 'training'. This function will be called iteratively"""
            self.iter += 1
            self.guess += 1
            return {
                "accuracy": abs(self.guess - self.password),
                "training_iteration": self.iter  # Tune will automatically provide this.
            }


    analysis = tune.run(
        Guesser,
        stop={"training_iteration": 10},
        num_samples=10,
        config={
            "guess": tune.randint(1, 10000)
        })

    print('best config: ', analysis.get_best_config(metric="diff", mode="min"))

As a subclass of ``tune.Trainable``, Tune will create a ``Guesser`` object on a separate process (using the Ray Actor API).

  1. ``_setup`` function is invoked once training starts.
  2. ``_train`` is invoked **multiple times**. Each time, the Guesser object executes one logical iteration of training in the tuning process, which may include one or more iterations of actual training.
  3. ``_stop`` is invoked when training is finished.

.. tip:: As a rule of thumb, the execution time of ``_train`` should be large enough to avoid overheads (i.e. more than a few seconds), but short enough to report progress periodically (i.e. at most a few minutes).

In this example, we only implemented the ``_setup`` and ``_train`` methods for simplification. Next, we'll implement ``_save`` and ``_restore`` for checkpoint and fault tolerance.

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
