.. _trainable-docs:

.. TODO: these "basic" sections before the actual API docs start don't really belong here. Then again, the function
    API does not really have a signature to just describe.
.. TODO: Reusing actors and advanced resources allocation seem ill-placed.

Training (tune.Trainable, session.report)
==========================================

Training can be done with either a **Class API** (``tune.Trainable``) or **function API** (``session.report``).

For the sake of example, let's maximize this objective function:

.. code-block:: python

    def objective(x, a, b):
        return a * (x ** 0.5) + b

.. _tune-function-api:

Function API
------------

With the Function API, you can report intermediate metrics by simply calling ``session.report`` within the provided function.

.. code-block:: python

    def trainable(config):
        # config (dict): A dict of hyperparameters.

        for x in range(20):
            intermediate_score = objective(x, config["a"], config["b"])

            session.report({"score": intermediate_score})  # This sends the score to Tune.

    tuner = tune.Tuner(
        trainable,
        param_space={"a": 2, "b": 4}
    )
    results = tuner.fit()

    print("best config: ", results.get_best_result(metric="score", mode="max").config)

.. tip:: Do not use ``session.report`` within a ``Trainable`` class.

Tune will run this function on a separate thread in a Ray actor process.

You'll notice that Ray Tune will output extra values in addition to the user reported metrics,
such as ``iterations_since_restore``. See :ref:`tune-autofilled-metrics` for an explanation/glossary of these values.

.. code-block:: python

    def trainable(config):
        # config (dict): A dict of hyperparameters.

        final_score = 0
        for x in range(20):
            final_score = objective(x, config["a"], config["b"])

        return {"score": final_score}  # This sends the score to Tune.

    tuner = tune.Tuner(
        trainable,
        param_space={"a": 2, "b": 4}
    )
    results = tuner.fit()

    print("best config: ", results.get_best_result(metric="score", mode="max").config)


.. _tune-function-checkpointing:

Function API Checkpointing
~~~~~~~~~~~~~~~~~~~~~~~~~~

Many Tune features rely on checkpointing, including the usage of certain Trial Schedulers and fault tolerance.
You can save and load checkpoint in Ray Tune in the following manner:

.. code-block:: python

        import time
        from ray import tune
        from ray.air import session
        from ray.air.checkpoint import Checkpoint

        def train_func(config):
            step = 0
            loaded_checkpoint = session.get_checkpoint()
            if loaded_checkpoint:
                last_step = loaded_checkpoint.to_dict()["step"]
                step = last_step + 1

            for iter in range(step, 100):
                time.sleep(1)

                checkpoint = Checkpoint.from_dict({"step": step})
                session.report({"message": "Hello world Ray Tune!"}, checkpoint=checkpoint)

        tuner = tune.Tuner(train_func)
        results = tuner.fit()

.. note:: ``checkpoint_frequency`` and ``checkpoint_at_end`` will not work with Function API checkpointing.

In this example, checkpoints will be saved by training iteration to ``local_dir/exp_name/trial_name/checkpoint_<step>``.

Tune also may copy or move checkpoints during the course of tuning. For this purpose,
it is important not to depend on absolute paths in the implementation of ``save``.

.. _tune-class-api:

Trainable Class API
-------------------

.. caution:: Do not use ``session.report`` within a ``Trainable`` class.

The Trainable **class API** will require users to subclass ``ray.tune.Trainable``. Here's a naive example of this API:

.. code-block:: python

    from ray import tune

    class Trainable(tune.Trainable):
        def setup(self, config):
            # config (dict): A dict of hyperparameters
            self.x = 0
            self.a = config["a"]
            self.b = config["b"]

        def step(self):  # This is called iteratively.
            score = objective(self.x, self.a, self.b)
            self.x += 1
            return {"score": score}

    tuner = tune.Tuner(
        Trainable,
        tune_config=air.RunConfig(stop={"training_iteration": 20}),
        param_space={
            "a": 2,
            "b": 4
        })
    results = tuner.fit()

    print('best config: ', results.get_best_result(metric="score", mode="max").config)

As a subclass of ``tune.Trainable``, Tune will create a ``Trainable`` object on a
separate process (using the :ref:`Ray Actor API <actor-guide>`).

  1. ``setup`` function is invoked once training starts.
  2. ``step`` is invoked **multiple times**.
     Each time, the Trainable object executes one logical iteration of training in the tuning process,
     which may include one or more iterations of actual training.
  3. ``cleanup`` is invoked when training is finished.

.. tip:: As a rule of thumb, the execution time of ``step`` should be large enough to avoid overheads
    (i.e. more than a few seconds), but short enough to report progress periodically (i.e. at most a few minutes).

You'll notice that Ray Tune will output extra values in addition to the user reported metrics,
such as ``iterations_since_restore``.
See :ref:`tune-autofilled-metrics` for an explanation/glossary of these values.

.. _tune-trainable-save-restore:

Class API Checkpointing
~~~~~~~~~~~~~~~~~~~~~~~

You can also implement checkpoint/restore using the Trainable Class API:

.. code-block:: python

    class MyTrainableClass(Trainable):
        def save_checkpoint(self, tmp_checkpoint_dir):
            checkpoint_path = os.path.join(tmp_checkpoint_dir, "model.pth")
            torch.save(self.model.state_dict(), checkpoint_path)
            return tmp_checkpoint_dir

        def load_checkpoint(self, tmp_checkpoint_dir):
            checkpoint_path = os.path.join(tmp_checkpoint_dir, "model.pth")
            self.model.load_state_dict(torch.load(checkpoint_path))

    tuner = tune.Tuner(MyTrainableClass, run_config=air.RunConfig(checkpoint_config=air.CheckpointConfig(checkpoint_frequency=2)))
    results = tuner.fit()

You can checkpoint with three different mechanisms: manually, periodically, and at termination.

**Manual Checkpointing**: A custom Trainable can manually trigger checkpointing by returning ``should_checkpoint: True``
(or ``tune.result.SHOULD_CHECKPOINT: True``) in the result dictionary of `step`.
This can be especially helpful in spot instances:

.. code-block:: python

    def step(self):
        # training code
        result = {"mean_accuracy": accuracy}
        if detect_instance_preemption():
            result.update(should_checkpoint=True)
        return result


**Periodic Checkpointing**: periodic checkpointing can be used to provide fault-tolerance for experiments.
This can be enabled by setting ``checkpoint_frequency=<int>`` and ``max_failures=<int>`` to checkpoint trials
every *N* iterations and recover from up to *M* crashes per trial, e.g.:

.. code-block:: python

    tuner = tune.Tuner(
        my_trainable,
        run_config=air.RunConfig(
            checkpoint_config=air.CheckpointConfig(checkpoint_frequency=10),
            failure_config=air.FailureConfig(max_failures=5))
    )
    results = tuner.fit()

**Checkpointing at Termination**: The checkpoint_frequency may not coincide with the exact end of an experiment.
If you want a checkpoint to be created at the end of a trial, you can additionally set the ``checkpoint_at_end=True``:

.. code-block:: python
   :emphasize-lines: 5

    tuner = tune.Tuner(
        my_trainable,
        run_config=air.RunConfig(
            checkpoint_config=air.CheckpointConfig(checkpoint_frequency=10, checkpoint_at_end=True),
            failure_config=air.FailureConfig(max_failures=5))
    )
    results = tuner.fit()


Use ``validate_save_restore`` to catch ``save_checkpoint``/``load_checkpoint`` errors before execution.

.. code-block:: python

    from ray.tune.utils import validate_save_restore

    # both of these should return
    validate_save_restore(MyTrainableClass)
    validate_save_restore(MyTrainableClass, use_object_store=True)



Advanced: Reusing Actors
~~~~~~~~~~~~~~~~~~~~~~~~

.. note:: This feature is only for the Trainable Class API.

Your Trainable can often take a long time to start.
To avoid this, you can do ``tune.TuneConfig(reuse_actors=True)`` (which is taken in by ``Tuner``) to reuse the same Trainable Python process and
object for multiple hyperparameters.

This requires you to implement ``Trainable.reset_config``, which provides a new set of hyperparameters.
It is up to the user to correctly update the hyperparameters of your trainable.

.. code-block:: python

    class PytorchTrainble(tune.Trainable):
        """Train a Pytorch ConvNet."""

        def setup(self, config):
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


Advanced Resource Allocation
----------------------------

Trainables can themselves be distributed. If your trainable function / class creates further Ray actors or tasks
that also consume CPU / GPU resources, you will want to add more bundles to the :class:`PlacementGroupFactory`
to reserve extra resource slots.
For example, if a trainable class requires 1 GPU itself, but also launches 4 actors, each using another GPU,
then you should use this:

.. code-block:: python
   :emphasize-lines: 4-10

    tuner = tune.Tuner(
        tune.with_resources(my_trainable, tune.PlacementGroupFactory([
            {"CPU": 1, "GPU": 1},
            {"GPU": 1},
            {"GPU": 1},
            {"GPU": 1},
            {"GPU": 1}
        ])),
        run_config=air.RunConfig(name="my_trainable")
    )

The ``Trainable`` also provides the ``default_resource_requests`` interface to automatically
declare the resources per trial based on the given configuration.

It is also possible to specify memory (``"memory"``, in bytes) and custom resource requirements.


.. _tune-function-docstring:

session (Function API)
----------------------

.. autofunction:: ray.air.session.report
    :noindex:

.. autofunction:: ray.air.session.get_checkpoint
    :noindex:

.. autofunction:: ray.air.session.get_trial_name
    :noindex:

.. autofunction:: ray.air.session.get_trial_id
    :noindex:

.. autofunction:: ray.air.session.get_trial_resources
    :noindex:

tune.Trainable (Class API)
--------------------------


.. autoclass:: ray.tune.Trainable
    :member-order: groupwise
    :private-members:
    :members:

.. _tune-util-ref:

Utilities
---------

.. autofunction:: ray.tune.utils.wait_for_gpu

.. autofunction:: ray.tune.utils.diagnose_serialization

.. autofunction:: ray.tune.utils.validate_save_restore


.. _tune-with-parameters:

tune.with_parameters
--------------------

.. autofunction:: ray.tune.with_parameters

