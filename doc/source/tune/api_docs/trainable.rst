.. _trainable-docs:

.. TODO: these "basic" sections before the actual API docs start don't really belong here. Then again, the function
    API does not really have a signature to just describe.
.. TODO: Reusing actors and advanced resources allocation seem ill-placed.

Training (tune.Trainable, tune.report)
======================================

Training can be done with either a **Class API** (``tune.Trainable``) or **function API** (``tune.report``).

For the sake of example, let's maximize this objective function:

.. code-block:: python

    def objective(x, a, b):
        return a * (x ** 0.5) + b

.. _tune-function-api:

Function API
------------

With the Function API, you can report intermediate metrics by simply calling ``tune.report`` within the provided function.

.. code-block:: python

    def trainable(config):
        # config (dict): A dict of hyperparameters.

        for x in range(20):
            intermediate_score = objective(x, config["a"], config["b"])

            tune.report(score=intermediate_score)  # This sends the score to Tune.

    analysis = tune.run(
        trainable,
        config={"a": 2, "b": 4}
    )

    print("best config: ", analysis.get_best_config(metric="score", mode="max"))

.. tip:: Do not use ``tune.report`` within a ``Trainable`` class.

Tune will run this function on a separate thread in a Ray actor process.

You'll notice that Ray Tune will output extra values in addition to the user reported metrics,
such as ``iterations_since_restore``. See :ref:`tune-autofilled-metrics` for an explanation/glossary of these values.

Function API return and yield values
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Instead of using ``tune.report()``, you can also use Python's ``yield``
statement to report metrics to Ray Tune:


.. code-block:: python

    def trainable(config):
        # config (dict): A dict of hyperparameters.

        for x in range(20):
            intermediate_score = objective(x, config["a"], config["b"])

            yield {"score": intermediate_score}  # This sends the score to Tune.

    analysis = tune.run(
        trainable,
        config={"a": 2, "b": 4}
    )

    print("best config: ", analysis.get_best_config(metric="score", mode="max"))

If you yield a dictionary object, this will work just as ``tune.report()``.
If you yield a number, if will be reported to Ray Tune with the key ``_metric``, i.e.
as if you had called ``tune.report(_metric=value)``.

Ray Tune supports the same functionality for return values if you only
report metrics at the end of each run:

.. code-block:: python

    def trainable(config):
        # config (dict): A dict of hyperparameters.

        final_score = 0
        for x in range(20):
            final_score = objective(x, config["a"], config["b"])

        return {"score": final_score}  # This sends the score to Tune.

    analysis = tune.run(
        trainable,
        config={"a": 2, "b": 4}
    )

    print("best config: ", analysis.get_best_config(metric="score", mode="max"))


.. _tune-function-checkpointing:

Function API Checkpointing
~~~~~~~~~~~~~~~~~~~~~~~~~~

Many Tune features rely on checkpointing, including the usage of certain Trial Schedulers and fault tolerance.
To use Tune's checkpointing features, you must expose a ``checkpoint_dir`` argument in the function signature,
and call ``tune.checkpoint_dir`` :

.. code-block:: python

        import time
        from ray import tune

        def train_func(config, checkpoint_dir=None):
            start = 0
            if checkpoint_dir:
                with open(os.path.join(checkpoint_dir, "checkpoint")) as f:
                    state = json.loads(f.read())
                    start = state["step"] + 1

            for iter in range(start, 100):
                time.sleep(1)

                with tune.checkpoint_dir(step=step) as checkpoint_dir:
                    path = os.path.join(checkpoint_dir, "checkpoint")
                    with open(path, "w") as f:
                        f.write(json.dumps({"step": start}))

                tune.report(hello="world", ray="tune")

        tune.run(train_func)

.. note:: ``checkpoint_freq`` and ``checkpoint_at_end`` will not work with Function API checkpointing.

In this example, checkpoints will be saved by training iteration to ``local_dir/exp_name/trial_name/checkpoint_<step>``.
You can restore a single trial checkpoint by using ``tune.run(restore=<checkpoint_dir>)``:

.. code-block:: python

        analysis = tune.run(
            train,
            config={
                "max_iter": 5
            },
        ).trials
        last_ckpt = trial.checkpoint.value
        analysis = tune.run(train, config={"max_iter": 10}, restore=last_ckpt)

Tune also may copy or move checkpoints during the course of tuning. For this purpose,
it is important not to depend on absolute paths in the implementation of ``save``.

.. _tune-class-api:

Trainable Class API
-------------------

.. caution:: Do not use ``tune.report`` within a ``Trainable`` class.

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

    analysis = tune.run(
        Trainable,
        stop={"training_iteration": 20},
        config={
            "a": 2,
            "b": 4
        })

    print('best config: ', analysis.get_best_config(metric="score", mode="max"))

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

    tune.run(MyTrainableClass, checkpoint_freq=2)

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
This can be enabled by setting ``checkpoint_freq=<int>`` and ``max_failures=<int>`` to checkpoint trials
every *N* iterations and recover from up to *M* crashes per trial, e.g.:

.. code-block:: python

    tune.run(
        my_trainable,
        checkpoint_freq=10,
        max_failures=5,
    )

**Checkpointing at Termination**: The checkpoint_freq may not coincide with the exact end of an experiment.
If you want a checkpoint to be created at the end of a trial, you can additionally set the ``checkpoint_at_end=True``:

.. code-block:: python
   :emphasize-lines: 5

    tune.run(
        my_trainable,
        checkpoint_freq=10,
        checkpoint_at_end=True,
        max_failures=5,
    )


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
To avoid this, you can do ``tune.run(reuse_actors=True)`` to reuse the same Trainable Python process and
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

    tune.run(
        my_trainable,
        name="my_trainable",
        resources_per_trial=tune.PlacementGroupFactory([
            {"CPU": 1, "GPU": 1},
            {"GPU": 1},
            {"GPU": 1},
            {"GPU": 1},
            {"GPU": 1}
        ])
    )

The ``Trainable`` also provides the ``default_resource_requests`` interface to automatically
declare the ``resources_per_trial`` based on the given configuration.

It is also possible to specify memory (``"memory"``, in bytes) and custom resource requirements.


.. _tune-function-docstring:

tune.report / tune.checkpoint (Function API)
--------------------------------------------

.. autofunction:: ray.tune.report

.. autofunction:: ray.tune.checkpoint_dir

.. autofunction:: ray.tune.get_trial_dir

.. autofunction:: ray.tune.get_trial_name

.. autofunction:: ray.tune.get_trial_id

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


StatusReporter
--------------

.. autoclass:: ray.tune.function_runner.StatusReporter
    :members: __call__, logdir
