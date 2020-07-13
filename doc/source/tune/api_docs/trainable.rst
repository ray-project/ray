.. _trainable-docs:

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

Here is a simple example of using the function API. You can report intermediate metrics by simply calling ``tune.report`` within the provided function.

.. code-block:: python

    def trainable(config):
        # config (dict): A dict of hyperparameters.

        for x in range(20):
            intermediate_score = objective(x, config["a"], config["b"])

            tune.report(value=intermediate_score)  # This sends the score to Tune.

    analysis = tune.run(
        trainable,
        config={"a": 2, "b": 4}
    )

    print("best config: ", analysis.get_best_config(metric="score", mode="max"))

.. tip:: Do not use ``tune.report`` within a ``Trainable`` class.

Tune will run this function on a separate thread in a Ray actor process.


Function API Checkpointing
~~~~~~~~~~~~~~~~~~~~~~~~~~

Many Tune features rely on checkpointing, including the usage of certain Trial Schedulers and fault tolerance. To use Tune's checkpointing features, you must expose a ``checkpoint`` argument in the function signature, and call ``tune.make_checkpoint_dir`` and ``tune.save_checkpoint``:

.. code-block:: python

        import time
        from ray import tune

        def train_func(config, checkpoint=None):
            start = 0
            if checkpoint:
                with open(checkpoint) as f:
                    state = json.loads(f.read())
                    start = state["step"] + 1

            for iter in range(start, 100):
                time.sleep(1)

                #
                checkpoint_dir = tune.make_checkpoint_dir(step=step)
                path = os.path.join(checkpoint_dir, "checkpoint")
                with open(path, "w") as f:
                    f.write(json.dumps({"step": start}))
                tune.save_checkpoint(path)

                tune.report(hello="world", ray="tune")

        tune.run(train_func)

In this example, checkpoints will be saved by training iteration to ``local_dir/exp_name/trial_name/checkpoint_<step>``. You can restore a single trial checkpoint by using ``tune.run(restore=<checkpoint_dir>)``:

.. code-block:: python

        analysis = tune.run(
            train,
            config={
                "max_iter": 5
            },
        ).trials
        last_ckpt = trial.checkpoint.value
        analysis = tune.run(train, config={"max_iter": 10}, restore=last_ckpt)

Tune also may copy or move checkpoints during the course of tuning. For this purpose, it is important not to depend on absolute paths in the implementation of ``save``.

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

As a subclass of ``tune.Trainable``, Tune will create a ``Trainable`` object on a separate process (using the :ref:`Ray Actor API <actor-guide>`).

  1. ``setup`` function is invoked once training starts.
  2. ``step`` is invoked **multiple times**. Each time, the Trainable object executes one logical iteration of training in the tuning process, which may include one or more iterations of actual training.
  3. ``cleanup`` is invoked when training is finished.

.. tip:: As a rule of thumb, the execution time of ``step`` should be large enough to avoid overheads (i.e. more than a few seconds), but short enough to report progress periodically (i.e. at most a few minutes).


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

**Manual Checkpointing**: A custom Trainable can manually trigger checkpointing by returning ``should_checkpoint: True`` (or ``tune.result.SHOULD_CHECKPOINT: True``) in the result dictionary of `step`. This can be especially helpful in spot instances:

.. code-block:: python

    def step(self):
        # training code
        result = {"mean_accuracy": accuracy}
        if detect_instance_preemption():
            result.update(should_checkpoint=True)
        return result


**Periodic Checkpointing**: periodic checkpointing can be used to provide fault-tolerance for experiments. This can be enabled by setting ``checkpoint_freq=<int>`` and ``max_failures=<int>`` to checkpoint trials every *N* iterations and recover from up to *M* crashes per trial, e.g.:

.. code-block:: python

    tune.run(
        my_trainable,
        checkpoint_freq=10,
        max_failures=5,
    )

**Checkpointing at Termination**: The checkpoint_freq may not coincide with the exact end of an experiment. If you want a checkpoint to be created at the end
of a trial, you can additionally set the ``checkpoint_at_end=True``:

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

Your Trainable can often take a long time to start. To avoid this, you can do ``tune.run(reuse_actors=True)`` to reuse the same Trainable Python process and object for multiple hyperparameters.

This requires you to implement ``Trainable.reset_config``, which provides a new set of hyperparameters. It is up to the user to correctly update the hyperparameters of your trainable.

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



.. _tune-function-docstring:

tune.report / tune.checkpoint (Function API)
--------------------------------------------

.. autofunction:: ray.tune.report

.. autofunction:: ray.tune.make_checkpoint_dir

.. autofunction:: ray.tune.save_checkpoint

.. autofunction:: ray.tune.get_trial_dir

.. autofunction:: ray.tune.get_trial_name

.. autofunction:: ray.tune.get_trial_id

tune.Trainable (Class API)
--------------------------


.. autoclass:: ray.tune.Trainable
    :member-order: groupwise
    :private-members:
    :members:

tune.DurableTrainable
---------------------

.. autoclass:: ray.tune.DurableTrainable


StatusReporter
--------------

.. autoclass:: ray.tune.function_runner.StatusReporter
    :members: __call__, logdir
