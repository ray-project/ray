.. _trainable-docs:

.. TODO: these "basic" sections before the actual API docs start don't really belong here. Then again, the function
    API does not really have a signature to just describe.
.. TODO: Reusing actors and advanced resources allocation seem ill-placed.

Training in Tune (tune.Trainable, session.report)
=================================================

Training can be done with either a **Function API** (:ref:`session.report <tune-function-docstring>`) or **Class API** (:ref:`tune.Trainable <tune-trainable-docstring>`).

For the sake of example, let's maximize this objective function:

.. literalinclude:: /tune/doc_code/trainable.py
    :language: python
    :start-after: __example_objective_start__
    :end-before: __example_objective_end__

.. _tune-function-api:

Tune's Function API
-------------------

The Function API allows you to define a custom training function that Tune will run in parallel Ray actor processes,
one for each Tune trial.

The ``config`` argument in the function is a dictionary populated automatically by Ray Tune and corresponding to
the hyperparameters selected for the trial from the :ref:`search space <tune-key-concepts-search-spaces>`.

With the Function API, you can report intermediate metrics by simply calling ``session.report`` within the function.

.. literalinclude:: /tune/doc_code/trainable.py
    :language: python
    :start-after: __function_api_report_intermediate_metrics_start__
    :end-before: __function_api_report_intermediate_metrics_end__

.. tip:: Do not use ``session.report`` within a ``Trainable`` class.

In the previous example, we reported on every step, but this metric reporting frequency
is configurable. For example, we could also report only a single time at the end with the final score:

.. literalinclude:: /tune/doc_code/trainable.py
    :language: python
    :start-after: __function_api_report_final_metrics_start__
    :end-before: __function_api_report_final_metrics_end__

It's also possible to return a final set of metrics to Tune by returning them from your function:

.. literalinclude:: /tune/doc_code/trainable.py
    :language: python
    :start-after: __function_api_return_final_metrics_start__
    :end-before: __function_api_return_final_metrics_end__

You'll notice that Ray Tune will output extra values in addition to the user reported metrics,
such as ``iterations_since_restore``. See :ref:`tune-autofilled-metrics` for an explanation/glossary of these values.

.. _tune-function-checkpointing:

Function API Checkpointing
~~~~~~~~~~~~~~~~~~~~~~~~~~

Many Tune features rely on checkpointing, including the usage of certain Trial Schedulers and fault tolerance.
You can save and load checkpoints in Ray Tune in the following manner:

.. literalinclude:: /tune/doc_code/trainable.py
    :language: python
    :start-after: __function_api_checkpointing_start__
    :end-before: __function_api_checkpointing_end__

.. note:: ``checkpoint_frequency`` and ``checkpoint_at_end`` will not work with Function API checkpointing.

In this example, checkpoints will be saved by training iteration to ``<local_dir>/<exp_name>/trial_name/checkpoint_<step>``.

Tune also may copy or move checkpoints during the course of tuning. For this purpose,
it is important not to depend on absolute paths in the implementation of ``save``.

See :ref:`here for more information on creating checkpoints <air-checkpoint-ref>`.
If using framework-specific trainers from Ray AIR, see :ref:`here <air-trainer-ref>` for
references to framework-specific checkpoints such as `TensorflowCheckpoint`.

.. _tune-class-api:

Tune's Trainable Class API
--------------------------

.. caution:: Do not use ``session.report`` within a ``Trainable`` class.

The Trainable **class API** will require users to subclass ``ray.tune.Trainable``. Here's a naive example of this API:

.. literalinclude:: /tune/doc_code/trainable.py
    :language: python
    :start-after: __class_api_example_start__
    :end-before: __class_api_example_end__

As a subclass of ``tune.Trainable``, Tune will create a ``Trainable`` object on a
separate process (using the :ref:`Ray Actor API <actor-guide>`).

  1. ``setup`` function is invoked once training starts.
  2. ``step`` is invoked **multiple times**.
     Each time, the Trainable object executes one logical iteration of training in the tuning process,
     which may include one or more iterations of actual training.
  3. ``cleanup`` is invoked when training is finished.

The ``config`` argument in the ``setup`` method is a dictionary populated automatically by Tune and corresponding to
the hyperparameters selected for the trial from the :ref:`search space <tune-key-concepts-search-spaces>`.

.. tip:: As a rule of thumb, the execution time of ``step`` should be large enough to avoid overheads
    (i.e. more than a few seconds), but short enough to report progress periodically (i.e. at most a few minutes).

You'll notice that Ray Tune will output extra values in addition to the user reported metrics,
such as ``iterations_since_restore``.
See :ref:`tune-autofilled-metrics` for an explanation/glossary of these values.

.. _tune-trainable-save-restore:

Class API Checkpointing
~~~~~~~~~~~~~~~~~~~~~~~

You can also implement checkpoint/restore using the Trainable Class API:

.. literalinclude:: /tune/doc_code/trainable.py
    :language: python
    :start-after: __class_api_checkpointing_start__
    :end-before: __class_api_checkpointing_end__

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



Advanced: Reusing Actors in Tune
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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


Comparing Tune's Function API and Class API
-------------------------------------------

Here are a few key concepts and what they look like for the Function and Class API's.

======================= =============================================== ==============================================
Concept                 Function API                                    Class API
======================= =============================================== ==============================================
Training Iteration      Increments on each `session.report` call        Increments on each `Trainable.step` call
Report  metrics         `session.report(metrics)`                       Return metrics from `Trainable.step`
Saving a checkpoint     `session.report(..., checkpoint=checkpoint)`    `Trainable.save_checkpoint`
Loading a checkpoint    `session.get_checkpoint()`                      `Trainable.load_checkpoint`
Accessing config        Passed as an argument `def train_func(config):` Passed through `Trainable.setup`
======================= =============================================== ==============================================


Advanced Resource Allocation
----------------------------

Trainables can themselves be distributed. If your trainable function / class creates further Ray actors or tasks
that also consume CPU / GPU resources, you will want to add more bundles to the :class:`PlacementGroupFactory`
to reserve extra resource slots.
For example, if a trainable class requires 1 GPU itself, but also launches 4 actors, each using another GPU,
then you should use :ref:`tune-with-resources` like this:

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

.. autofunction:: ray.air.session.get_trial_dir
    :noindex:

.. _tune-trainable-docstring:

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

.. _tune-with-resources:

tune.with_resources
--------------------

.. autofunction:: ray.tune.with_resources