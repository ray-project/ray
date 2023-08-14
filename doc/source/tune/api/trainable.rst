.. _trainable-docs:

.. TODO: these "basic" sections before the actual API docs start don't really belong here. Then again, the function
    API does not really have a signature to just describe.
.. TODO: Reusing actors and advanced resources allocation seem ill-placed.

Training in Tune (tune.Trainable, session.report)
=================================================

Training can be done with either a **Function API** (:ref:`session.report <tune-function-docstring>`) or
**Class API** (:ref:`tune.Trainable <tune-trainable-docstring>`).

For the sake of example, let's maximize this objective function:

.. literalinclude:: /tune/doc_code/trainable.py
    :language: python
    :start-after: __example_objective_start__
    :end-before: __example_objective_end__

.. _tune-function-api:

Function Trainable API
----------------------

Use the Function API to define a custom training function that Tune runs in Ray actor processes. Each trial is placed
into a Ray actor process and runs in parallel.

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

Note that Ray Tune outputs extra values in addition to the user reported metrics,
such as ``iterations_since_restore``. See :ref:`tune-autofilled-metrics` for an explanation of these values.

See how to configure checkpointing for a function trainable :ref:`here <tune-function-trainable-checkpointing>`.

.. _tune-class-api:

Class Trainable API
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

See how to configure checkpoint for class trainable :ref:`here <tune-class-trainable-checkpointing>`.


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
then you should use :func:`tune.with_resources <ray.tune.with_resources>` like this:

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

.. currentmodule:: ray

.. autosummary::

    air.session.report
    air.session.get_checkpoint
    air.session.get_trial_name
    air.session.get_trial_id
    air.session.get_trial_resources
    air.session.get_trial_dir

.. _tune-trainable-docstring:

Trainable (Class API)
---------------------

Constructor
~~~~~~~~~~~

.. autosummary::
    :toctree: doc/

    ~tune.Trainable


Trainable Methods to Implement
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autosummary::
    :toctree: doc/

    ~tune.Trainable.setup
    ~tune.Trainable.save_checkpoint
    ~tune.Trainable.load_checkpoint
    ~tune.Trainable.step
    ~tune.Trainable.reset_config
    ~tune.Trainable.cleanup
    ~tune.Trainable.default_resource_request


.. _tune-util-ref:

Tune Trainable Utilities
-------------------------

Tune Data Ingestion Utilities
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autosummary::
    :toctree: doc/

    tune.with_parameters


Tune Resource Assignment Utilities
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autosummary::
    :toctree: doc/

    tune.with_resources
    ~tune.execution.placement_groups.PlacementGroupFactory
    tune.utils.wait_for_gpu


Tune Trainable Debugging Utilities
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autosummary::
    :toctree: doc/

    tune.utils.diagnose_serialization
    tune.utils.validate_save_restore
