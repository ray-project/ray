.. _air-tuner:

Hyperparameter Tuning
=====================
The Ray AIR Tuner API is the recommended way to launch hyperparameter tuning jobs in Ray AIR.

Suppose that you already have a Ray AIR trainer at hand.
You have fitted it and obtained a model and training results.

Next, you may want to see if you can change some hyperparameters to further improve the model.

The Tuner API allows you to plug in your existing Trainer. Additionally you can specify the search space
and search algorithms for your hyperparameter tuning job.

In this guide, we will show you how to do this.

.. note::
   The Tuner API can also be used to launch hyperparameter tuning without using Ray AIR Trainers - see
   :ref:`the Ray Tune documentation <tune-main>` for more guides examples.

Basic usage
-----------

Suppose that you already have your Ray AIR Trainer at hand.
You have fitted it and obtained a model and training results.
Below, we demonstrate how you can plug in your existing Trainer into a Tuner.

.. literalinclude:: doc_code/tuner.py
    :language: python
    :start-after: __basic_start__
    :end-before: __basic_end__


Defining the search space
-------------------------

The Tuner API takes in a `param_space` argument where you can define the search space
from which hyperparameter configurations will be sampled.

Generally, you can search over most arguments and configurations provided by Ray AIR (and your own options).
This includes Ray Datasets, Preprocessors, the distributed training configuration (ScalingConfig)
and general hyperparameters.

Examples for common parameters (depending on the Trainer and model) you can tune are:

- The training batch size
- The learning rate for SGD-based training (e.g. image classification)
- The maximum depth for tree-based models (e.g. XGBoost)

The following shows some example code on how to specify the ``param_space``.

.. tabbed:: XGBoost

    .. literalinclude:: doc_code/tuner.py
        :language: python
        :start-after: __xgboost_start__
        :end-before: __xgboost_end__

.. tabbed:: Deep Learning

    .. literalinclude:: doc_code/tuner.py
        :language: python
        :start-after: __torch_start__
        :end-before: __torch_end__


As you can see in the above example, the Tuner API allows you to specify a search space over most parameters.
The parameters will be passed to the Trainer.

A couple gotchas about the behavior of merging parameters with Trainer:
- Dictionaries will be merged
- Scaling configs will be merged
- Duplicate items (which are both defined in the Trainer and Tuner) will be overwritten by the ones defined in the Tuner

The Tuner API also offers the possibility to apply different data preprocessing steps, as shown in the following snippet.

.. literalinclude:: doc_code/tuner.py
    :language: python
    :start-after: __tune_preprocess_start__
    :end-before: __tune_preprocess_end__

The Tuner API allows you to even tune the train/validation datasets:

.. literalinclude:: doc_code/tuner.py
    :language: python
    :start-after: __tune_dataset_start__
    :end-before: __tune_dataset_end__

In general, all the arguments accepted by your :ref:`Trainer <air-trainer-ref>` can be tuned.
The main exclusion here are all arguments of the :class:`ray.air.config.RunConfig`, which are inherently un-tunable.
Another exclusion is the :class:`ray.tune.tune_config.TuneConfig`, which defines the tuning behavior itself - the settings
apply to all trials and thus can't be tuned.


Specify TuneConfig
------------------
This config contains tuning specific settings, including the tuning algorithm to use, the metric and mode to rank results etc.

The following we showcase some common configuration of :class:`ray.tune.tune_config.TuneConfig`.

.. literalinclude:: doc_code/tuner.py
    :language: python
    :start-after: __tune_config_start__
    :end-before: __tune_config_end__

See the :class:`Tuner <ray.tune.tuner.Tuner>` and the :class:`TuneConfig API reference <ray.tune.tune_config.TuneConfig>` for more details.


Specify RunConfig
-----------------
This config contains framework's runtime configurations that are more generic than tuning specific settings.
This may include failure/retry configurations, verbosity levels, the name of the experiment, its logging directory,
checkpoint configuration as well as its syncing configuration.

The following we showcase some common configuration of :class:`ray.air.config.RunConfig`.

.. literalinclude:: doc_code/tuner.py
    :language: python
    :start-after: __run_config_start__
    :end-before: __run_config_end__

See the :class:`RunConfig API reference <ray.air.config.RunConfig>` for more details.

Putting everything together
---------------------------
We can now construct a Tuner and call ``Tuner.fit`` on it!

.. literalinclude:: doc_code/tuner.py
    :language: python
    :start-after: __tuner_start__
    :end-before: __tuner_end__

.. note::
    ``num_samples=2`` here will be applied to the whole suite of grid search. In other words,
    we will generate 4 trials.

For a more end-to-end example, checkout our :doc:`/ray-air/examples/analyze_tuning_results` guide.

Inspect result
--------------
``Tuner.fit()`` generates a result grid, which you can inspect in the following way:

.. literalinclude:: doc_code/tuner.py
    :language: python
    :start-after: __result_grid_inspection_start__
    :end-before: __result_grid_inspection_end__

Restoring and resuming
----------------------
Coming soon!





