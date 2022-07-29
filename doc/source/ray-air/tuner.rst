.. _air-tuner:

Hyperparameter Tuning
=====================
The Ray AIR Tuner API is the recommended way to launch hyperparameter tuning jobs with Ray Tune.

Suppose that you already have an AIR trainer at hand. You have fitted it and obtained some model and results.

Next, you may want to see if you can change some hyperparameters to further improve the model.

The Tuner API allows you to plug in your existing Trainer. Additionally you can specify the search space
and searching algorithms for your hyperparameter tuning job.

In this guide, we will show you how to do this.

.. note::
   The Tuner API can also be used to launch hyperparameter tuning without using Trainers - see
   [todo ref] for an example.

Defining the search space
-------------------------

The Tuner API takes in a `param_space` argument where you can define the search space that Ray Tune will
use to try out hyperparameter configurations.

Generally, you can search over most arguments and configurations provided by Ray AIR (and your own options).
This includes Ray Datasets, Preprocessors, the distributed training configuration (ScalingConfig)
and general hyperparameters.

The main exclusion here are all arguments of the ``RunConfig``, which are inherently un-tunable.

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


As you can see in the above example, the Tuner API allows you to choose a different ScalingConfig for different trials.
The Tuner API also offers the possibility to apply different data preprocessing steps, as shown in the following snippet.

.. literalinclude:: doc_code/tuner.py
    :language: python
    :start-after: __tune_preprocess_start__
    :end-before: __tune_preprocess_end__

TThe uner API allows you to even tune the train/validation datasets! Examples coming soon!

In general, all the arguments accepted by your :ref:`Trainer <air-trainer-ref>` (except for the ``RunConfig``)
can be tuned.


Specify TuneConfig
------------------
This config contains tuning specific settings, including the tuning algorithm to use, the metric and mode to rank results etc.
See :ref:`Tuner <air-tuner-ref>` for details.


Specify RunConfig
-----------------
This config contains framework's runtime configurations that are more generic than tuning specific settings.
This may include failure/retry configurations, verbosity levels, the name of the experiment, its logging directory,
checkpoint configuration as well as its syncing configuration.

Putting everything together
---------------------------
We can now construct a Tuner and call ``Tuner.fit`` on it!

.. literalinclude:: doc_code/tuner.py
    :language: python
    :start-after: __tuner_start__
    :end-before: __tuner_end__

.. note::
    ``num_samples = 2`` here will be applied to the whole suite of grid search. In other words,
    we will generate 4 trials.

For a more end-to-end example, checkout our :ref:`ray-air/examples/analyze_tuning_results`: guide.

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





