.. _air-tuner:

Hyperparameter Tuning
=====================
Tuner API is the recommended way of launching hyperparameter tuning jobs with Ray Tune.
Suppose that you already have an AIR trainer at hand. You have fitted it and obtained some model and results.
Now you want to see, by changing some hyperparamters, is it possible to further improve the model.
Tuner API allows you to seamlessly plug in your existing Trainer and additionally
specify the search space and searching algorithms for your hyperparamter tuning job.
The following sections will walk you through that.

Specify Search Space
--------------------

Tuner takes in a `param_space` argument where user can specify the search space to run Hyperparameter tuning on.
Depending on the nature of the Trainer and the model, some common hyperparameters may include "batch_size", "lr"
for deep learning or "eta", "max_depth" for Tree-based models like XGBoost or LightGBM.
The following shows some example code on how to specify `param_space`.

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

As you can see in the above example, Tuner API allows you to choose different ScalingConfig for different trials.

Tuner API also offers the possibility to apply different data preprocessing steps, as shown in the following snippet.

.. literalinclude:: doc_code/tuner.py
    :language: python
    :start-after: __tune_preprocess_start__
    :end-before: __tune_preprocess_end__

Tuner API allows you to even tune the train/validation datasets! Examples coming soon!

In general, all the arguments accepted by your :ref:`Trainer <air-trainer-ref>` can be tunable.


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





