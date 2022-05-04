AIR Key Concepts
================

Here, we cover the main concepts that AIR is consisted of.

.. contents::
    :local:


Preprocessors
-------------

Preprocessors are primitives that can be used to transform input data into features.

A preprocessor can be fitted during Training, and applied at runtime in both Training and Serving on data batches in the same way.

Preprocessors operate on :ref:`Ray Datasets <datasets>`.

.. code-block:: python

    from ray.ml.preprocessors import *

    col_a = [-1, -1, 1, 1]
    col_b = [1, 1, 1, None]
    col_c = ["sunday", "monday", "tuesday", "tuesday"]
    in_df = pd.DataFrame.from_dict({"A": col_a, "B": col_b, "C": col_c})
    ds = ray.data.from_pandas(in_df)

    imputer = SimpleImputer(["B"])
    scaler = StandardScaler(["A", "B"])
    encoder = LabelEncoder("C")
    chain = Chain(scaler, imputer, encoder)

    # Fit data.
    chain.fit(ds)
    assert imputer.stats_ == {
        "mean(B)": 0.0,
    }

Trainers
--------

Trainers are wrapper classes around third-party training frameworks like XGBoost and Pytorch. They are built to help integrate with core Ray actors (for distribution), Tune, and Datasets.

See the documentation on :ref:`Trainers <air-trainer-ref>`.

.. code-block:: python

    from ray.ml.train.integrations.xgboost import XGBoostTrainer

    # XGBoost specific params
    params = {
        "tree_method": "approx",
        "objective": "binary:logistic",
        "eval_metric": ["logloss", "error"],
        "max_depth": 2,
    }

    trainer = XGBoostTrainer(
        scaling_config={
            "num_workers": num_workers,
            "use_gpu": use_gpu,
        },
        label_column="target",
        params=params,
        datasets={"train": train_dataset, "valid": valid_dataset},
        preprocessor=preprocessor,
        num_boost_round=100,
    )

    result = trainer.fit()


Trainer objects will produce a :ref:`Results <air-results-ref>` object after calling ``.fit()``.  These objects will contain training metrics as long as checkpoints to retrieve the best model.

.. code-block:: python

    print(result.metrics)
    print(result.checkpoint)


Tuner
-----

:ref:`Tuners <air-tuner-ref>` offer scalable hyperparameter tuning as part of :ref:`Ray Tune <tune-main>`.

Tuners can work seamlessly with any Trainer but also can support arbitrary training functions.

.. code-block:: python

    from ray.tune import Tuner, TuneConfig

    tuner = Tuner(
        trainer,
        param_space={
            "params": {
                "max_depth": tune.randint(1, 9)
            }
        },
        tune_config=TuneConfig(num_samples=20, metric="loss", mode="min"),
    )
    result_grid = tuner.fit()
    best_result = result_grid.get_best_result()
    print(best_result)


Batch Predictor
---------------

You can take a trained model and do batch inference using the BatchPredictor object.


.. code-block:: python

    batch_predictor = BatchPredictor.from_checkpoint(
        result.checkpoint, XGBoostPredictor
    )

    predicted_labels = (
        batch_predictor.predict(test_dataset)
        .map_batches(lambda df: (df > 0.5).astype(int), batch_format="pandas")
        .to_pandas(limit=float("inf"))
    )


Online Inference
----------------

TODO
