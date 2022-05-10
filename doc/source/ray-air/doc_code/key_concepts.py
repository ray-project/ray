# __air_preprocessors_start__
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
# __air_preprocessors_end__

# __air_trainer_start__
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
# __air_trainer_end__

# __air_trainer_output_start__
print(result.metrics)
print(result.checkpoint)
# __air_trainer_output_end__

# __air_tuner_start__
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
# __air_tuner_end__

# __air_batch_predictor_start__

batch_predictor = BatchPredictor.from_checkpoint(
    result.checkpoint, XGBoostPredictor
)

predicted_labels = (
    batch_predictor.predict(test_dataset)
    .map_batches(lambda df: (df > 0.5).astype(int), batch_format="pandas")
    .to_pandas(limit=float("inf"))
)

# __air_batch_predictor_end__
