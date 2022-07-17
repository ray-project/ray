import ray
from ray.data.preprocessors import StandardScaler
from ray.air import train_test_split
from ray.train.batch_predictor import BatchPredictor
from ray.train.xgboost import XGBoostTrainer, XGBoostPredictor

# Split data into train and validation.
dataset = ray.data.read_csv("s3://anonymous@air-example-data/breast_cancer.csv")
train_dataset, valid_dataset = train_test_split(dataset, test_size=0.3)
test_dataset = valid_dataset.drop_columns(["target"])

columns_to_scale = ["mean radius", "mean texture"]
preprocessor = StandardScaler(columns=columns_to_scale)

trainer = XGBoostTrainer(
    label_column="target",
    num_boost_round=20,
    scaling_config={"num_workers": 2},
    params={
        "objective": "binary:logistic",
        "eval_metric": ["logloss", "error"],
    },
    datasets={"train": train_dataset},
    preprocessor=preprocessor,
)
result = trainer.fit()

# You can also create a checkpoint from a trained model using `to_air_checkpoint`.

# import xgboost as xgb
# from ray.train.xgboost import to_air_checkpoint
# model = xgb.Booster()
# model.load_model(...)
# checkpoint = to_air_checkpoint(".", model)
checkpoint = result.checkpoint

batch_predictor = BatchPredictor.from_checkpoint(checkpoint, XGBoostPredictor)

predicted_probabilities = batch_predictor.predict(test_dataset)
predicted_probabilities.show()
