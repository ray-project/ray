import ray
from ray.data.preprocessors import StandardScaler
from ray.train.batch_predictor import BatchPredictor
from ray.train.xgboost import XGBoostTrainer, XGBoostPredictor
from ray.air.config import ScalingConfig

# Split data into train and validation.
dataset = ray.data.read_csv("s3://anonymous@air-example-data/breast_cancer.csv")
train_dataset, valid_dataset = dataset.train_test_split(test_size=0.3)
test_dataset = valid_dataset.drop_columns(["target"])

columns_to_scale = ["mean radius", "mean texture"]
preprocessor = StandardScaler(columns=columns_to_scale)

trainer = XGBoostTrainer(
    label_column="target",
    num_boost_round=20,
    scaling_config=ScalingConfig(num_workers=2),
    params={
        "objective": "binary:logistic",
        "eval_metric": ["logloss", "error"],
    },
    datasets={"train": train_dataset},
    preprocessor=preprocessor,
)
result = trainer.fit()

# You can also create a checkpoint from a trained model using
# `XGBoostCheckpoint.from_model`.

# import xgboost as xgb
# from ray.train.xgboost import XGBoostCheckpoint
# model = xgb.Booster()
# model.load_model(...)
# checkpoint = XGBoostCheckpoint.from_model(model, path=".")
checkpoint = result.checkpoint

batch_predictor = BatchPredictor.from_checkpoint(checkpoint, XGBoostPredictor)

predicted_probabilities = batch_predictor.predict(test_dataset)
predicted_probabilities.show()
