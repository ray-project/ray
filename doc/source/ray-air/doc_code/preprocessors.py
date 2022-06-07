# flake8: noqa


# __preprocessor_setup_start__
import pandas as pd
import ray
from ray.air.preprocessors import MinMaxScaler

# Generate two simple datasets.
dataset = ray.data.range_table(8)
dataset1, dataset2 = dataset.split(2)

print(dataset1.take())
# [{'value': 0}, {'value': 1}, {'value': 2}, {'value': 3}]

print(dataset2.take())
# [{'value': 4}, {'value': 5}, {'value': 6}, {'value': 7}]
# __preprocessor_setup_end__


# __preprocessor_fit_transform_start__
# Fit the preprocessor on dataset1, and transform both dataset1 and dataset2.
preprocessor = MinMaxScaler(["value"])

dataset1_transformed = preprocessor.fit_transform(dataset1)
print(dataset1_transformed.take())
# [{'value': 0.0}, {'value': 0.3333333333333333}, {'value': 0.6666666666666666}, {'value': 1.0}]

dataset2_transformed = preprocessor.transform(dataset2)
print(dataset2_transformed.take())
# [{'value': 1.3333333333333333}, {'value': 1.6666666666666667}, {'value': 2.0}, {'value': 2.3333333333333335}]
# __preprocessor_fit_transform_end__


# __preprocessor_transform_batch_start__
batch = pd.DataFrame({"value": list(range(8, 12))})
batch_transformed = preprocessor.transform_batch(batch)
print(batch_transformed)
#       value
# 0  2.666667
# 1  3.000000
# 2  3.333333
# 3  3.666667
# __preprocessor_transform_batch_end__


# __trainer_start__
import ray

from ray.air.train.integrations.xgboost import XGBoostTrainer
from ray.air.preprocessors import MinMaxScaler

train_dataset = ray.data.from_items([{"x": x, "y": 2 * x} for x in range(0, 32, 3)])
valid_dataset = ray.data.from_items([{"x": x, "y": 2 * x} for x in range(1, 32, 3)])

preprocessor = MinMaxScaler(["x"])

trainer = XGBoostTrainer(
    label_column="y",
    params={"objective": "reg:squarederror"},
    scaling_config={"num_workers": 2},
    datasets={"train": train_dataset, "valid": valid_dataset},
    preprocessor=preprocessor,
)
result = trainer.fit()
# __trainer_end__


# __checkpoint_start__
import os
import ray.cloudpickle as cpickle
from ray.air.constants import PREPROCESSOR_KEY

checkpoint = result.checkpoint
with checkpoint.as_directory() as checkpoint_path:
    path = os.path.join(checkpoint_path, PREPROCESSOR_KEY)
    with open(path, "rb") as f:
        preprocessor = cpickle.load(f)
    print(preprocessor)
# MixMaxScaler(columns=['x'], stats={'min(x)': 0, 'max(x)': 30})
# __checkpoint_end__


# __predictor_start__
from ray.air.batch_predictor import BatchPredictor
from ray.air.predictors.integrations.xgboost import XGBoostPredictor

test_dataset = ray.data.from_items([{"x": x} for x in range(2, 32, 3)])

batch_predictor = BatchPredictor.from_checkpoint(checkpoint, XGBoostPredictor)
predicted_labels = batch_predictor.predict(test_dataset)
print(predicted_labels.to_pandas())
#    predictions
# 0     0.098437
# 1     5.604667
# 2    11.405312
# 3    15.684700
# 4    23.990948
# 5    29.900211
# 6    34.599442
# 7    40.696899
# 8    45.681076
# 9    50.290031
# __predictor_end__


# __chain_start__
import ray
from ray.air.preprocessors import Chain, MinMaxScaler, SimpleImputer

# Generate one simple dataset.
dataset = ray.data.from_items(
    [{"value": 0}, {"value": 1}, {"value": 2}, {"value": 3}, {"value": None}]
)
print(dataset.take())
# [{'value': 0}, {'value': 1}, {'value': 2}, {'value': 3}, {'value': None}]

preprocessor = Chain(SimpleImputer(["value"]), MinMaxScaler(["value"]))

dataset_transformed = preprocessor.fit_transform(dataset)
print(dataset_transformed.take())
# [{'value': 0.0}, {'value': 0.3333333333333333}, {'value': 0.6666666666666666}, {'value': 1.0}, {'value': 0.5}]
# __chain_end__


# __custom_stateless_start__
import ray
from ray.air.preprocessors import BatchMapper

# Generate a simple dataset.
dataset = ray.data.range_table(4)
print(dataset.take())
# [{'value': 0}, {'value': 1}, {'value': 2}, {'value': 3}]

# Create a stateless preprocess that multiplies values by 2.
preprocessor = BatchMapper(lambda df: df * 2)
dataset_transformed = preprocessor.transform(dataset)
print(dataset_transformed.take())
# [{'value': 0}, {'value': 2}, {'value': 4}, {'value': 6}]
# __custom_stateless_end__
