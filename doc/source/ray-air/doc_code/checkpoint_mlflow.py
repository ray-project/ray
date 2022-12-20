# flake8: noqa
# isort: skip_file

# __mlflow_checkpoint_start__
from ray.air.checkpoint import Checkpoint
from sklearn.ensemble import RandomForestClassifier
import mlflow.sklearn

# Create an sklearn classifier
clf = RandomForestClassifier(max_depth=7, random_state=0)
# ... e.g. train model with clf.fit()
# Save model using MLflow
mlflow.sklearn.save_model(clf, "model_directory")

# Create checkpoint object from path
checkpoint = Checkpoint.from_directory("model_directory")

# Write it to some other directory
checkpoint.to_directory("other_directory")
# You can also use `checkpoint.to_uri/from_uri` to
# read from/write to cloud storage

# We can now use MLflow to re-load the model
clf = mlflow.sklearn.load_model("other_directory")

# It is guaranteed that the original data was recovered
assert isinstance(clf, RandomForestClassifier)
# __mlflow_checkpoint_end__
