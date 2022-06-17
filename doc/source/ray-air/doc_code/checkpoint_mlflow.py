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

# Convert into dictionary
checkpoint_dict = checkpoint.to_dict()

# This dict can then be passed around, e.g. to a different function

# At some other location, recover checkpoint object from dict
checkpoint = Checkpoint.from_dict(checkpoint_dict)

# Convert into a directory again
checkpoint.to_directory("other_directory")

# We can now use MLflow to re-load the model
clf = mlflow.sklearn.load_model("other_directory")

# It is guaranteed that the original data was recovered
assert isinstance(clf, RandomForestClassifier)
# __mlflow_checkpoint_end__