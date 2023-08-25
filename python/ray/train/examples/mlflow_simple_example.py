from pathlib import Path
from ray import train
from ray.train import ScalingConfig, RunConfig
from ray.train.torch import TorchTrainer
from ray.air.integrations.mlflow import MLflowLoggerCallback
from ray.tune.logger import TBXLoggerCallback


def train_func():
    for i in range(3):
        train.report(dict(epoch=i))


trainer = TorchTrainer(
    train_func,
    scaling_config=ScalingConfig(num_workers=2),
    run_config=RunConfig(
        callbacks=[
            MLflowLoggerCallback(experiment_name="train_experiment"),
            TBXLoggerCallback(),
        ],
    ),
)

# Run the training function, logging all the intermediate results
# to MLflow and Tensorboard.
result = trainer.fit()

# For MLFLow logs:

# MLFlow logs will by default be saved in an `mlflow` directory
# in the current working directory.

# $ cd mlflow
# # View the MLflow UI.
# $ mlflow ui

# You can change the directory by setting the `tracking_uri` argument
# in `MLflowLoggerCallback`.

# For TensorBoard logs:

# Print the latest run directory and keep note of it.
# For example: /home/ubuntu/ray_results/TorchTrainer_2022-06-13_20-31-06
print("Run directory:", Path(result.path).parent)  # TensorBoard is saved in parent dir

# How to visualize the logs

# Navigate to the run directory of the trainer.
# For example `cd /home/ubuntu/ray_results/TorchTrainer_2022-06-13_20-31-06`
# $ cd <TRAINER_RUN_DIR>
#
# # View the tensorboard UI.
# $ tensorboard --logdir .
