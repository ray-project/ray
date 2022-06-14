from ray import train
from ray.air import RunConfig
from ray.train.torch import TorchTrainer
from ray.tune.integration.mlflow import MLflowLoggerCallback
from ray.tune.logger import TBXLoggerCallback


def train_func():
    for i in range(3):
        train.report(epoch=i)


trainer = TorchTrainer(
    train_func,
    scaling_config={"num_workers": 2},
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

# Print the latest run directory and keep note of it.
# For example: /home/ubuntu/ray_results/TorchTrainer_2022-06-13_20-31-06\
# /TorchTrainer_c02c7_00000_0_2022-06-13_20-31-06
print("Run directory:", result.logdir)

# How to visualize the logs

# Navigate to the run directory of the trainer.
# For example `cd /home/ubuntu/ray_results/TorchTrainer_2022-06-13_20-31-06\
# /TorchTrainer_c02c7_00000_0_2022-06-13_20-31-06`
# $ cd <TRAINER_RUN_DIR>
#
# # View the MLflow UI.
# $ mlflow ui
#
# # View the tensorboard UI.
# $ tensorboard --logdir .
