from ray import train
from ray.train import Trainer
from ray.train.callbacks import MLflowLoggerCallback, TBXLoggerCallback


def train_func():
    for i in range(3):
        train.report(epoch=i)


trainer = Trainer(backend="torch", num_workers=2)
trainer.start()

# Run the training function, logging all the intermediate results
# to MLflow and Tensorboard.
result = trainer.run(
    train_func,
    callbacks=[
        MLflowLoggerCallback(experiment_name="train_experiment"),
        TBXLoggerCallback(),
    ],
)

# Print the latest run directory and keep note of it.
# For example: /home/ray_results/train_2021-09-01_12-00-00/run_001
print("Run directory:", trainer.latest_run_dir)

trainer.shutdown()

# How to visualize the logs

# Navigate to the run directory of the trainer.
# For example `cd /home/ray_results/train_2021-09-01_12-00-00/run_001`
# $ cd <TRAINER_RUN_DIR>
#
# # View the MLflow UI.
# $ mlflow ui
#
# # View the tensorboard UI.
# $ tensorboard --logdir .
