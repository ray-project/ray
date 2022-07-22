import json
import os
import time

import ray
from ray.air import ScalingConfig
from ray.train.examples.horovod.horovod_example import (
    train_func as horovod_torch_train_func,
)
from ray.train.constants import TRAINING_ITERATION
from ray.train.horovod.horovod_trainer import HorovodTrainer

if __name__ == "__main__":
    ray.init(address=os.environ.get("RAY_ADDRESS", "auto"))
    start_time = time.time()

    num_workers = 8
    num_epochs = 10
    trainer = HorovodTrainer(
        horovod_torch_train_func,
        train_loop_config={"num_epochs": num_epochs, "lr": 1e-3},
        scaling_config=ScalingConfig(
            num_workers=num_workers,
            trainer_resources={"CPU": 0},
        ),
    )
    results = trainer.fit()
    result = results.metrics
    assert result[TRAINING_ITERATION] == num_epochs

    loss = list(results.metrics_dataframe["loss"])
    assert len(loss) == num_epochs
    assert loss[-1] < loss[0]

    delta = time.time() - start_time
    with open(os.environ["TEST_OUTPUT_JSON"], "w") as f:
        f.write(json.dumps({"train_time": delta, "success": True}))
