import json
import os
import time

import ray
from ray.train import Trainer
from ray.train.examples.horovod.horovod_example import (
    train_func as horovod_torch_train_func,
)

if __name__ == "__main__":
    ray.init(address=os.environ.get("RAY_ADDRESS", "auto"))
    start_time = time.time()

    num_workers = 8
    num_epochs = 10
    trainer = Trainer("horovod", num_workers)
    trainer.start()
    results = trainer.run(
        horovod_torch_train_func, config={"num_epochs": num_epochs, "lr": 1e-3}
    )
    trainer.shutdown()

    assert len(results) == num_workers
    for worker_result in results:
        assert len(worker_result) == num_epochs
        assert worker_result[num_epochs - 1] < worker_result[0]

    delta = time.time() - start_time
    with open(os.environ["TEST_OUTPUT_JSON"], "w") as f:
        f.write(json.dumps({"train_time": delta, "success": True}))
