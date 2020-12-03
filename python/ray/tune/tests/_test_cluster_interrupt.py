import os
import argparse

from ray.tune.utils._mock_trainable import MyTrainableClass

import os
import time
import ray
from ray import tune
from ray.tune import register_trainable

os.environ["TUNE_GLOBAL_CHECKPOINT_S"] = "0"

parser = argparse.ArgumentParser(
    description="PyTorch Example (FOR TEST ONLY)")
parser.add_argument(
    "--resume", action="store_true", help="Resuming from checkpoint.")
parser.add_argument("--local-dir", help="Checkpoint path")
parser.add_argument(
    "--ray-address",
    help="Address of Ray cluster for seamless distributed execution.")
args = parser.parse_args()

if __name__ == '__main__':
    main()
    ray.init(address=args.ray_address)


    register_trainable("trainable", MyTrainableClass)
    tune.run(
        "trainable",
        name="experiment",
        resume=args.resume,
        verbose=0,
        local_dir=args.local_dir,
        stop=dict(training_iteration=5),
        checkpoint_freq=1,
        max_failures=1
    )