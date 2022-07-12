import os
import argparse
from ray import tune
from ray.tune import run
from ray.tune.utils.mock_trainable import MyTrainableClass
from ray.tune.search import ConcurrencyLimiter

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PyTorch Example (FOR TEST ONLY)")
    parser.add_argument(
        "--resume", action="store_true", help="Resuming from checkpoint."
    )
    parser.add_argument("--local-dir", help="Checkpoint path")
    parser.add_argument("--searcher", help="Search Algorithm")
    parser.add_argument(
        "--ray-address",
        help="Address of Ray cluster for seamless distributed execution.",
    )
    args = parser.parse_args()

    space = {
        "width": tune.uniform(0.1, 20),
        "height": tune.uniform(-100, 100),
        "activation": "relu",
    }
    current_best_params = [
        {
            "width": 1,
            "height": 2,
            # "activation": "relu"  # Activation will be relu
        },
        {
            "width": 4,
            "height": 2,
            # "activation": "tanh"  # Activation will be tanh
        },
    ]
    algo = tune.create_searcher(args.searcher, points_to_evaluate=current_best_params)
    algo = ConcurrencyLimiter(algo, max_concurrent=1)
    from ray.tune import register_trainable

    register_trainable("trainable", MyTrainableClass)
    os.environ["TUNE_GLOBAL_CHECKPOINT_S"] = "0"
    run(
        "trainable",
        search_alg=algo,
        resume=args.resume,
        verbose=0,
        num_samples=20,
        fail_fast=True,
        metric="episode_reward_mean",
        mode="max",
        config=space,
        stop={"training_iteration": 2},
        local_dir=args.local_dir,
        name="experiment",
    )
