import argparse
import os

# set up the background observer to write checkpoint ids to a file periodically.
import ray
from ray.tune.utils.release_test_util import get_and_run_exp_folder_observer


def parse_script_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--experiment-dir", type=str)
    parser.add_argument("--trainable-name", type=str)
    parser.add_argument("--output-path", type=str)
    return parser.parse_known_args()


def main():
    """Start the observer to experiment folder."""
    args, _ = parse_script_args()
    ray.init(address="auto")
    get_and_run_exp_folder_observer(
        experiment_dir=os.path.expanduser("~/ray_results/torch_pbt_failure"),
        trainable_name="TorchTrainer",
        output_path="/tmp/ckpt_ids.txt",
    )
    print("Successfully deployed an experiment folder observer.")


main()
