#!/usr/bin/env python

# __tutorial_imports_begin__
import argparse
import os
import numpy as np
import torch
import torch.optim as optim
from ray.tune.examples.mnist_pytorch import train, test, ConvNet, get_data_loaders

from ray import tune
from ray.tune.schedulers import PopulationBasedTraining
from ray.tune.trial import ExportFormat

# __tutorial_imports_end__


# __train_begin__
def train_convnet(config, checkpoint_dir=None):
    # Create our data loaders, model, and optmizer.
    step = 0
    train_loader, test_loader = get_data_loaders()
    model = ConvNet()
    optimizer = optim.SGD(
        model.parameters(),
        lr=config.get("lr", 0.01),
        momentum=config.get("momentum", 0.9),
    )

    # If checkpoint_dir is not None, then we are resuming from a checkpoint.
    # Load model state and iteration step from checkpoint.
    if checkpoint_dir:
        print("Loading from checkpoint.")
        path = os.path.join(checkpoint_dir, "checkpoint")
        checkpoint = torch.load(path)
        model.load_state_dict(checkpoint["model_state_dict"])
        step = checkpoint["step"]

    while True:
        train(model, optimizer, train_loader)
        acc = test(model, test_loader)
        if step % 5 == 0:
            # Every 5 steps, checkpoint our current state.
            # First get the checkpoint directory from tune.
            with tune.checkpoint_dir(step=step) as checkpoint_dir:
                # Then create a checkpoint file in this directory.
                path = os.path.join(checkpoint_dir, "checkpoint")
                # Save state to checkpoint file.
                # No need to save optimizer for SGD.
                torch.save(
                    {
                        "step": step,
                        "model_state_dict": model.state_dict(),
                        "mean_accuracy": acc,
                    },
                    path,
                )
        step += 1
        tune.report(mean_accuracy=acc)


# __train_end__


def test_best_model(analysis):
    """Test the best model given output of tune.run"""
    best_checkpoint_path = analysis.best_checkpoint
    best_model = ConvNet()
    best_checkpoint = torch.load(os.path.join(best_checkpoint_path, "checkpoint"))
    best_model.load_state_dict(best_checkpoint["model_state_dict"])
    # Note that test only runs on a small random set of the test data, thus the
    # accuracy may be different from metrics shown in tuning process.
    test_acc = test(best_model, get_data_loaders()[1])
    print("best model accuracy: ", test_acc)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--smoke-test", action="store_true", help="Finish quickly for testing"
    )
    parser.add_argument(
        "--server-address",
        type=str,
        default=None,
        required=False,
        help="The address of server to connect to if using Ray Client.",
    )
    args, _ = parser.parse_known_args()

    if args.server_address:
        import ray

        ray.init(f"ray://{args.server_address}")

    # __pbt_begin__
    scheduler = PopulationBasedTraining(
        time_attr="training_iteration",
        perturbation_interval=5,
        hyperparam_mutations={
            # distribution for resampling
            "lr": lambda: np.random.uniform(0.0001, 1),
            # allow perturbations within this set of categorical values
            "momentum": [0.8, 0.9, 0.99],
        },
    )

    # __pbt_end__

    # __tune_begin__
    class CustomStopper(tune.Stopper):
        def __init__(self):
            self.should_stop = False

        def __call__(self, trial_id, result):
            max_iter = 5 if args.smoke_test else 100
            if not self.should_stop and result["mean_accuracy"] > 0.96:
                self.should_stop = True
            return self.should_stop or result["training_iteration"] >= max_iter

        def stop_all(self):
            return self.should_stop

    stopper = CustomStopper()

    analysis = tune.run(
        train_convnet,
        name="pbt_test",
        scheduler=scheduler,
        metric="mean_accuracy",
        mode="max",
        verbose=1,
        stop=stopper,
        export_formats=[ExportFormat.MODEL],
        checkpoint_score_attr="mean_accuracy",
        keep_checkpoints_num=4,
        num_samples=4,
        config={
            "lr": tune.uniform(0.001, 1),
            "momentum": tune.uniform(0.001, 1),
        },
    )
    # __tune_end__

    if args.server_address:
        # If using Ray Client, we want to make sure checkpoint access
        # happens on the server. So we wrap `test_best_model` in a Ray task.
        # We have to make sure it gets executed on the same node that
        # ``tune.run`` is called on.
        from ray.util.ml_utils.node import force_on_current_node

        remote_fn = force_on_current_node(ray.remote(test_best_model))
        ray.get(remote_fn.remote(analysis))
    else:
        test_best_model(analysis)
