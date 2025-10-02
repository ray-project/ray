# Example demonstrating how to use SHOULD_CHECKPOINT in a tuner callback
# for smart checkpointing logic. This shows how to trigger checkpointing from
# callbacks based on training progress rather than fixed intervals.

import argparse
import json
import os
import time

from ray import tune
from ray.tune import Callback
from ray.tune.result import SHOULD_CHECKPOINT

# Hint: SHOULD_CHECKPOINT is an alias of the string "should_checkpoint"


# Some dummy function
def evaluation_fn(step, width, height):
    time.sleep(0.1)
    return (0.1 + width * step / 100) ** (-1) + height * 0.1


class SmartCheckpointCallback(Callback):
    """Custom callback that triggers checkpointing by updating the result dict.

    This callback demonstrates checkpointing logic beyond
    simple periodic checkpointing. It checkpoints based on performance improvements
    or when the loss becomes unstable.

    Args:
        checkpoint_on_improvement: Checkpoint when loss improves significantly
        checkpoint_on_instability: Checkpoint when loss becomes unstable
    """

    def __init__(
        self,
        *,
        checkpoint_on_improvement: bool = True,
        checkpoint_on_instability: bool = True,
    ):
        self.checkpoint_on_improvement = checkpoint_on_improvement
        self.checkpoint_on_instability = checkpoint_on_instability
        self.best_loss_per_trial = {}
        self.recent_losses_per_trial = {}

    def on_trial_result(self, iteration, trials, trial, result, **info):
        """Called after receiving a result from the trainable.

        This hook implements intelligent checkpointing logic:
        1. Checkpoint when we see significant improvement
        2. Checkpoint when loss becomes unstable (variance increases)
        3. Always checkpoint at specific milestones (every 10 steps)
        """
        trial_id = trial.trial_id
        current_loss = result.get("mean_loss", float("inf"))
        current_step = result.get("iterations", 0)

        # Initialize tracking for this trial
        if trial_id not in self.best_loss_per_trial:
            self.best_loss_per_trial[trial_id] = float("inf")
            self.recent_losses_per_trial[trial_id] = []

        should_checkpoint = False
        reason = ""

        # 1. Checkpoint every 10 steps as a baseline
        if current_step > 0 and current_step % 10 == 0:
            should_checkpoint = True
            reason = f"milestone at step {current_step}"

        # 2. Checkpoint on significant improvement
        if self.checkpoint_on_improvement:
            if (
                current_loss < self.best_loss_per_trial[trial_id] * 0.9
            ):  # 10% improvement
                should_checkpoint = True
                reason = f"significant improvement: {current_loss:.4f} < {self.best_loss_per_trial[trial_id]:.4f}"
                self.best_loss_per_trial[trial_id] = current_loss

        # 3. Checkpoint on instability (high variance in recent losses)
        if self.checkpoint_on_instability and current_step > 5:
            recent_losses = self.recent_losses_per_trial[trial_id]
            recent_losses.append(current_loss)
            if len(recent_losses) > 5:
                recent_losses.pop(0)  # Keep only last 5 losses

            if len(recent_losses) == 5:
                variance = (
                    sum((x - sum(recent_losses) / 5) ** 2 for x in recent_losses) / 5
                )
                if variance > 0.1:  # High variance threshold
                    should_checkpoint = True
                    reason = f"instability detected: variance={variance:.4f}"
        else:
            # Track recent losses
            recent_losses = self.recent_losses_per_trial[trial_id]
            recent_losses.append(current_loss)
            if len(recent_losses) > 5:
                recent_losses.pop(0)

        if should_checkpoint:
            print(
                f"Callback requesting checkpoint for trial {trial_id} at step {current_step}: {reason}"
            )
            result[SHOULD_CHECKPOINT] = True


class OptimizationTrainable(tune.Trainable):
    """A simple trainable that demonstrates automatic checkpointing with callbacks"""

    def setup(self, config):
        """Initialize the trainable"""
        self.current_step = 0
        self.width = config["width"]
        self.height = config["height"]

    def step(self):
        """Perform one step of training"""
        intermediate_score = evaluation_fn(self.current_step, self.width, self.height)
        self.current_step += 1

        return {
            "iterations": self.current_step,
            "mean_loss": intermediate_score,
            "step": self.current_step,  # For tracking
        }

    def save_checkpoint(self, checkpoint_dir):
        """Save checkpoint

        Called automatically by Tune when SHOULD_CHECKPOINT is in the result
        """
        checkpoint_path = os.path.join(checkpoint_dir, "checkpoint.json")
        with open(checkpoint_path, "w") as f:
            json.dump(
                {"step": self.current_step, "width": self.width, "height": self.height},
                f,
            )
        print(f"Checkpoint saved at step {self.current_step}")

    def load_checkpoint(self, checkpoint):
        """Load checkpoint - called automatically by Tune during restoration"""
        checkpoint_path = os.path.join(checkpoint, "checkpoint.json")
        with open(checkpoint_path, "r") as f:
            state = json.load(f)
        self.current_step = state["step"]
        self.width = state["width"]
        self.height = state["height"]
        print(f"Checkpoint loaded from step {self.current_step}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--smoke-test", action="store_true", help="Finish quickly for testing"
    )
    args, _ = parser.parse_known_args()

    print(
        "=" * 60,
        "Ray Tune Example: Smart Checkpointing with custom SHOULD_CHECKPOINT key",
        "=" * 60,
        "",
        "This example demonstrates how to set the SHOULD_CHECKPOINT key in a callback",
        "to implement intelligent checkpointing based on training progress.",
        "",
        "Key features:",
        "- Callback-driven checkpointing by setting result[SHOULD_CHECKPOINT] = True",
        "- Checkpoints triggered by performance improvements",
        "- Milestone-based checkpointing every 10 steps",
        "- Instability detection (high variance in recent losses)",
        "- Automatic checkpoint save/load via class trainable",
        sep="\n",
    )

    # Create the smart checkpoint callback
    checkpoint_callback = SmartCheckpointCallback(
        checkpoint_on_improvement=True, checkpoint_on_instability=True
    )

    tuner = tune.Tuner(
        OptimizationTrainable,
        run_config=tune.RunConfig(
            name="smart_checkpoint_test",
            stop={"training_iteration": 1 if args.smoke_test else 20},
            callbacks=[checkpoint_callback],  # Add our custom callback
            # Disable automatic periodic checkpointing to show callback control
            checkpoint_config=tune.CheckpointConfig(
                checkpoint_frequency=0,  # Disable periodic checkpointing
                checkpoint_at_end=True,  # Still checkpoint at the end
            ),
        ),
        tune_config=tune.TuneConfig(
            metric="mean_loss",
            mode="min",
            num_samples=3,
        ),
        param_space={
            "width": tune.randint(10, 100),
            "height": tune.loguniform(10, 100),
        },
    )

    print(
        "Starting hyperparameter tuning with smart checkpointing...",
        "Watch for checkpoint messages triggered by the callback!",
        sep="\n",
    )

    results = tuner.fit()
    best_result = results.get_best_result()
    print(
        "\n" + "=" * 60,
        "RESULTS",
        "=" * 60,
        f"Best hyperparameters: {best_result.config}",
        f"Best checkpoint: {best_result.checkpoint}",
        "",
        "The checkpoints were triggered by the SmartCheckpointCallback",
        sep="\n",
    )
