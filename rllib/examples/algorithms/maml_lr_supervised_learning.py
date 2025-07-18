"""Example of how to use `TorchMetaLearner` and `DifferentiableLearner` for MAML.

Meta-learning, or “learning to learn,” trains models to quickly adapt to new tasks
using only a few examples. One prominent method is Model-Agnostic Meta-Learning
(MAML), which is compatible with any model trained via gradient descent. MAML has
been successfully applied across domains such as classification, regression, and
reinforcement learning.

In this MAML example, the goal is to train a model that can adapt to an infinite
number of tasks, where each task corresponds to a sinusoidal function with randomly
sampled amplitude and phase. Because each new task introduces a shift in data
distribution, traditional learning algorithms would fail to generalize — they’d
overfit to the training task and struggle on unseen ones. Meta-learning addresses
this by optimizing the model parameters such that they can be fine-tuned rapidly
for any new task.

During training, a DifferentiableLearner performs an inner-loop update using the
training error for each task. The outer-loop TorchMetaLearner then evaluates the
model’s performance on held-out data (the task's test set) and updates the meta-
parameters so that they lead to better generalization across all tasks. This bi-
level optimization ensures that gradients across tasks remain close, enabling
fast adaptation.

At inference time, the trained model can adapt to a new task using just a small
batch of examples — performing few-shot learning to adjust quickly and accurately.

This example shows:
- how to implement MAML with RLlib in just a few lines of code.
- how to define a `TorchDifferentiableLearner` to register a custom train loss
    function.
- how to define a `TorchMetaLearner` class to implement a custom meta (test) train
    loss function.
- how to configure both learners top be used with each others via the
    `DifferentiableAlgorithmConfig` and `DifferentiableLearnerConfig`.
- how to update the `RLModule` in a meta-learning fashion.
- how to fine-tune an `RLModule` with gradient descent within a few iterations with
    only using the meta (test) loss.

See :py:class:`~ray.rllib.examples.learners.classes.lr_meta_learner.LRTorchMetaLearner` # noqa
class for details on how to override the main `TorchMetaLearner`. And see
:py:class:`~ray.rllib.examples.learners.classes.lr_differentiable_learner.LRTorchDifferentiableLearner` # noqa
class for an example of how to override the main `TorchDifferentiableLearner`.

Note, the meta-learner needs a long-enough training (`default_iters`=~70,000) to learn
to adapt quickly to new tasks.

How to run this script
----------------------
`python [script file name].py --iters=70000 --meta-train-batch-size=5 --fine-tune-batch-size=5`

Use the `--meta-train-batch-size` to set the training/testing batch size in meta-learning and
the `--fine-tune-batch-size` to adjust the number of samples used in all updates during
few-shot learning.

To suppress plotting (plotting is the default) use `--no-plot` and for taking a longer
look at the plot increase the seconds for which plotting is paused at the end of the
script by `--pause-plot-secs`.

Results to expect
-----------------
You should expect to see sometimes alternating test losses ("Total Loss") due to new
(unseen) tasks during meta learning. In few-shot learning after the meta-learning the
(few shot) loss should decrease almost monotonically. In the plot you can expect to see
a decent adaption to the new task after fine-tuning updates of the `RLModule` weights.

With `--iters=70_000`, `--meta-train-batch-size=5`, `--fine-tune-batch-size=5`,
`--fine-tune-lr=0.01`, `--fine-tune-iters=10`, `--meta-lr=0.001`, `--noise-std=0.0`,
and no seed defined.
-------------------------

Iteration: 68000
Total loss: 0.013758559711277485
-------------------------

Iteration: 69000
Total loss: 0.7246640920639038
-------------------------

Iteration: 70000
Total loss: 3.091259002685547

Few shot loss: 2.754437208175659
Few shot loss: 2.7399725914001465
Few shot loss: 2.499554395675659
Few shot loss: 2.1763901710510254
Few shot loss: 1.793503999710083
Few shot loss: 1.4362313747406006
Few shot loss: 1.083552598953247
Few shot loss: 0.7845061421394348
Few shot loss: 0.5579453110694885
Few shot loss: 0.4087105393409729
"""
import gymnasium as gym
import matplotlib.pyplot as plt
import numpy as np

from ray.rllib.algorithms.algorithm_config import DifferentiableAlgorithmConfig
from ray.rllib.core import DEFAULT_MODULE_ID
from ray.rllib.core.columns import Columns
from ray.rllib.core.learner.differentiable_learner_config import (
    DifferentiableLearnerConfig,
)
from ray.rllib.core.learner.training_data import TrainingData
from ray.rllib.core.rl_module.multi_rl_module import MultiRLModuleSpec
from ray.rllib.core.rl_module.rl_module import RLModuleSpec
from ray.rllib.examples.algorithms.classes.maml_lr_differentiable_learner import (
    MAMLTorchDifferentiableLearner,
)
from ray.rllib.examples.algorithms.classes.maml_lr_meta_learner import (
    MAMLTorchMetaLearner,
)
from ray.rllib.examples.algorithms.classes.maml_lr_differentiable_rlm import (
    DifferentiableTorchRLModule,
)
from ray.rllib.policy.sample_batch import MultiAgentBatch, SampleBatch
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.test_utils import add_rllib_example_script_args

# Import torch.
torch, _ = try_import_torch()

# Implement generation of data from sinusoid curves.
def generate_sinusoid_task(batch_size, noise_std=0.1, return_params=False):
    """Generate a sinusoid task with random amplitude and phase.

    Args:
        batch_size: The number of data points to be generated.
        noise_std: An optional standard deviation to be used in the sinusoid
            data generation. Defines a linear error term added to the sine
            curve.
        return_params: If the sampled amplitude and phase should be returned.

    Returns:
        Torch tensors with the support data and the labels of a sinusoid
        curve.
    """
    # Sample the amplitude and the phase for a task.
    amplitude = np.random.uniform(0.1, 5.0)
    phase = np.random.uniform(0.0, np.pi)

    # Sample the support.
    x = np.random.uniform(-5.0, 5.0, (batch_size, 1))

    # Generate the labels.
    y = amplitude * np.sin(x - phase)

    # Add noise.
    y += noise_std * np.random.random((batch_size, 1))

    # If sampled parameters should be returned.
    if return_params:
        # Return torch tensors.
        return (
            torch.tensor(x, dtype=torch.float32),
            torch.tensor(y, dtype=torch.float32),
            amplitude,
            phase,
        )
    # Otherwise, return only the sampled data.
    else:
        return (
            torch.tensor(x, dtype=torch.float32),
            torch.tensor(y, dtype=torch.float32),
        )


def sample_task(batch_size=10, noise_std=0.1, training_data=False, return_params=False):
    """Samples training batches for meta learner and differentiable learner.

    Args:
        batch_size: The batch size for both meta learning and task learning.
        noise_std: An optional standard deviation to be used in the sinusoid
            data generation. Defines a linear error term added to the sine
            curve.
        training_data: Whether data should be returned as `TrainingData`.
            Otherwise, a `MultiAgentBatch` is returned. Default is `False`.
        return_params: If the sampled amplitude and phase should be returned.

    Returns:
        A tuple with training batches for the meta learner and the differentiable
        learner. If `training_data` is `True`, the data is wrapped into
        `TrainingData`, otherwise both batches are `MultiAgentBatch`es.

    """
    # Generate training data for meta learner and differentiable learner.
    train_batch = {}
    generated_data = generate_sinusoid_task(
        batch_size * 2, noise_std=noise_std, return_params=return_params
    )
    train_batch[Columns.OBS], train_batch["y"] = generated_data[:2]

    # Convert to `MultiAgentBatch`.
    meta_train_batch = MultiAgentBatch(
        env_steps=batch_size,
        policy_batches={
            DEFAULT_MODULE_ID: SampleBatch(
                {k: train_batch[k][:batch_size] for k in train_batch}
            )
        },
    )
    task_train_batch = MultiAgentBatch(
        env_steps=batch_size,
        policy_batches={
            DEFAULT_MODULE_ID: SampleBatch(
                {k: train_batch[k][batch_size:] for k in train_batch}
            )
        },
    )
    # If necessary convert to `TrainingData`.
    if training_data:
        meta_train_batch = TrainingData(
            batch=meta_train_batch,
        )
        task_train_batch = TrainingData(
            batch=task_train_batch,
        )

    # If amplitude and phase should be returned add them to the return tuple.
    if return_params:
        return meta_train_batch, task_train_batch, *generated_data[2:]
    # Otherwise return solely train data.
    else:
        return meta_train_batch, task_train_batch


# Define arguments.
parser = add_rllib_example_script_args(default_iters=70_000)

parser.add_argument(
    "--meta-train-batch-size",
    type=int,
    default=5,
    help="The number of samples per train and test update (meta-learning).",
)
parser.add_argument(
    "--meta-lr",
    type=float,
    default=0.001,
    help="The learning rate to be used for meta learning (in the `MetaLearner`).",
)
parser.add_argument(
    "--fine-tune-batch-size",
    type=int,
    default=10,
    help="The number of samples for the fine-tuning updates.",
)
parser.add_argument(
    "--noise-std",
    type=float,
    default=0.0,
    help="The standard deviation for noise added to the single tasks.",
)
parser.add_argument(
    "--seed",
    type=int,
    default=None,
    help="An optional random seed. If not set, the experiment is not reproducable.",
)
parser.add_argument(
    "--fine-tune-iters",
    type=int,
    default=10,
    help="The number of updates in fine-tuning.",
)
parser.add_argument(
    "--fine-tune-lr",
    type=float,
    default=0.01,
    help="The learning rate to be used in fine-tuning the model in the test phase.",
)
parser.add_argument(
    "--no-plot",
    action="store_true",
    help=(
        "If plotting should suppressed. Otherwise user action is needed to close "
        "the plot early."
    ),
)
parser.add_argument(
    "--pause-plot-secs",
    type=int,
    default=1000,
    help=(
        "The number of seconds to keep the plot open. Note the plot can always be "
        "closed by the user when open."
    ),
)

# Parse the arguments.
args = parser.parse_args()

# If a random seed is provided set it for torch and numpy.
if args.seed:
    torch.random.manual_seed(args.seed)
    np.random.seed(args.seed)


if __name__ == "__main__":
    # Define the `RLModule`.
    module_spec = RLModuleSpec(
        module_class=DifferentiableTorchRLModule,
        # Note, the spaces are needed by default but are not used.
        observation_space=gym.spaces.Box(-np.inf, np.inf, (1,), dtype=np.float32),
        action_space=gym.spaces.Box(-np.inf, np.inf, (1,), dtype=np.float32),
    )
    # `Learner`s work on `MultiRLModule`s.
    multi_module_spec = MultiRLModuleSpec(
        rl_module_specs={DEFAULT_MODULE_ID: module_spec}
    )

    # Build the `MultiRLModule`.
    module = multi_module_spec.build()

    # Configure the `DifferentiableLearner`.
    diff_learner_config = DifferentiableLearnerConfig(
        learner_class=MAMLTorchDifferentiableLearner,
        minibatch_size=args.meta_train_batch_size,
        lr=0.01,
    )

    # Configure the `TorchMetaLearner` via the `DifferentiableAlgorithmConfig`.
    config = (
        DifferentiableAlgorithmConfig()
        .learners(
            # Add the `DifferentiableLearnerConfig`s.
            differentiable_learner_configs=[diff_learner_config],
            num_gpus_per_learner=args.num_gpus_per_learner or 0,
        )
        .training(
            lr=args.meta_lr,
            train_batch_size=args.meta_train_batch_size,
            # Use the full batch in a single update.
            minibatch_size=args.meta_train_batch_size,
        )
    )

    # Initialize the `TorchMetaLearner`.
    meta_learner = MAMLTorchMetaLearner(config=config, module_spec=module_spec)
    # Build the `TorchMetaLearner`.
    meta_learner.build()

    for i in range(args.stop_iters):
        # Sample the training data.
        meta_training_data, task_training_data = sample_task(
            args.meta_train_batch_size, noise_std=args.noise_std, training_data=True
        )

        # Update the module.
        outs = meta_learner.update(
            training_data=meta_training_data,
            num_epochs=1,
            others_training_data=[task_training_data],
        )
        iter = i + 1
        if iter % 1000 == 0:
            total_loss = outs["default_policy"]["total_loss"].peek()
            print("-------------------------\n")
            print(f"Iteration: {iter}")
            print(f"Total loss: {total_loss}")

    # Generate test data.
    test_batch, _, amplitude, phase = sample_task(
        batch_size=args.fine_tune_batch_size,
        noise_std=args.noise_std,
        return_params=True,
    )

    if config.num_gpus_per_learner > 0:
        test_batch = meta_learner._convert_batch_type(test_batch)

    # Run inference and plot results.
    with torch.no_grad():
        # Generate a grid for the support.
        x_grid = torch.tensor(
            np.arange(-5.0, 5.0, 0.02), dtype=torch.float32, device=meta_learner._device
        ).view(-1, 1)
        # Get label prediction from the model trained by MAML.
        y_pred = meta_learner.module[DEFAULT_MODULE_ID]({Columns.OBS: x_grid})["y_pred"]

    # Plot the results if requested.
    if not args.no_plot:
        # Sort the data by the support.
        x_order = np.argsort(test_batch[DEFAULT_MODULE_ID][Columns.OBS].numpy()[:, 0])
        x_sorted = test_batch[DEFAULT_MODULE_ID][Columns.OBS].numpy()[:, 0][x_order]
        y_sorted = test_batch[DEFAULT_MODULE_ID]["y"][:, 0][x_order]

        # Plot the data.
        def sinusoid(t):
            return amplitude * np.sin(t - phase)

        plt.ion()
        plt.figure(figsize=(5, 3))
        # Plot the true sinusoid curve.
        plt.plot(x_grid, sinusoid(x_grid), "r", label="Ground Truth")
        # Add the sampled support values.
        plt.plot(x_sorted, y_sorted, "^", color="purple")
        # Add the prediction made by the model after MAML training.
        plt.plot(x_grid, y_pred, ":", label="Prediction", color="#90EE90")
        plt.title(f"MAML Results from {args.fine_tune_iters} fine-tuning steps.")

    # Fine-tune with the meta loss for just a few steps.
    optim = meta_learner.get_optimizers_for_module(DEFAULT_MODULE_ID)[0][1]
    # Set the learning rate to a larger value.
    for g in optim.param_groups:
        g["lr"] = args.fine_tune_lr
    # Now run the fine-tune iterations and update the model via the meta-learner loss.
    for i in range(args.fine_tune_iters):
        # Forward pass.
        fwd_out = {
            DEFAULT_MODULE_ID: meta_learner.module[DEFAULT_MODULE_ID](
                test_batch[DEFAULT_MODULE_ID]
            )
        }
        # Compute the MSE prediction loss.
        loss_per_module = meta_learner.compute_losses(fwd_out=fwd_out, batch=test_batch)
        # Optimize parameters.
        optim.zero_grad(set_to_none=True)
        loss_per_module[DEFAULT_MODULE_ID].backward()
        optim.step()
        # Show the loss for few-shot learning (fine-tuning).
        print(f"Few shot loss: {loss_per_module[DEFAULT_MODULE_ID].item()}")

    # Run the model again after fine-tuning.
    with torch.no_grad():
        y_pred_fine_tuned = meta_learner.module[DEFAULT_MODULE_ID](
            {Columns.OBS: x_grid}
        )["y_pred"]

    if not args.no_plot:
        # Plot the predictions of the fine-tuned model.
        plt.plot(
            x_grid,
            y_pred_fine_tuned,
            "-.",
            label="Tuned Prediction",
            color="green",
            mfc="gray",
        )
        plt.legend()
        plt.show()

        # Pause the plot until the user closes it.
        plt.pause(args.pause_plot_secs)
