"""
Reinforcement learning example using GPU-to-GPU Ray Direct Transport (RDT) and GRPO algorithm.

Based on: https://github.com/meta-pytorch/monarch/blob/0de4e6b4ad7da37e5dbb00a0e6fb61ef8105eac5/examples/presentation/demo.py
"""

import argparse
import copy
import time
from typing import Any

import ray
import numpy as np
import torch
import torch.nn as nn
import torch.optim as optim
from torch.distributions import Categorical

# -- TrajectorySlice --
# TrajectorySlice holds one state's sampled actions and associated metadata:
# - state: The 2D input vector fed to the generator model.
# - actions: The generator model's predictions for this state.
# - policy_version: Version of the generator model when these actions were generated.
# - rewards: The per-action rewards computed by the scorer for this state.
# - old_logps: The log-probabilities of the sampled actions under the policy that generated them.
TrajectorySlice = dict[str, torch.Tensor | int]


# -- Training --
BATCH_SIZE = 32
# Keep learning rate low so that the model does not jump outside
# the trust policy region.
MAX_LR = 1e-6
WEIGHT_DECAY = 1e-10
# Adaptively reduces the learning rate to prevent large updates in a single step.
GRAD_CLIP_NORM = 1.0
# Adds a warmup and cooldown to the learning rate schedule.
WARMUP_FRAC = 0.2
COOLDOWN_FRAC = 0.15

# -- GRPO algorithm --
# Number of actions to sample for each state.
GROUP_SIZE = 10
# How far the new policy is allowed to stray from the older policies
# AKA the "trust region".
GRPO_CLIP_EPS = 0.1
# Discard old experiences, so that the new model can gradually explore
# further from the initial random policy.
MAX_BUFFER_SIZE = BATCH_SIZE * GROUP_SIZE * 5

# -- Environment --
STATE_DIM = 2  # The contextual bandit operates in 2D.
ACTION_DIM = 8  # Eight compass directions: [W, NW, N, NE, E, SE, S, SW].
# Unit direction vectors for the eight compass actions (W, NW, N, NE, E, SE, S, SW).
DIAGONAL_MAGNITUDE = 2**0.5 / 2.0
ACTION_DIRECTIONS = torch.tensor(
    [
        [-1.0, 0.0],  # W
        [-DIAGONAL_MAGNITUDE, DIAGONAL_MAGNITUDE],  # NW
        [0.0, 1.0],  # N
        [DIAGONAL_MAGNITUDE, DIAGONAL_MAGNITUDE],  # NE
        [1.0, 0.0],  # E
        [DIAGONAL_MAGNITUDE, -DIAGONAL_MAGNITUDE],  # SE
        [0.0, -1.0],  # S
        [-DIAGONAL_MAGNITUDE, -DIAGONAL_MAGNITUDE],  # SW
    ],
    dtype=torch.float32,
)


# -- Model --
# To demonstrate speed-ups from RDT, we use an oversized model.
# Residual connections prevent vanishing gradients for deep models.
class ResidualBlock(torch.nn.Module):
    def __init__(self, hidden_dim: int) -> None:
        super().__init__()
        self.norm = torch.nn.LayerNorm(hidden_dim)
        self.activation = torch.nn.ReLU()
        self.linear = torch.nn.Linear(hidden_dim, hidden_dim, bias=True)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        residual = x
        out = self.norm(x)
        out = self.activation(out)
        out = self.linear(out)
        return residual + out


class ResidualMLP(torch.nn.Module):  # Sized to ~50 MB of parameters.
    """Model used for Generator and Learner.

    It takes a 2D state vector as input and produces logits for each action.
    """

    def __init__(self, hidden_dim: int = 512, depth: int = 50):
        super().__init__()
        self.input = torch.nn.Linear(STATE_DIM, hidden_dim, bias=True)
        self.backbone = torch.nn.ModuleList(
            ResidualBlock(hidden_dim) for _ in range(depth - 1)
        )
        self.head = torch.nn.Linear(hidden_dim, ACTION_DIM, bias=True)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        x = self.input(x)
        for block in self.backbone:
            x = block(x)
        x = self.head(x)
        return x


# -- Utilities --
def sample_unit_vector(batch_size: int, dim: int = STATE_DIM) -> torch.Tensor:
    """Sample unit vectors of shape [batch_size, dim] by normalizing Gaussian draws."""
    assert batch_size > 1, "Batch size must be greater than 1"
    v = torch.randn(batch_size, dim)
    norms = v.norm(dim=-1, keepdim=True) + 1e-8
    return v / norms


# -- Actors --
@ray.remote
class ReplayBuffer:
    """Storage for scored trajectory slices.

    This class stores the past experiences (AKA trajectories, or slices) of the model.
    This allows the learner to sample and learn from the same experiences multiple times
    by comparing the latest model with previous models.

    The sampler weights the trajectories by the policy version, such trajectories produced
    by more recent versions of the model are more likely to be sampled.
    """

    def __init__(self) -> None:
        # Each entry stores a TrajectorySlice with CPU tensors.
        self.storage: list[TrajectorySlice] = []

    def put(self, slice: TrajectorySlice) -> None:
        """Add a new slice to the buffer.

        The buffer discards the oldest slices if the buffer gets too large to prevent memory leaks,
        and so that the latest model can gradually explore further from the initial random policy.
        """
        self.storage.append(slice)
        if len(self.storage) > MAX_BUFFER_SIZE:
            self.storage = self.storage[-MAX_BUFFER_SIZE:]

    def sample_from(self, n: int) -> list[TrajectorySlice]:
        """Sample n scored trajectory slices."""
        if self.size() < n:
            print(
                f"Not enough slices in the buffer to sample {n} slices. Waiting for more slices..."
            )
            while len(self.storage) < n:
                time.sleep(0.05)
        # The probability of sampling a slice is proportional to its policy version.
        total = sum(slice["policy_version"] for slice in self.storage)
        probs = [slice["policy_version"] / total for slice in self.storage]
        # Sample with replacement without exceeding the buffer's size.
        n = min(n, self.size())
        chosen = np.random.choice(self.size(), size=n, p=probs, replace=True)
        return [self.storage[i] for i in chosen]

    def size(self) -> int:
        return len(self.storage)


@ray.remote(num_gpus=1)
class Scorer:
    """Evaluates actions and assigns rewards to trajectory slices.

    This scorer implements an analytic contextual bandit reward: for a 2D unit
    context vector `s` and a discrete action `a` in {W, NW, N, NE, E, SE, S, SW},
    reward is cosine_similarity(s, direction[a]) == dot(s, unit_direction[a]).
    """

    def __init__(self, replay_buffer) -> None:
        self.replay_buffer = replay_buffer
        self.action_dirs = ACTION_DIRECTIONS.to("cuda")  # [ACTION_DIM, STATE_DIM]

    def enqueue_trajectory_batch(self, batched_slices: dict) -> None:
        """Score a batch of trajectory slices."""
        states = batched_slices["state"]
        actions = batched_slices["actions"]
        old_logps = batched_slices["old_logps"]
        policy_version = batched_slices["policy_version"]

        for i in range(states.shape[0]):
            # Compute rewards on the GPU: rewards = dot(state, unit_dir).
            dirs = self.action_dirs[actions[i]]  # [GROUP_SIZE, STATE_DIM]
            rewards = torch.mv(dirs, states[i])

            scored = TrajectorySlice(
                policy_version=policy_version,
                state=states[i].detach().cpu(),
                actions=actions[i].detach().cpu(),
                old_logps=old_logps[i].detach().cpu(),
                rewards=rewards.detach().cpu(),
            )

            self.replay_buffer.put.remote(scored)


@ray.remote(num_gpus=1)
class Learner:
    """Updates policy based on collected experiences using GRPO algorithm."""

    def __init__(self, replay_buffer, scheduler_kwargs: dict) -> None:
        self.model = ResidualMLP().to("cuda")

        # Use smaller betas to favor recent momentum history.
        self.optim = optim.AdamW(
            self.model.parameters(),
            lr=MAX_LR,
            weight_decay=WEIGHT_DECAY,
            betas=(0.9, 0.9),
        )

        # Learning rate scheduler with warmup and cooldown.
        warmup = torch.optim.lr_scheduler.LinearLR(
            self.optim,
            start_factor=0.001,
            end_factor=1.0,
            total_iters=scheduler_kwargs["warmup_steps"],
        )
        constant = torch.optim.lr_scheduler.ConstantLR(
            self.optim, factor=1.0, total_iters=scheduler_kwargs["hold_steps"]
        )
        cooldown = torch.optim.lr_scheduler.LinearLR(
            self.optim,
            start_factor=1.0,
            end_factor=0.001,
            total_iters=scheduler_kwargs["cooldown_steps"],
        )

        self.scheduler = torch.optim.lr_scheduler.SequentialLR(
            self.optim,
            [warmup, constant, cooldown],
            milestones=[
                scheduler_kwargs["warmup_steps"],
                scheduler_kwargs["warmup_steps"] + scheduler_kwargs["hold_steps"],
            ],
        )

        self.policy_version = 1
        self.replay_buffer = replay_buffer

    def _compute_advantages(self, rewards: torch.Tensor) -> torch.Tensor:
        """Compute advantages from rewards.

        In GRPO, advantages represent how much better a reward is compared to the mean reward for the group of actions
        Normalizing the advantages stabilizes training by maintaining a consistent scale of updates.
        """
        # Unflatten rewards into [batch_size, GROUP_SIZE] in order to
        # compute per-state mean baselines.
        batch_size = rewards.shape[0] // GROUP_SIZE
        rewards_reshaped = rewards.view(batch_size, GROUP_SIZE)

        # Compute the mean reward for each state's group of actions.
        baselines = rewards_reshaped.mean(dim=1, keepdim=True)  # [batch_size, 1]

        # Subtract the mean reward from each action's reward to get advantages.
        advantages = rewards_reshaped - baselines  # [batch_size, GROUP_SIZE]

        # Flatten the advantages back to the original shape.
        advantages = advantages.reshape(-1)  # [batch_size * GROUP_SIZE]

        # Normalize the advantages for training stability.
        advantages = (advantages - advantages.mean()) / (advantages.std() + 1e-8)

        return advantages

    def _apply_policy_update(
        self,
        states: torch.Tensor,
        actions: torch.Tensor,
        old_logps: torch.Tensor,
        advantages: torch.Tensor,
    ) -> dict[str, float]:
        """Apply GRPO update to the model."""
        # Compute the new policy's action log-probabilities.
        dist_new = Categorical(logits=self.model(states))
        new_logps = dist_new.log_prob(actions)
        # Compare the new log-probabilities to the old log-probabilities to get the probability ratios.
        # This is a proxy for how different the new policy is from the old policy.
        ratio = (new_logps - old_logps).exp()
        unclipped = ratio * advantages
        # The 1 ± ε ratio defines the trust region. If the new policy's probability for an action is more than 1 ± ε times the old policy, clip the ratio.
        # The clamp() operation causes the gradients to be zero outside the trust region. Therefore, any element which is outside the trust region will
        # not contribute to a gradient update (but it still contributes to the loss value)
        clipped = torch.clamp(ratio, 1 - GRPO_CLIP_EPS, 1 + GRPO_CLIP_EPS) * advantages
        # Inside the trust region (ratio is 1 ± ε), both clamp() and min() are no-ops, so you get the normal policy gradient signal (positive for advantages > 0, negative for advantages < 0).
        # For advantages > 0 (good action):
        #   ratio > 1 + ε: the min() selects the clipped branch. This prevents a too-large update, even though the model chose a better action.
        #   ratio < 1 - ε: the min() selects the unclipped branch. The model is penalized for reducing the probabily of a better action.

        # For advantages < 0 (bad action):
        #   ratio > 1 + ε: the min() selects the unclipped branch. The model is penalized for increasing the probabily of a bad action.
        #   ratio < 1 - ε: the min() selects the clipped branch. This prevents the
        #     policy from decreasing the probability for a bad action "too much".
        loss = -torch.min(unclipped, clipped).mean()
        # Fraction of actions which did not contribute to the gradient update.
        clip_fraction = (
            ((ratio < 1 - GRPO_CLIP_EPS) | (ratio > 1 + GRPO_CLIP_EPS)).float().mean()
        )

        # Update the policy network.
        self.optim.zero_grad()
        loss.backward()
        # Clip the gradients to prevent exploding gradients and stabilize training.
        nn.utils.clip_grad_norm_(self.model.parameters(), GRAD_CLIP_NORM)
        self.optim.step()
        self.scheduler.step()
        self.policy_version += 1

        return {
            "loss": loss.detach().item(),
            "clip_fraction": clip_fraction.detach().item(),
        }

    def step(self) -> dict[str, Any]:
        """Perform one training step and return metrics.

        Each step samples a batch of trajectory slices from the replay buffer, computes the advantages, and updates the policy using the GRPO algorithm.
        """
        slices: list[TrajectorySlice] = ray.get(
            self.replay_buffer.sample_from.remote(BATCH_SIZE)
        )
        # Prepare the tensors for the policy update.
        actions = torch.cat([s["actions"] for s in slices]).to("cuda")
        old_logps = torch.cat([s["old_logps"] for s in slices]).to("cuda")
        rewards = torch.cat([s["rewards"] for s in slices]).to("cuda")
        mean_rewards = torch.mean(rewards).item()
        states = torch.stack([s["state"] for s in slices])
        states = states.repeat_interleave(GROUP_SIZE, 0).to("cuda")

        # Compute advantages and update the policy network using GRPO.
        advantages = self._compute_advantages(rewards)
        results = self._apply_policy_update(states, actions, old_logps, advantages)
        results["rewards"] = mean_rewards

        return results

    @ray.method(tensor_transport="nixl")
    def get_weights(self) -> dict[str, torch.Tensor]:
        """Get the current model weights.

        The tensor_transport="nixl" option enables NIXL via RDT to transfer model weight
        tensors. Without it, the weights will be transferred using the Ray object store.
        """
        # Note: deepcopy() is needed because RDT does not support pointers to tensors yet.
        # This will not be needed in the future.
        state_dict = copy.deepcopy(self.model.state_dict())
        assert next(
            iter(state_dict.values())).device.type == "cuda"
            ), "Expected tensors in the sender's cuda memory to demonstrate RDT."

        return state_dict

    def get_version(self) -> int:
        return self.policy_version


@ray.remote(num_gpus=1)
class Generator:
    """Holds the current policy network and generates unscored trajectory slices."""

    def __init__(self, scorer) -> None:
        self.model = ResidualMLP().to("cuda").eval()
        self.scorer = scorer
        self.policy_version = 1

    @ray.method(tensor_transport="nixl")
    def generate(self, states: torch.Tensor):
        """Generate actions using the current policy and send them and their metadata
        to the Scorer.

        Note: GRPO requires *sampling* from the current policy (not just the most probable "greedy" action).
        """
        with torch.no_grad():
            states = states.to("cuda")
            logits = self.model(states)  # [batch_size, ACTION_DIM]


            dist = Categorical(logits=logits)
            # Sample GROUP_SIZE actions for each state.
            actions = dist.sample((GROUP_SIZE,))  # [GROUP_SIZE, batch_size]
            logps = dist.log_prob(actions)  # [GROUP_SIZE, batch_size]
            # Transpose actions and logprobs for compatibility with the states tensor.
            actions = actions.transpose(0, 1).contiguous()  # [batch_size, GROUP_SIZE]
            logps = logps.transpose(0, 1).contiguous()  # [batch_size, GROUP_SIZE]

        # Create trajectory slices and enqueue them for scoring.
        slice_batch = {
            "policy_version": self.policy_version,
            "state": states,
            "actions": actions,
            "old_logps": logps,
        }
        self.scorer.enqueue_trajectory_batch.remote(slice_batch)

    def update_weights(self, cuda_weights, version: int):
        """Update the generator's weights from the learner's weights.

        Note: the actor is single-threaded, so weight loads do not overlap with generation.
        """
        first_tensor = next(iter(cuda_weights.values()))
        assert (
            first_tensor.device.type == "cuda"
            ), "Expected CUDA tensors after GPU-to-GPU direct transfer"
        self.model.load_state_dict(cuda_weights)
        self.model.eval()
        self.policy_version = version


# -- Control loop --
def train(total_steps: int) -> None:
    """Run one end-to-end training session."""
    # Calculate the number of steps for the warmup, hold, and cooldown phases.
    scheduler_kwargs = {
        "warmup_steps": WARMUP_FRAC * total_steps,
        "hold_steps": (1 - WARMUP_FRAC - COOLDOWN_FRAC) * total_steps,
        "cooldown_steps": COOLDOWN_FRAC * total_steps,
    }

    # Instantiate one instance of each actor.
    replay_buf = ReplayBuffer.remote()
    learner = Learner.remote(replay_buf, scheduler_kwargs)
    scorer = Scorer.remote(replay_buf)
    generator = Generator.remote(scorer)

    # Initialize the generator with current learner weights.
    ray.get(
        generator.update_weights.remote(
            learner.get_weights.remote(), learner.get_version.remote()
        )
    )

    # Pre-fill the ReplayBuffer before starting GRPO.
    # Training will block until until enough scored trajectories are available.
    ray.get(generator.generate.remote(sample_unit_vector(batch_size=BATCH_SIZE)))
    losses, rewards, clip_fractions = [], [], []
    for i in range(total_steps):
        states = sample_unit_vector(batch_size=BATCH_SIZE)
        generator.generate.remote(states)
        step_result = ray.get(learner.step.remote())
        losses.append(step_result["loss"])
        rewards.append(step_result["rewards"])
        clip_fractions.append(step_result["clip_fraction"])
        if i % 20 == 0 and i > 0:
            print(
                f"Step {i}/{total_steps} | Loss: {sum(losses[-20:]) / 20} | Rewards: {sum(rewards[-20:]) / 20:.3f} | Fraction clipped: {sum(clip_fractions[-20:]) / 20:.3f}"
            )

        # Update the generator with new weights and version.
        weights_ref = learner.get_weights.remote()
        version_ref = learner.get_version.remote()
        generator.update_weights.remote(weights_ref, version_ref)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--steps",
        type=int,
        default=450,
    )

    args = parser.parse_args()

    ray.init(ignore_reinit_error=True)
    train(total_steps=args.steps)
    print("Done!")
