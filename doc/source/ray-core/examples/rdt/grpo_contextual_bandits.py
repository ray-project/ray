"""
Reinforcement learning example using GPU-to-GPU Ray Direct Transport (RDT) and GRPO algorithm.
"""
import argparse
import copy
import threading
from contextlib import contextmanager
from typing import Any

import ray
import numpy as np
import torch
import torch.nn as nn
import torch.optim as optim
from torch.distributions import Categorical, kl_divergence
from tqdm.auto import trange


STATE_DIM = 2  # Contextual bandit in 2D
# Eight compass directions: [W, NW, N, NE, E, SE, S, SW]
ACTION_DIM = 8
GROUP_SIZE = 8
BASE_LR = 5e-6
ADAM_EPS = 1e-8
EMA_DECAY = 0.999
PPO_CLIP_EPS = 0.5
KL_COEFF = 0.1
BATCH_SIZE = 16
GRAD_CLIP_NORM = 1.0

# Unit direction vectors for 8 compass actions (W, NW, N, NE, E, SE, S, SW)
diag = 2**0.5 / 2.0
ACTION_DIRECTIONS = torch.tensor(
    [
        [-1.0, 0.0],  # W
        [-diag, diag],  # NW
        [0.0, 1.0],  # N
        [diag, diag],  # NE
        [1.0, 0.0],  # E
        [diag, -diag],  # SE
        [0.0, -1.0],  # S
        [-diag, -diag],  # SW
    ],
    dtype=torch.float32,
)

TrajectorySlice = dict[str, torch.Tensor]


class MLP(torch.nn.Sequential):  # Sized to ~50 MB parameters
    def __init__(self):
        layers = []
        in_dim = STATE_DIM
        for _ in range(50):
            layers.append(torch.nn.Linear(in_dim, 512, bias=True))
            layers.append(torch.nn.LayerNorm(512))
            layers.append(torch.nn.ReLU())
            layers.append(torch.nn.Dropout(0.1))
            in_dim = 512

        # Output layer outputs logits for each action
        layers.append(torch.nn.Linear(in_dim, ACTION_DIM, bias=True))
        layers.append(torch.nn.Dropout(0.1))

        super().__init__(*layers)


def sample_unit_vector(dim: int = STATE_DIM, batch_size: int = 1) -> torch.Tensor:
    """Sample unit vector(s) by normalizing Gaussian draws."""
    v = torch.randn(batch_size, dim)
    norms = v.norm(dim=-1, keepdim=True) + 1e-8
    unit_vectors = v / norms  # [batch_size, STATE_DIM]

    if batch_size == 1:
        return unit_vectors.squeeze(0)
    return unit_vectors


@ray.remote
class ReplayBuffer:
    """Storage for scored trajectory slices."""

    def __init__(self) -> None:
        # (policy_version, TrajectorySlice with CPU tensors)
        self.storage: list[tuple[int, TrajectorySlice]] = []
        # Sum of policy versions. Used for weighted sampling proportional to 
        # policy version.
        self.total = 0

    def put(self, slice: TrajectorySlice) -> None:
        self.storage.append((slice["policy_version"]), slice)
        self.total += slice["policy_version"]

    def sample_from(self, n: int) -> list[TrajectorySlice]:
        """Sample n scored trajectory slices."""
        assert len(self.storage) > 0
        # Probability of sampling a slice is proportional to its policy version.
        probs = [version / self.total for version, _ in self.storage]
        indices = list(range(len(self.storage)))
        n = min(n, len(self.storage))  # sample with replacement but never exceed size
        chosen = np.random.choice(indices, size=n, p=probs, replace=True)
        return [self.storage[i][1] for i in chosen]

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

    @ray.method(tensor_transport="nixl")
    def enqueue_trajectory_batch(self, batched_slices: dict) -> None:
        """Score a batched trajectory slice synchronously."""
        states = batched_slices["state"]
        actions = batched_slices["actions"]
        old_logps = batched_slices["old_logps"]
        policy_version = batched_slices["policy_version"]

        # Ray delivers actor calls one-at-a-time, so doing the work inline keeps
        # ordering deterministic while maintaining a synchronous API surface.
        for i in range(states.shape[0]):

            # Compute rewards on GPU: rewards = dot(state, unit_dir)
            dirs = self.action_dirs[actions[i]]  # [ACTION_DIM, STATE_DIM]
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
    """Updates policy based on collected experiences using PPO algorithm."""

    def __init__(self, replay_buffer) -> None:
        self.model = MLP().to("cuda")

        # Maintain a frozen EMA teacher of the policy for KL computation
        self.ref_model = copy.deepcopy(self.model)
        for p in self.ref_model.parameters():
            p.requires_grad = False
        self.ref_model.eval()

        self.optim = optim.Adam(self.model.parameters(), lr=BASE_LR, eps=ADAM_EPS)

        self.policy_version = 1
        self.replay_buffer = replay_buffer

    def _compute_advantages(self, rewards: torch.Tensor) -> torch.Tensor:
        """Compute advantages from rewards.

        In PPO, advantages represent how much better an action is compared to the average.
        Here we compute advantages by subtracting a baseline (mean reward) from the rewards
        and then normalizing to stabilize training.

        Args:
            rewards: Raw rewards tensor [batch_size * ACTION_DIM]

        Returns:
            Advantages tensor [batch_size * ACTION_DIM]
        """
        # First, reshape rewards to [batch_size, ACTION_DIM] to compute per-state baseline
        batch_size = rewards.shape[0] // ACTION_DIM
        rewards_reshaped = rewards.view(batch_size, ACTION_DIM)

        # Compute baseline (mean reward) for each state
        baselines = rewards_reshaped.mean(dim=1, keepdim=True)  # [batch_size, 1]

        # Subtract baseline from rewards to get advantages
        advantages = rewards_reshaped - baselines  # [batch_size, ACTION_DIM]

        # Reshape back to original shape
        advantages = advantages.reshape(-1)  # [batch_size * ACTION_DIM]

        # Normalize advantages for training stability
        if advantages.numel() > 1:  # Check if we have more than one element
            advantages = (advantages - advantages.mean()) / (advantages.std() + 1e-8)

        return advantages

    def _apply_policy_update(
        self,
        states: torch.Tensor,
        actions: torch.Tensor,
        old_logps: torch.Tensor,
        advantages: torch.Tensor,
    ) -> torch.Tensor:
        """Apply PPO update to policy network."""
        # Compute new log probabilities and ratio
        dist_new = Categorical(logits=self.model(states))
        new_logps = dist_new.log_prob(actions)
        ratio = (new_logps - old_logps).exp()
        unclipped = ratio * advantages
        clipped = torch.clamp(ratio, 1 - PPO_CLIP_EPS, 1 + PPO_CLIP_EPS) * advantages
        ppo_loss = -torch.min(unclipped, clipped).mean()

        # Compute KL divergence between new policy and reference policy
        with torch.no_grad():
            ref_logits = self.ref_model(states)
        kl = kl_divergence(Categorical(logits=ref_logits), dist_new).mean()

        # Combine PPO loss and KL divergence to prevent large policy updates
        loss = ppo_loss + KL_COEFF * kl

        # Update policy network
        self.optim.zero_grad()
        loss.backward()
        nn.utils.clip_grad_norm_(self.model.parameters(), GRAD_CLIP_NORM)
        self.optim.step()
        # Update EMA teacher weights
        with torch.no_grad():
            for p_ref, p in zip(self.ref_model.parameters(), self.model.parameters()):
                # decay * old + (1 - decay) * new
                p_ref.mul_(EMA_DECAY).add_(p.data, alpha=1.0 - EMA_DECAY)
        self.policy_version += 1
        return loss.detach()

    def step(self) -> dict[str, Any]:
        """Perform one training step and return lightweight metrics."""
        slices: list[TrajectorySlice] = ray.get(
            self.replay_buffer.sample_from.remote(BATCH_SIZE)
        )
        raw_states = torch.stack([s["state"] for s in slices])
        actions = torch.cat([s["actions"] for s in slices])
        old_logps = torch.cat([s["old_logps"] for s in slices])
        rewards = torch.cat([s["rewards"] for s in slices])

        # Track cosine gap between best possible action and model prediction for reporting.
        first_state = raw_states[0]
        first_reward = float(rewards[0].item())
        cosine_values = torch.mv(ACTION_DIRECTIONS, first_state)
        best_first_reward = float(torch.max(cosine_values).item())
        cosine_gap = abs(best_first_reward - first_reward)

        # Prepare tensors for update
        states = raw_states.repeat_interleave(ACTION_DIM, 0).to("cuda")
        actions = actions.to("cuda")
        old_logps = old_logps.to("cuda")
        rewards = rewards.to("cuda")

        # Compute advantages and update policy
        advs = self._compute_advantages(rewards)
        loss = self._apply_policy_update(states, actions, old_logps, advs)

        return {
            "loss": float(loss.item()),
            "cosine_gap": float(cosine_gap),
        }

    @ray.method(tensor_transport="nixl")
    def get_weights(self) -> dict[str, torch.Tensor]:
        """The tensor_transport="nixl" option uses NIXL via RDT to transfer model weight tensors. Removing it will default to the Ray object store."""
        state_dict = self.model.state_dict()
        assert (
            next(iter(state_dict.values())).device.type == "cuda"
            ), "Expected tensors to be on cuda on sender"
        return self.model.state_dict()

    def get_version(self) -> int:
        return self.policy_version


@ray.remote(num_cpus=0)
class SignalActor:
    """Gate to pause sampling during weight updates."""

    def __init__(self) -> None:
        self.generation_allowed = threading.Event()
        self.generation_allowed.set()

    def disable_generation(self) -> bool:
        was_allowed = self.generation_allowed.is_set()
        self.generation_allowed.clear()
        return was_allowed

    def allow_generation(self) -> bool:
        was_allowed = self.generation_allowed.is_set()
        self.generation_allowed.set()
        return was_allowed

    def is_generation_allowed(self) -> bool:
        is_allowed = self.generation_allowed.is_set()
        return is_allowed

    def wait_for_generation(self, timeout: float | None = None) -> bool:
        """Block until generation is allowed or timeout expires."""
        return self.generation_allowed.wait(timeout)


@ray.remote(num_gpus=1)
class Generator:
    """Generates actions using the current policy and sends them to scoring."""

    def __init__(self, scorer, generation_signal) -> None:
        self.model = MLP().to("cuda").eval()
        self.scorer = scorer
        self._generation_signal = generation_signal
        self.policy_version = 1

    @contextmanager
    def _pause_generation(self):
        """Temporarily block new generation requests while we swap weights."""
        # SignalActor uses a threading.Event, so this round-trip keeps the pause scoped.
        ray.get(self._generation_signal.disable_generation.remote())
        try:
            yield
        finally:
            # Always re-enable to avoid deadlocking the driver on failures.
            ray.get(self._generation_signal.allow_generation.remote())

    @ray.method(tensor_transport="nixl")
    def generate(self, states: torch.Tensor):
        ray.get(self._generation_signal.wait_for_generation.remote())
        with torch.no_grad():
            states_cuda = states.cuda()
            logits = self.model(states_cuda)  # [batch_size, ACTION_DIM]

            # GRPO requires sampling from the current policy (not just the greedy action)

            # Create distribution for each state (batch_size distributions over ACTION_DIM actions each)
            dist = Categorical(logits=logits)
            # Sample GROUP_SIZE actions from each state's distribution.
            acts = dist.sample((GROUP_SIZE,))  # [GROUP_SIZE, batch_size]
            logps = dist.log_prob(acts)  # [GROUP_SIZE, batch_size]
            # Transpose actions and logprobs for compatibility with the state tensor.
            acts = acts.transpose(0, 1).contiguous()  # [batch_size, GROUP_SIZE]
            logps = logps.transpose(0, 1).contiguous()  # [batch_size, GROUP_SIZE]

        # Create trajectory slices and enqueue trajectory slices for Scorer
        slice_batch = {
            "policy_version": self.policy_version,
            "state": states,
            "actions": acts,
            "old_logps": logps,
        }
        self.scorer.enqueue_trajectory_batch.remote(slice_batch)

    def update_weights(self, cuda_weights, version: int) -> bool:
        """Apply GPU-to-GPU policy weight updates while pausing sampling."""
        # Disable sampling while the GPU object transfer and weight load are in-flight.
        with self._pause_generation():
            first_tensor = next(iter(cuda_weights.values()))
            if first_tensor.device.type != "cuda":
                raise RuntimeError(
                    "Expected CUDA tensors after GPU-to-GPU direct transfer"
                )
            self.model.load_state_dict(cuda_weights)
            self.model.eval()
            self.policy_version = version
        return True


def run_once(total_steps: int) -> None:
    """Run one end-to-end training session."""
    # Instantiate one instance of each actor.
    replay_buf = ReplayBuffer.remote()
    learner = Learner.remote(replay_buf)
    scorer = Scorer.remote(replay_buf)
    signal = SignalActor.remote()
    generator = Generator.remote(scorer, signal)

    # Initialize generator with current learner weights
    ray.get(
        generator.update_weights.remote(
            learner.get_weights_ref.remote(), learner.get_version.remote()
        )
    )

    # Pre-fill ReplayBuffer before starting PPO.
    ray.get(generator.generate.remote(sample_unit_vector(batch_size=BATCH_SIZE)))

    

    for i in trange(total_steps, desc="Training", unit="step"):
        states = sample_unit_vector(
            batch_size=BATCH_SIZE
        )  # [BATCH_SIZE, STATE_DIM]
        generator.generate.remote(states)
        step_result = ray.get(learner.step.remote())

        if i % 100 == 0:
            print(f"loss: {step_result['loss']:.3f}, cosine_gap: {step_result['cosine_gap']:.3f}")

        # Update generator with new weights/version
        weights_ref = learner.get_weights.remote()
        version_ref = learner.get_version.remote()
        generator.update_weights.remote(weights_ref, version_ref)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--steps",
        type=int,
        default=2000,
    )

    args = parser.parse_args()

    ray.init(ignore_reinit_error=True)
    run_once(total_steps=args.steps)
    print("Done!")
