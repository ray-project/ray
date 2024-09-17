"""
This example shows how to pretrain an RLModule using behavioral cloning from offline
data and, thereafter, continue training it online with PPO (fine-tuning).
"""
from typing import Dict

import gymnasium as gym
import shutil
import tempfile
import torch

import ray
from ray import tune
from ray.air.constants import TRAINING_ITERATION
from ray.train import RunConfig, FailureConfig
from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.algorithms.ppo.torch.ppo_torch_rl_module import PPOTorchRLModule
from ray.rllib.algorithms.ppo.ppo_catalog import PPOCatalog
from ray.rllib.core.models.base import ACTOR, ENCODER_OUT
from ray.rllib.core.rl_module.rl_module import RLModuleSpec
from ray.rllib.utils.metrics import (
    EPISODE_RETURN_MEAN,
    ENV_RUNNER_RESULTS,
)

GYM_ENV_NAME = "CartPole-v1"
GYM_ENV = gym.make(GYM_ENV_NAME)


class BCActor(torch.nn.Module):
    """A wrapper for the encoder and policy networks of a PPORLModule.

    Args:
        encoder_network: The encoder network of the PPORLModule.
        policy_network: The policy network of the PPORLModule.
        distribution_cls: The distribution class to construct with the logits outputed
            by the policy network.
    """

    def __init__(
        self,
        encoder_network: torch.nn.Module,
        policy_network: torch.nn.Module,
        distribution_cls: torch.distributions.Distribution,
    ):
        super().__init__()
        self.encoder_network = encoder_network
        self.policy_network = policy_network
        self.distribution_cls = distribution_cls

    def forward(
        self, batch: Dict[str, torch.Tensor]
    ) -> torch.distributions.Distribution:
        """Return an action distribution output by the policy network.

        batch: A dict containing the key "obs" mapping to a torch tensor of
            observations.

        """
        # The encoder network has outputs for the actor and critic heads of the
        # PPORLModule. We only want the outputs for the actor head.
        encoder_out = self.encoder_network(batch)[ENCODER_OUT][ACTOR]
        action_logits = self.policy_network(encoder_out)
        distribution = self.distribution_cls(logits=action_logits)
        return distribution


def train_ppo_module_with_bc_finetune(
    dataset: ray.data.Dataset, ppo_module_spec: RLModuleSpec
) -> str:
    """Train an Actor with BC finetuning on dataset.

    Args:
        dataset: The dataset to train on.
        module_spec: The module spec of the PPORLModule that will be trained
            after its encoder and policy networks are pretrained with BC.

    Returns:
        The path to the checkpoint of the pretrained PPORLModule.
    """
    batch_size = 512
    learning_rate = 1e-3
    num_epochs = 10

    module = ppo_module_spec.build()
    # We want to pretrain the encoder and policy networks of the RLModule. We don't want
    # to pretrain the value network. The actor will use the Categorical distribution,
    # as its output distribution since we are training on the CartPole environment which
    # has a discrete action space.
    BCActorNetwork = BCActor(module.encoder, module.pi, torch.distributions.Categorical)
    optim = torch.optim.Adam(BCActorNetwork.parameters(), lr=learning_rate)

    for epoch in range(num_epochs):
        for batch in dataset.iter_torch_batches(
            batch_size=batch_size, dtypes=torch.float32
        ):
            action_dist = BCActorNetwork(batch)
            loss = -torch.mean(action_dist.log_prob(batch["actions"]))
            optim.zero_grad()
            loss.backward()
            optim.step()
        print(f"Epoch {epoch} loss: {loss.detach().item()}")

    checkpoint_dir = tempfile.mkdtemp()
    module.save_to_path(checkpoint_dir)
    return checkpoint_dir


def train_ppo_agent_from_checkpointed_module(
    module_spec_from_ckpt: RLModuleSpec,
) -> float:
    """Trains a checkpointed RLModule using PPO.

    Args:
        module_spec_from_ckpt: The module spec of the checkpointed RLModule.

    Returns:
        The best reward mean achieved by the PPO agent.
    """
    config = (
        PPOConfig()
        .api_stack(enable_rl_module_and_learner=True)
        .rl_module(rl_module_spec=module_spec_from_ckpt)
        .environment(GYM_ENV_NAME)
        .training(
            lr=0.0001,
            gamma=0.99,
            num_epochs=6,
            vf_loss_coeff=0.01,
        )
    )

    tuner = tune.Tuner(
        "PPO",
        param_space=config.to_dict(),
        run_config=RunConfig(
            stop={TRAINING_ITERATION: 20},
            failure_config=FailureConfig(fail_fast="raise"),
            verbose=2,
        ),
    )
    results = tuner.fit()
    best_reward_mean = results.get_best_result().metrics[ENV_RUNNER_RESULTS][
        EPISODE_RETURN_MEAN
    ]
    return best_reward_mean


if __name__ == "__main__":
    ray.init()

    ray.data.DataContext.get_current().enable_progress_bars = False

    # You can use Ray Data to load a dataset from pandas or from a JSON file.
    # The columns of the dataset are ["obs", "actions"].

    ds = ray.data.read_json("s3://rllib-oss-tests/cartpole-expert")

    module_spec = RLModuleSpec(
        module_class=PPOTorchRLModule,
        observation_space=GYM_ENV.observation_space,
        action_space=GYM_ENV.action_space,
        model_config_dict={
            "vf_share_layers": True,
        },
        catalog_class=PPOCatalog,
    )

    # Run supervised training on a PPO Module with behavioral cloning loss.
    module_checkpoint_path = train_ppo_module_with_bc_finetune(ds, module_spec)

    # Modify the load_state_path attribute of module_spec to indicate the checkpoint
    # path for the RLModule. This allows us to resume RL fine-tuning after loading the
    # pre-trained model weights.
    module_spec.load_state_path = module_checkpoint_path

    best_reward = train_ppo_agent_from_checkpointed_module(module_spec)
    assert (
        best_reward > 400.0
    ), "The PPO agent with pretraining should achieve a reward of at least 400.0."
    # clean up the checkpoint directory
    shutil.rmtree(module_checkpoint_path)
