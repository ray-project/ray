import gym
import torch

from ray.rllib.core.optim.rl_optimizer import RLOptimizer
from ray.rllib.core.examples.simple_ppo_rl_module import (
    SimplePPOModule,
    get_shared_encoder_config,
)
from ray.rllib.core.rl_module.torch.tests.test_torch_rl_module import (
    to_numpy,
    to_tensor,
)
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.torch_utils import convert_to_torch_tensor
from ray.rllib.utils.numpy import convert_to_numpy


class DummyPPOTorchOptimizer(RLOptimizer):
    def __init__(self, module, config):
        super().__init__(module, config)

    def compute_loss(self, batch, fwd_out):
        adv = batch["rewards"] - fwd_out["vf"]
        actor_loss = -(fwd_out["logp"] * adv).mean()
        critic_loss = (adv**2).mean()
        loss = actor_loss + critic_loss
        loss_dict = {
            "total_loss": loss,
            "actor_loss": actor_loss,
            "critic_loss": critic_loss,
        }
        return loss_dict

    def construct_optimizers(self):
        return [
            torch.optim.SGD(self.module.parameters(), lr=self._config.get("lr", 1e-3))
        ]


class DummyPPOTorchTrainer:
    def __init__(self, config) -> None:
        module_config = config["module_config"]
        optimizer_config = {}
        self._module = SimplePPOModule(module_config)
        self._rl_optimizer = DummyPPOTorchOptimizer(self._module, optimizer_config)
        self._optimizers = self._rl_optimizer.construct_optimizers()

    @staticmethod
    def on_after_compute_gradients(gradients_dict):
        return gradients_dict

    def compile_results(
        self, batch, fwd_out, postprocessed_loss, post_processed_gradients
    ):
        """Compile results from the update."""

        loss_numpy = convert_to_numpy(postprocessed_loss)
        results = {
            "avg_reward": batch["rewards"].mean(),
        }
        results.update(loss_numpy)
        return results

    def update(self, batch):
        """Perform an update on this Trainer."""
        torch_batch = convert_to_torch_tensor(batch)
        fwd_out = self._module.forward_train(torch_batch)
        loss = self._rl_optimizer.compute_loss(torch_batch, fwd_out)
        postprocessed_loss = self._rl_optimizer.on_after_compute_loss(loss)
        gradients = self.compute_gradients(loss)
        post_processed_gradients = self.on_after_compute_gradients(gradients)
        self.apply_gradients(post_processed_gradients)
        results = self.compile_results(
            batch, fwd_out, postprocessed_loss, post_processed_gradients
        )
        return results

    def compute_gradients(self, optimization_vars, **kwargs):
        """Perform an update on self._module

            For example compute and apply gradients to self._module if
                necessary.

        Args:
            optimization_vars: named variables used for optimizing self._module

        Returns:
            A dictionary of extra information and statistics.
        """
        # for torch
        optimization_vars["total_loss"].backward()
        grads = {n: p.grad for n, p in self._module.named_parameters()}
        return grads

    def apply_gradients(self, gradients):
        """Perform an update on self._module"""
        del gradients
        for optimizer in self._optimizers:
            optimizer.step()
            optimizer.zero_grad()

    def set_state(self, state):
        """Set the state of the trainer."""
        self._module.set_state(state.get("module_state", {}))


if __name__ == "__main__":
    env = gym.make("CartPole-v1")
    module_config = get_shared_encoder_config(env)
    module_for_inference = SimplePPOModule(module_config)
    trainer = DummyPPOTorchTrainer(dict(module_config=get_shared_encoder_config(env)))
    trainer.set_state(dict(module_state=module_for_inference.get_state()))

    obs = env.reset()
    returns_to_go = 0
    _obs, _next_obs, _actions, _rewards, _dones = [], [], [], [], []
    for _ in range(env._max_episode_steps):
        _obs.append(obs)
        fwd_out = module_for_inference.forward_inference({"obs": to_tensor(obs)[None]})
        action = to_numpy(fwd_out["action_dist"].sample().squeeze(0))
        next_obs, reward, done, info = env.step(action)
        returns_to_go += reward
        _next_obs.append(next_obs)
        _actions.append([action])
        _rewards.append([reward])
        _dones.append([done])
        if done:
            break
        obs = next_obs
    batch = SampleBatch(
        {
            "obs": _obs,
            "next_obs": _next_obs,
            "actions": _actions,
            "rewards": _rewards,
            "dones": _dones,
        }
    )
    trainer.update(batch)
