from unicodedata import name
from ray.tune.registry import register_env
from ray.tune.logger import pretty_print
from IPython.display import clear_output
from ray.rllib.models import ModelCatalog
from ray.rllib.agents.bdq.bdq_model import BDQModelClass
import gym
import numpy as np
from ray.rllib.agents.bdq import (BDQ_DEFAULT_CONFIG, BDQTrainer,
                                  SimpleBDQTFPolicy)


class DiscreteToContinuous(gym.ActionWrapper):
    def __init__(self, env, action_per_branch):
        super().__init__(env)
        self.action_per_branch = action_per_branch
        low = self.action_space.low
        high = self.action_space.high
        num_branches = self.action_space.shape[0]
        self.action_space = gym.spaces.MultiDiscrete([action_per_branch] * num_branches)
        setattr(self.action_space, "n", num_branches)
        self.mesh = []
        for l, h in zip(low, high):
            self.mesh.append(np.linspace(l, h, action_per_branch))

    def action(self, act):
        # modify act
        act = np.array([self.mesh[i][a] for i, a in enumerate(act)])
        return act


def env_creator(env_config):
    return DiscreteToContinuous(gym.make("BipedalWalker-v3"), env_config["action_per_branch"])

if __name__ == "__main__":
    register_env("my_env", env_creator)
    ModelCatalog.register_custom_model("bdq_tf_model", BDQModelClass)

    # Create an RLlib Trainer instance.
    action_per_branch = 8
    bdq_config = BDQ_DEFAULT_CONFIG.copy()
    bdq_config.update({
        "num_workers": 0,
        # Env class to use (here: our gym.Env sub-class from above).
        "env": "my_env",
        # env_config
        "env_config": {"action_per_branch": action_per_branch},
        # bdq model
        "model": {
            "custom_model": "bdq_tf_model",
            # Extra kwargs to be passed to your model's c'tor.
            "custom_model_config": {
                "hiddens_common": [512, 256],
                "hiddens_actions": [128],
                "hiddens_value": [128],
                "action_per_branch": action_per_branch,
            },
        },
        # framework
        "framework": "tf2",
        "eager_tracing": True,
    })
    trainer = BDQTrainer(config=bdq_config)

    # train
    for i in range(10):
        clear_output(wait=True)
        result = trainer.train()
        print(pretty_print(result))
        if i % 100 == 0:
            checkpoint_dir = trainer.save()
            print('Saved trained results in ', checkpoint_dir)
