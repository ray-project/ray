from deepmind_lab import dmenv_module

import ray
from ray.rllib import env
from ray.rllib.agents import ppo


class Watermaze(env.DMEnv):

    def __init__(self, env_config):
        lab = dmenv_module.Lab(
            "contributed/dmlab30/rooms_watermaze",
            ["RGBD"],
            config=env_config,
        )
        super(Watermaze, self).__init__(lab)


ray.init()
trainer = ppo.PPOTrainer(env=Watermaze, config={
    "env_config": {"width": "320",
                   "height": "160"},
})

for i in range(10):
    print(trainer.train())
