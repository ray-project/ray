from deepmind_lab import dmenv_module

import ray
from ray.rllib import env
from ray.rllib.agents import ppo


class Watermaze(env.DMEnv):

    LOOK_LR = "LOOK_LEFT_RIGHT_PIXELS_PER_FRAME"

    def __init__(self, env_config):
        lab = dmenv_module.Lab(
            "contributed/dmlab30/rooms_watermaze",
            ["RGBD"],
            config=env_config,
        )
        print(lab.observation_spec())
        print(lab.action_spec())
        super(Watermaze, self).__init__(lab)


ray.init()
trainer = ppo.PPOTrainer(env=Watermaze, config={
    "env_config": {"width": "320",
                   "height": "160"},
})

for i in range(10):
    print(trainer.train())
