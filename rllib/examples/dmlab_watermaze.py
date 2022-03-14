from deepmind_lab import dmenv_module

from ray.rllib import env


class Watermaze(env.DMEnv):
    def __init__(self, env_config):
        lab = dmenv_module.Lab(
            "contributed/dmlab30/rooms_watermaze",
            ["RGBD"],
            config=env_config,
        )
        super(Watermaze, self).__init__(lab)


env = Watermaze({"width": "320", "height": "160"})
print(env.action_space)

for i in range(2):
    print(
        env.step(
            {
                "CROUCH": 0.0,
                "FIRE": 0.0,
                "JUMP": 0.0,
                "LOOK_DOWN_UP_PIXELS_PER_FRAME": 0.0,
                "LOOK_LEFT_RIGHT_PIXELS_PER_FRAME": 0.0,
                "MOVE_BACK_FORWARD": 0.0,
                "STRAFE_LEFT_RIGHT": 0.0,
            }
        )
    )
