from gym.envs.registration import register

register(
    id='CustomCartpole-v0',
    entry_point='CustomCartpole.CustomCartpole:CustomCartpoleEnv',
)