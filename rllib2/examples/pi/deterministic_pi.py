


# continuous
config = PiConfig(
    observation_space=Box(low=-1, high=1, shape=(10,)),
    action_space=Box(low=-1, high=1, shape=(2,)),
    is_deterministic=True,
    # free_log_std=True,
    # squash_actions=True,
)
pi = Pi(config)
print(pi)


# discrete
config = PiConfig(
    observation_space=Box(low=-1, high=1, shape=(10,)),
    action_space=Discrete(2),
    is_deterministic=True,
    # free_log_std=True,
)
pi = Pi(config)
print(pi)
