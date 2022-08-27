


################################################################
###########  a ~ N(mu(s), std(s))
################################################################

config = PiConfig(
    observation_space=Box(low=-1, high=1, shape=(10,)),
    action_space=Box(low=-1, high=1, shape=(2,)),
    is_deterministic=False,
    free_log_std=True,
    # squash_actions=True,
)
pi = Pi(config)
print(pi)
