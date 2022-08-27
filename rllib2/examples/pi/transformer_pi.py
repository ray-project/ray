


###################################################################
########### Transformer policy
###################################################################

config = PiConfig(
    observation_space=Box(low=-1, high=1, shape=(84, 84, 3)),
    action_space=Discrete(2),
    is_transformer=True,
    # free_log_std=True,
)
pi = Pi(config)
print(pi)