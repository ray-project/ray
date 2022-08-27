


###################################################################
########### Recurrent Policies
###################################################################

config = PiConfig(
    observation_space=Box(low=-1, high=1, shape=(10,)),
    action_space=Discrete(2),
    is_rnn=True,
    # free_log_std=True,
)
pi = Pi(config)
print(pi)
