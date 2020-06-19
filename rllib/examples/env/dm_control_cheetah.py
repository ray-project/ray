from ray.rllib.env.dm_control_wrapper import DMCMake

env = DMCMake(domain_name="cheetah", task_name="run", seed=42)

done = False
obs = env.reset()
print("Initial Obs: {}".format(obs))

for i in range(2):
    action = env.action_space.sample()
    print("Action: {}".format(action))
    obs, reward, done, info = env.step(action)
    print("Obs: {}, Rew: {}, Done: {}".format(obs, reward, done))
