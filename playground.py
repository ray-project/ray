from ray.rllib.agents.a3c import A3CConfig, A3CTrainer
from ray.tune.logger import pretty_print
from ray import tune
import pprint

config = A3CConfig().environment(env='CartPole-v0')\
    .framework('tf').rollouts(create_env_on_local_worker=True)\
    .training(gamma=0.95)

# build the algorithm object
algorithm = config.build()

# run a hyper-parameter search for maximizing reward after 200
tune_config = config.to_dict()
pprint.pprint(tune_config)
tune_config.update({'lr': tune.grid_search([0.01, 0.001, 0.0001])})
tune.run(
    A3CTrainer,
    stop={'episode_reward_mean': 200},
    config=tune_config
)