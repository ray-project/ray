from ray.rllib.algorithms.crr import CRR, CRRConfig
import ray

data_file = 'rllib/tests/data/pendulum/small.json'

config = CRRConfig()
config.framework('torch')
config.environment(env='Pendulum-v0')
config.offline_data(input_=[data_file])
config.training(train_batch_size=128)

ray.init(local_mode=True)
algo = CRR(config=config)
results = algo.train()

# from ray.rllib.algorithms.cql import CQLTrainer, CQLConfig
# config = CQLConfig()
# config.framework('torch')
# # config.environment(env='Pendulum-v0')
# config.offline_data(input_='d4rl')
# algo = config.build()
# result = algo.train()