import numpy as np

import ray
import ray.rllib.ppo as ppo
import summarization

ray.init()

@ray.remote
def import_summarization():
    summarization

config = ppo.DEFAULT_CONFIG.copy()
config["lambda"] = 0.98
config["gamma"] = 0.99
config["kl_coeff"] = 0.05
config["kl_target"] = 0.02
config["sgd_batchsize"] = 8192
config["num_sgd_iter"] = 20
config["sgd_stepsize"] = 1e-4
config["model"] = {"fcnet_hiddens": [4, 4]}
config["timesteps_per_batch"] = 40000
config["observation_filter"] = "NoFilter"
config["num_workers"] = 2
alg = ppo.PPOAgent("SimilaritySummarization-v0", config)

for i in range(1000):
    result = alg.train()
    print("current status: {}".format(result))
    if i % 5 == 0:
        print("checkpoint path: {}".format(alg.save()))
