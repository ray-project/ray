import os
import tempfile

import tensorflow as tf
import numpy as np
from datetime import datetime
import gym
import ray
from ddpg import DDPGAgent, DEFAULT_CONFIG
from ray.tune.registry import register_trainable
from ray.rllib.agent import get_agent_class
from ray.tune.logger import NoopLogger, UnifiedLogger, pretty_print

def date_str():
    return datetime.today().strftime("%Y-%m-%d_%H-%M-%S")

def main():
    ray.init(
        redis_address=None,
        num_cpus=6, num_gpus=1)

    config = DEFAULT_CONFIG
    config["env"] = "Pendulum-v0"
    config["clip_rewards"] = False
    config["random_starts"] = False
    config["exploration_fraction"] = 0.4
    register_trainable("DDPG", get_agent_class("DDPG"))

    result_logger = None
    local_dir = "/gruntdata/app_data/jones.wz/rl/temp_log"
    if not result_logger:
        if not os.path.exists(local_dir):
            os.makedirs(local_dir)
        logdir = tempfile.mkdtemp(
            prefix="{}_{}".format(
                "test_ddpg", date_str()),
            dir=local_dir)
        #result_logger = UnifiedLogger(
        #    config, logdir, None)
    remote_logdir = logdir

    def logger_creator(config):
        # Set the working dir in the remote process, for user file writes
        if not os.path.exists(remote_logdir):
            os.makedirs(remote_logdir)
        os.chdir(remote_logdir)
        return NoopLogger(config, remote_logdir)

    runner = DDPGAgent(config=config, \
                       registry=ray.tune.registry.get_registry(), \
                       logger_creator=logger_creator)

    while True:
        result = runner.train()
        print(str(result))
        pretty_print(result)

if __name__=="__main__":
    main()

