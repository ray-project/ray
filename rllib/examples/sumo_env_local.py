#!/usr/bin/env python3
""" Example Trainer for RLLIB + SUMO Utlis

    Author: Lara CODECA lara.codeca@gmail.com

    See:
        https://github.com/lcodeca/rllibsumoutils
        https://github.com/lcodeca/rllibsumodocker
    for further details.
"""

import argparse
from copy import deepcopy
import logging
import os
import pathlib
from pprint import pformat

import ray
from ray import tune

from ray.rllib.agents.ppo import ppo
from ray.rllib.examples.simulators.sumo import marlenvironment
from ray.rllib.utils.test_utils import check_learning_achieved

logging.basicConfig(level=logging.WARN)
logger = logging.getLogger("ppotrain")

parser = argparse.ArgumentParser()
parser.add_argument(
    "--sumo-connect-lib",
    type=str,
    default="libsumo",
    choices=["libsumo", "traci"],
    help="The SUMO connector to import. Requires the env variable SUMO_HOME set.",
)
parser.add_argument(
    "--sumo-gui",
    action="store_true",
    help="Enables the SUMO GUI. Possible only with TraCI connector.",
)
parser.add_argument(
    "--sumo-config-file",
    type=str,
    default=None,
    help="The SUMO configuration file for the scenario.",
)
parser.add_argument(
    "--from-checkpoint",
    type=str,
    default=None,
    help="Full path to a checkpoint file for restoring a previously saved "
    "Trainer state.",
)
parser.add_argument("--num-workers", type=int, default=0)
parser.add_argument(
    "--as-test",
    action="store_true",
    help="Whether this script should be run as a test: --stop-reward must "
    "be achieved within --stop-timesteps AND --stop-iters.",
)
parser.add_argument(
    "--stop-iters", type=int, default=10, help="Number of iterations to train."
)
parser.add_argument(
    "--stop-timesteps", type=int, default=1000000, help="Number of timesteps to train."
)
parser.add_argument(
    "--stop-reward",
    type=float,
    default=30000.0,
    help="Reward at which we stop training.",
)

if __name__ == "__main__":
    args = parser.parse_args()
    ray.init()
    tune.register_env("sumo_test_env", marlenvironment.env_creator)

    # Algorithm.
    policy_class = ppo.PPOTFPolicy
    config = ppo.DEFAULT_CONFIG
    config["framework"] = "tf"
    config["gamma"] = 0.99
    config["lambda"] = 0.95
    config["log_level"] = "WARN"
    config["lr"] = 0.001
    config["min_time_s_per_reporting"] = 5
    config["num_gpus"] = int(os.environ.get("RLLIB_NUM_GPUS", "0"))
    config["num_workers"] = args.num_workers
    config["rollout_fragment_length"] = 200
    config["sgd_minibatch_size"] = 256
    config["train_batch_size"] = 4000

    config["batch_mode"] = "complete_episodes"
    config["no_done_at_end"] = True

    # Load default Scenario configuration for the LEARNING ENVIRONMENT
    scenario_config = deepcopy(marlenvironment.DEFAULT_SCENARIO_CONFING)
    scenario_config["seed"] = 42
    scenario_config["log_level"] = "INFO"
    scenario_config["sumo_config"]["sumo_connector"] = args.sumo_connect_lib
    scenario_config["sumo_config"]["sumo_gui"] = args.sumo_gui
    if args.sumo_config_file is not None:
        scenario_config["sumo_config"]["sumo_cfg"] = args.sumo_config_file
    else:
        filename = "{}/simulators/sumo/scenario/sumo.cfg.xml".format(
            pathlib.Path(__file__).parent.absolute()
        )
        scenario_config["sumo_config"]["sumo_cfg"] = filename

    scenario_config["sumo_config"]["sumo_params"] = ["--collision.action", "warn"]
    scenario_config["sumo_config"]["trace_file"] = True
    scenario_config["sumo_config"]["end_of_sim"] = 3600  # [s]
    scenario_config["sumo_config"][
        "update_freq"
    ] = 10  # number of traci.simulationStep()
    # for each learning step.
    scenario_config["sumo_config"]["log_level"] = "INFO"
    logger.info("Scenario Configuration: \n %s", pformat(scenario_config))

    # Associate the agents with their configuration.
    agent_init = {
        "agent_0": deepcopy(marlenvironment.DEFAULT_AGENT_CONFING),
        "agent_1": deepcopy(marlenvironment.DEFAULT_AGENT_CONFING),
    }
    logger.info("Agents Configuration: \n %s", pformat(agent_init))

    # MARL Environment Init
    env_config = {
        "agent_init": agent_init,
        "scenario_config": scenario_config,
    }
    marl_env = marlenvironment.SUMOTestMultiAgentEnv(env_config)

    # Config for the PPO trainer from the MARLEnv
    policies = {}
    for agent in marl_env.get_agents():
        agent_policy_params = {}
        policies[agent] = (
            policy_class,
            marl_env.get_obs_space(agent),
            marl_env.get_action_space(agent),
            agent_policy_params,
        )
    config["multiagent"]["policies"] = policies
    config["multiagent"][
        "policy_mapping_fn"
    ] = lambda agent_id, episode, **kwargs: agent_id
    config["multiagent"]["policies_to_train"] = ["ppo_policy"]

    config["env"] = "sumo_test_env"
    config["env_config"] = env_config

    logger.info("PPO Configuration: \n %s", pformat(config))

    stop = {
        "training_iteration": args.stop_iters,
        "timesteps_total": args.stop_timesteps,
        "episode_reward_mean": args.stop_reward,
    }

    # Run the experiment.
    results = tune.run(
        "PPO",
        config=config,
        stop=stop,
        verbose=1,
        checkpoint_freq=10,
        restore=args.from_checkpoint,
    )

    # And check the results.
    if args.as_test:
        check_learning_achieved(results, args.stop_reward)

    ray.shutdown()
