"""
Example of running a Unity3D (MLAgents) Policy server that can learn
Policies via sampling inside many connected Unity game clients (possibly
running in the cloud on n nodes).

To try this out:
1) Install Unity3D and `pip install mlagents`.

2) Compile a Unity3D example game with MLAgents support (e.g. 3DBall or any
   other one that you created yourself) and place the compiled binary
   somewhere, where your RLlib client script (see below) can access it.

2.1) To find Unity3D MLAgent examples, first `pip install mlagents`,
     then check out the `.../ml-agents/Project/Assets/ML-Agents/Examples/`
     folder.

3) Change this RLlib Policy server code so it knows the observation- and
   action Spaces, the different Policies (called "behaviors" in Unity3D
   MLAgents), and Agent-to-Policy mappings for your particular game.
   Alternatively, use one of the two already existing setups (3DBall or
   SoccerStrikersVsGoalie).

4) Then run (in two separate shells):
$ python unity3d_server.py --env 3DBall
$ python unity3d_client.py --inference-mode=local --game [path to game binary]
"""

import argparse
from gym.spaces import Box, MultiDiscrete, Tuple
import numpy as np
import os

import ray
from ray.tune import register_env
from ray.rllib.agents.ppo import PPOTrainer
from ray.rllib.env.policy_server_input import PolicyServerInput
from ray.rllib.examples.env.random_env import RandomMultiAgentEnv

SERVER_ADDRESS = "localhost"
SERVER_PORT = 9900
CHECKPOINT_FILE = "last_checkpoint_{}.out"

parser = argparse.ArgumentParser()
parser.add_argument(
    "--env", type=str, choices=["3DBall", "SoccerStrikersVsGoalie"])
parser.add_argument(
    "--port",
    type=int,
    default=SERVER_PORT,
    help="The Policy server's port to listen on for ExternalEnv client "
    "conections.")
parser.add_argument(
    "--checkpoint-freq",
    type=int,
    default=10,
    help="The frequency with which to create checkpoint files of the learnt "
    "Policies.")
parser.add_argument(
    "--no-restore",
    action="store_true",
    help="Whether to load the Policy "
    "weights from a previous checkpoint")

if __name__ == "__main__":
    args = parser.parse_args()
    ray.init(local_mode=True)

    # Create a fake-env for the server. This env will never be used (neither
    # for sampling, nor for evaluation) and its obs/action Spaces do not
    # matter either (multi-agent config below defines Spaces per Policy).
    register_env("fake_unity", lambda c: RandomMultiAgentEnv(c))

    # The RLlib server must know about the Spaces that the Client will be
    # using inside Unity3D, up-front.
    obs_spaces = {
        # SoccerStrikersVsGoalie.
        "Striker": Tuple([
            Box(float("-inf"), float("inf"), (231, )),
            Box(float("-inf"), float("inf"), (63, )),
        ]),
        "Goalie": Box(float("-inf"), float("inf"), (738, )),
        # 3DBall.
        "Agent": Box(float("-inf"), float("inf"), (8, )),
    }
    action_spaces = {
        # SoccerStrikersVsGoalie.
        "Striker": MultiDiscrete([3, 3, 3]),
        "Goalie": MultiDiscrete([3, 3, 3]),
        # 3DBall.
        "Agent": Box(float("-inf"), float("inf"), (2, ), dtype=np.float32),
    }

    # Policies (Unity: "behaviors") and agent-to-policy mapping fns.
    if args.env == "SoccerStrikersVsGoalie":
        policies = {
            "Striker": (None, obs_spaces["Striker"], action_spaces["Striker"],
                        {}),
            "Goalie": (None, obs_spaces["Goalie"], action_spaces["Goalie"],
                       {}),
        }

        def policy_mapping_fn(agent_id):
            return "Striker" if "Striker" in agent_id else "Goalie"

    else:  # 3DBall
        policies = {
            "Agent": (None, obs_spaces["Agent"], action_spaces["Agent"], {})
        }

        def policy_mapping_fn(agent_id):
            return "Agent"

    # The entire config will be sent to connecting clients so they can
    # build their own samplers (and also Policy objects iff
    # `inference_mode=local` on clients' command line).
    config = {
        # Use the connector server to generate experiences.
        "input": (
            lambda ioctx: PolicyServerInput(ioctx, SERVER_ADDRESS, args.port)),
        # Use a single worker process (w/ SyncSampler) to run the server.
        "num_workers": 0,
        # Disable OPE, since the rollouts are coming from online clients.
        "input_evaluation": [],

        # Other settings.
        "sample_batch_size": 64,
        "train_batch_size": 256,
        "rollout_fragment_length": 20,
        # Multi-agent setup for the particular env.
        "multiagent": {
            "policies": policies,
            "policy_mapping_fn": policy_mapping_fn,
        },
    }

    trainer = PPOTrainer(env="fake_unity", config=config)

    checkpoint_path = CHECKPOINT_FILE.format(args.env)

    # Attempt to restore from checkpoint if possible.
    if not args.no_restore and os.path.exists(checkpoint_path):
        checkpoint_path = open(checkpoint_path).read()
        print("Restoring from checkpoint path", checkpoint_path)
        trainer.restore(checkpoint_path)

    # Serving and training loop.
    count = 0
    while True:
        print(trainer.train())
        if count % args.checkpoint_freq == 0:
            print("Saving learning progress to checkpoint file.")
            checkpoint = trainer.save()
            # Write the latest checkpoint location to CHECKPOINT_FILE,
            # so we can pick up from the latest one after a server re-start.
            with open(checkpoint_path, "w") as f:
                f.write(checkpoint)
        count += 1
