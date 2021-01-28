"""
Example of running a Unity3D (MLAgents) Policy server that can learn
Policies via sampling inside many connected Unity game clients (possibly
running in the cloud on n nodes).
For a locally running Unity3D example, see:
`examples/unity3d_env_local.py`

To run this script against one or more possibly cloud-based clients:
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

4) Then run (two separate shells/machines):
$ python unity3d_server.py --env 3DBall
$ python unity3d_client.py --inference-mode=local --game [path to game binary]
"""

import argparse
import os

import ray
from ray.tune import register_env
from ray.rllib.agents.ppo import PPOTrainer
from ray.rllib.env.policy_server_input import PolicyServerInput
from ray.rllib.env.wrappers.unity3d_env import Unity3DEnv
from ray.rllib.examples.env.random_env import RandomMultiAgentEnv

SERVER_ADDRESS = "localhost"
SERVER_PORT = 9900
CHECKPOINT_FILE = "last_checkpoint_{}.out"

parser = argparse.ArgumentParser()
parser.add_argument(
    "--env",
    type=str,
    default="3DBall",
    choices=["3DBall", "SoccerStrikersVsGoalie"],
    help="The name of the Env to run in the Unity3D editor. Either `3DBall` "
    "or `SoccerStrikersVsGoalie` (feel free to add more to this script!)")
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
    ray.init()

    # Create a fake-env for the server. This env will never be used (neither
    # for sampling, nor for evaluation) and its obs/action Spaces do not
    # matter either (multi-agent config below defines Spaces per Policy).
    register_env("fake_unity", lambda c: RandomMultiAgentEnv(c))

    policies, policy_mapping_fn = \
        Unity3DEnv.get_policy_configs_for_game(args.env)

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
        "train_batch_size": 256,
        "rollout_fragment_length": 20,
        # Multi-agent setup for the particular env.
        "multiagent": {
            "policies": policies,
            "policy_mapping_fn": policy_mapping_fn,
        },
        "framework": "tf",
    }

    # Create the Trainer used for Policy serving.
    trainer = PPOTrainer(env="fake_unity", config=config)

    # Attempt to restore from checkpoint if possible.
    checkpoint_path = CHECKPOINT_FILE.format(args.env)
    if not args.no_restore and os.path.exists(checkpoint_path):
        checkpoint_path = open(checkpoint_path).read()
        print("Restoring from checkpoint path", checkpoint_path)
        trainer.restore(checkpoint_path)

    # Serving and training loop.
    count = 0
    while True:
        # Calls to train() will block on the configured `input` in the Trainer
        # config above (PolicyServerInput).
        print(trainer.train())
        if count % args.checkpoint_freq == 0:
            print("Saving learning progress to checkpoint file.")
            checkpoint = trainer.save()
            # Write the latest checkpoint location to CHECKPOINT_FILE,
            # so we can pick up from the latest one after a server re-start.
            with open(checkpoint_path, "w") as f:
                f.write(checkpoint)
        count += 1
