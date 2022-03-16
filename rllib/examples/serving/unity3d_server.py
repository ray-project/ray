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
from ray.rllib.agents.registry import get_trainer_class
from ray.rllib.env.policy_server_input import PolicyServerInput
from ray.rllib.env.wrappers.unity3d_env import Unity3DEnv

SERVER_ADDRESS = "localhost"
SERVER_PORT = 9900
CHECKPOINT_FILE = "last_checkpoint_{}.out"

parser = argparse.ArgumentParser()
parser.add_argument(
    "--run",
    default="PPO",
    choices=["DQN", "PPO"],
    help="The RLlib-registered algorithm to use.",
)
parser.add_argument(
    "--framework",
    choices=["tf", "tf2", "tfe", "torch"],
    default="tf",
    help="The DL framework specifier.",
)
parser.add_argument(
    "--num-workers",
    type=int,
    default=2,
    help="The number of workers to use. Each worker will create "
    "its own listening socket for incoming experiences.",
)
parser.add_argument(
    "--env",
    type=str,
    default="3DBall",
    choices=[
        "3DBall",
        "3DBallHard",
        "FoodCollector",
        "GridFoodCollector",
        "Pyramids",
        "SoccerStrikersVsGoalie",
        "Sorter",
        "Tennis",
        "VisualHallway",
        "Walker",
    ],
    help="The name of the Env to run in the Unity3D editor "
    "(feel free to add more to this script!)",
)
parser.add_argument(
    "--port",
    type=int,
    default=SERVER_PORT,
    help="The Policy server's port to listen on for ExternalEnv client conections.",
)
parser.add_argument(
    "--checkpoint-freq",
    type=int,
    default=10,
    help="The frequency with which to create checkpoint files of the learnt "
    "Policies.",
)
parser.add_argument(
    "--no-restore",
    action="store_true",
    help="Whether to load the Policy weights from a previous checkpoint",
)

if __name__ == "__main__":
    args = parser.parse_args()
    ray.init()

    # `InputReader` generator (returns None if no input reader is needed on
    # the respective worker).
    def _input(ioctx):
        # We are remote worker or we are local worker with num_workers=0:
        # Create a PolicyServerInput.
        if ioctx.worker_index > 0 or ioctx.worker.num_workers == 0:
            return PolicyServerInput(
                ioctx,
                SERVER_ADDRESS,
                args.port + ioctx.worker_index - (1 if ioctx.worker_index > 0 else 0),
            )
        # No InputReader (PolicyServerInput) needed.
        else:
            return None

    # Get the multi-agent policies dict and agent->policy
    # mapping-fn.
    policies, policy_mapping_fn = Unity3DEnv.get_policy_configs_for_game(args.env)

    # The entire config will be sent to connecting clients so they can
    # build their own samplers (and also Policy objects iff
    # `inference_mode=local` on clients' command line).
    config = {
        # Indicate that the Trainer we setup here doesn't need an actual env.
        # Allow spaces to be determined by user (see below).
        "env": None,
        # Use the `PolicyServerInput` to generate experiences.
        "input": _input,
        # Use n worker processes to listen on different ports.
        "num_workers": args.num_workers,
        # Disable OPE, since the rollouts are coming from online clients.
        "input_evaluation": [],
        # Other settings.
        "train_batch_size": 256,
        "rollout_fragment_length": 20,
        # Multi-agent setup for the given env.
        "multiagent": {
            "policies": policies,
            "policy_mapping_fn": policy_mapping_fn,
        },
        # DL framework to use.
        "framework": args.framework,
    }

    # Create the Trainer used for Policy serving.
    trainer = get_trainer_class(args.run)(config=config)

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
