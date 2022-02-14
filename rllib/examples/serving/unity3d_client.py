"""
Example of running a Unity3D client instance against an RLlib Policy server.
Unity3D clients can be run in distributed fashion on n nodes in the cloud
and all connect to the same RLlib server for faster sample collection.
For a locally running Unity3D example, see:
`examples/unity3d_env_local.py`

To run this script on possibly different machines
against a central Policy server:
1) Install Unity3D and `pip install mlagents`.

2) Compile a Unity3D example game with MLAgents support (e.g. 3DBall or any
   other one that you created yourself) and place the compiled binary
   somewhere, where your RLlib client script (see below) can access it.

2.1) To find Unity3D MLAgent examples, first `pip install mlagents`,
     then check out the `.../ml-agents/Project/Assets/ML-Agents/Examples/`
     folder.

3) Change your RLlib Policy server code so it knows the observation- and
   action Spaces, the different Policies (called "behaviors" in Unity3D
   MLAgents), and Agent-to-Policy mappings for your particular game.
   Alternatively, use one of the two already existing setups (3DBall or
   SoccerStrikersVsGoalie).

4) Then run (two separate shells/machines):
$ python unity3d_server.py --env 3DBall
$ python unity3d_client.py --inference-mode=local --game [path to game binary]
"""

import argparse

from ray.rllib.env.policy_client import PolicyClient
from ray.rllib.env.wrappers.unity3d_env import Unity3DEnv

SERVER_ADDRESS = "localhost"
SERVER_PORT = 9900

parser = argparse.ArgumentParser()
parser.add_argument(
    "--game",
    type=str,
    default=None,
    help="The game executable to run as RL env. If not provided, uses local "
    "Unity3D editor instance.",
)
parser.add_argument(
    "--horizon",
    type=int,
    default=200,
    help="The max. number of `step()`s for any episode (per agent) before "
    "it'll be reset again automatically.",
)
parser.add_argument(
    "--server",
    type=str,
    default=SERVER_ADDRESS,
    help="The Policy server's address to connect to from this client.",
)
parser.add_argument(
    "--port", type=int, default=SERVER_PORT, help="The port to use (on --server)."
)
parser.add_argument(
    "--no-train",
    action="store_true",
    help="Whether to disable training (on the server side).",
)
parser.add_argument(
    "--inference-mode",
    type=str,
    default="local",
    choices=["local", "remote"],
    help="Whether to compute actions `local`ly or `remote`ly. Note that "
    "`local` is much faster b/c observations/actions do not have to be "
    "sent via the network.",
)
parser.add_argument(
    "--update-interval-local-mode",
    type=float,
    default=10.0,
    help="For `inference-mode=local`, every how many seconds do we update "
    "learnt policy weights from the server?",
)
parser.add_argument(
    "--stop-reward",
    type=float,
    default=9999,
    help="Stop once the specified reward is reached.",
)

if __name__ == "__main__":
    args = parser.parse_args()

    # Start the client for sending environment information (e.g. observations,
    # actions) to a policy server (listening on port 9900).
    client = PolicyClient(
        "http://" + args.server + ":" + str(args.port),
        inference_mode=args.inference_mode,
        update_interval=args.update_interval_local_mode,
    )

    # Start and reset the actual Unity3DEnv (either already running Unity3D
    # editor or a binary (game) to be started automatically).
    env = Unity3DEnv(file_name=args.game, episode_horizon=args.horizon)
    obs = env.reset()
    eid = client.start_episode(training_enabled=not args.no_train)

    # Keep track of the total reward per episode.
    total_rewards_this_episode = 0.0

    # Loop infinitely through the env.
    while True:
        # Get actions from the Policy server given our current obs.
        actions = client.get_action(eid, obs)
        # Apply actions to our env.
        obs, rewards, dones, infos = env.step(actions)
        total_rewards_this_episode += sum(rewards.values())
        # Log rewards and single-agent dones.
        client.log_returns(eid, rewards, infos, multiagent_done_dict=dones)
        # Check whether all agents are done and end the episode, if necessary.
        if dones["__all__"]:
            print("Episode done: Reward={}".format(total_rewards_this_episode))
            if total_rewards_this_episode >= args.stop_reward:
                quit(0)
            # End the episode and reset Unity Env.
            total_rewards_this_episode = 0.0
            client.end_episode(eid, obs)
            obs = env.reset()
            # Start a new episode.
            eid = client.start_episode(training_enabled=not args.no_train)
