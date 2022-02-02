"""
Dummy in-place replacement for the unity3d_client.py script
in case you don't have an actual Unity3D engine installed or just want
to test client/server connectivity with the unity3d_server.py script.

This client script simply uses RLlib's RandomMultiAgentEnv to mimic
one of the ML Agents (Unity3D) example games (e.g. "3DBall").

To run this script on possibly different machines
against a central Policy server:

1) Run (two separate shells/machines):
$ python unity3d_server.py --env 3DBall
$ python unity3d_dummy_client.py --env 3DBall --inference-mode=local
"""

import argparse

from ray.rllib.env.policy_client import PolicyClient
from ray.rllib.env.wrappers.unity3d_env import Unity3DEnv
from ray.rllib.examples.env.random_env import RandomMultiAgentEnv

SERVER_ADDRESS = "localhost"
SERVER_PORT = 9900

parser = argparse.ArgumentParser()
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
        "Sorter",
        "Tennis",
        "VisualHallway",
        "Walker",
    ],
    help="The name of the Env to mimic. Only those examples supported so "
    "far for which all agents have the same "
    "observation- and action spaces (feel free to add more to this script!)",
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
    "--num-episodes",
    type=int,
    default=10,
    help="Stop once the specified number of episodes have been played.",
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

    # Get the multi-agent policies dict and agent->policy
    # mapping-fn.
    policies, policy_mapping_fn = Unity3DEnv.get_policy_configs_for_game(args.env)

    # Make sure all policies' obs- and action spaces are the same.
    # If not, we won't be able to mimic the Unity3D env using RLlib's
    # RandomMultiAgentEnv.
    first_policy_spec = next(iter(policies.values()))
    for pid, policy_spec in policies.items():
        assert policy_spec.observation_space == first_policy_spec.observation_space
        assert policy_spec.action_space == first_policy_spec.action_space

    # Start and reset the actual Unity3DEnv (either already running Unity3D
    # editor or a binary (game) to be started automatically).
    env = RandomMultiAgentEnv(
        {
            # Same number of agents as the actual Unity3D game would have.
            "num_agents": len(policies),
            # Make sure we stick to the user given horizons using our
            # RandomMultiAgentEnv options.
            "max_episode_len": args.horizon,
            "p_done": 0.0,
            # Same obs- action spaces as the actual Unity3D game would have.
            "observation_space": first_policy_spec.observation_space,
            "action_space": first_policy_spec.action_space,
        }
    )
    obs = env.reset()
    eid = client.start_episode(training_enabled=not args.no_train)

    # Keep track of the total reward per episode.
    total_rewards_this_episode = 0.0

    # Loop through the env until n episodes completed.
    num_episodes = 0
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

            num_episodes += 1
            if num_episodes >= args.num_episodes:
                quit(0)

            # End the episode and reset dummy Env.
            total_rewards_this_episode = 0.0
            client.end_episode(eid, obs)
            obs = env.reset()
            # Start a new episode.
            eid = client.start_episode(training_enabled=not args.no_train)
