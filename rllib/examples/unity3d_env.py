"""
Example of running an RLlib Trainer within a Unity3DEnv, which is connected
to a locally running Unity3D editor.

To try this out:
1) Install Unity3D and `pip install mlagents`.

2) Open the Unity3D Editor and load an example scene from the following
   ml-agent pip package location:
   `.../ml-agents/Project/Assets/ML-Agents/Examples/`

3) Then run this script (you will have to press Play in your Unity editor
   at some point to start the game and the learning process):
$ python unity3d_env.py --stop-reward [..] [--torch]?
"""

import argparse

from ray import tune
from ray.rllib.env.unity3d_env import Unity3DEnv
from ray.rllib.utils.test_utils import check_learning_achieved


parser = argparse.ArgumentParser()
parser.add_argument("--as-test", action="store_true")
parser.add_argument("--stop-iters", type=int, default=150)
parser.add_argument("--stop-reward", type=float, default=9999.0)
parser.add_argument("--stop-timesteps", type=int, default=100000)
parser.add_argument(
    "--horizon",
    type=int,
    default=200,
    help="The max. number of `step()`s for any episode (per agent) before "
         "it'll be reset again automatically.")
parser.add_argument("--torch", action="store_true")


if __name__ == "__main__":
    args = parser.parse_args()

    config = {
        "use_pytorch": args.torch,
    }

    stop = {
        "episode_reward_mean": args.stop_reward,
    }
    
    results = tune.run("PPO", config=config, stop=stop, verbose=1)

    if args.as_test:
        check_learning_achieved(results, args.stop_reward)


    # Start and reset the actual Unity3DEnv (either already running Unity3D
    # editor or a binary (game) to be started automatically).
    env = Unity3DEnv(episode_horizon=args.horizon)

    obs = env.reset()

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
