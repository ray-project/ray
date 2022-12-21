#!/usr/bin/env python
"""
Example of running an external simulator (a simple CartPole env
in this case) against an RLlib policy server listening on one or more
HTTP-speaking port(s). See `cartpole_server.py` in this same directory for
how to start this server.

This script will only create one single env altogether to illustrate
that RLlib can run w/o needing an internalized environment.

Setup:
1) Start the policy server:
    See `cartpole_server.py` on how to do this.
2) Run this client:
    $ python cartpole_client.py --inference-mode=local|remote --[other options]
      Use --help for help.

In "local" inference-mode, the action computations are performed
inside the PolicyClient used in this script w/o sending an HTTP request
to the server. This reduces network communication overhead, but requires
the PolicyClient to create its own RolloutWorker (+Policy) based on
the server's config. The PolicyClient will retrieve this config automatically.
You do not need to define the RLlib config dict here!

In "remote" inference mode, the PolicyClient will send action requests to the
server and not compute its own actions locally. The server then performs the
inference forward pass and returns the action to the client.

In either case, the user of PolicyClient must:
- Declare new episodes and finished episodes to the PolicyClient.
- Log rewards to the PolicyClient.
- Call `get_action` to receive an action from the PolicyClient (whether it'd be
  computed locally or remotely).
- Besides `get_action`, the user may let the PolicyClient know about
  off-policy actions having been taken via `log_action`. This can be used in
  combination with `get_action`, but will only work, if the connected server
  runs an off-policy RL algorithm (such as DQN, SAC, or DDPG).
"""

import argparse
import gymnasium as gym

from ray.rllib.env.policy_client import PolicyClient

parser = argparse.ArgumentParser()
parser.add_argument(
    "--no-train", action="store_true", help="Whether to disable training."
)
parser.add_argument(
    "--inference-mode", type=str, default="local", choices=["local", "remote"]
)
parser.add_argument(
    "--off-policy",
    action="store_true",
    help="Whether to compute random actions instead of on-policy "
    "(Policy-computed) ones.",
)
parser.add_argument(
    "--stop-reward",
    type=float,
    default=9999,
    help="Stop once the specified reward is reached.",
)
parser.add_argument(
    "--port", type=int, default=9900, help="The port to use (on localhost)."
)

if __name__ == "__main__":
    args = parser.parse_args()

    # The following line is the only instance, where an actual env will
    # be created in this entire example (including the server side!).
    # This is to demonstrate that RLlib does not require you to create
    # unnecessary env objects within the PolicyClient/Server objects, but
    # that only this following env and the loop below runs the entire
    # training process.
    env = gym.make("CartPole-v1")

    # If server has n workers, all ports between 9900 and 990[n-1] should
    # be listened on. E.g. if server has num_workers=2, try 9900 or 9901.
    # Note that no config is needed in this script as it will be defined
    # on and sent from the server.
    client = PolicyClient(
        f"http://localhost:{args.port}", inference_mode=args.inference_mode
    )

    # In the following, we will use our external environment (the CartPole
    # env we created above) in connection with the PolicyClient to query
    # actions (from the server if "remote"; if "local" we'll compute them
    # on this client side), and send back observations and rewards.

    # Start a new episode.
    obs, info = env.reset()
    eid = client.start_episode(training_enabled=not args.no_train)

    rewards = 0.0
    while True:
        # Compute an action randomly (off-policy) and log it.
        if args.off_policy:
            action = env.action_space.sample()
            client.log_action(eid, obs, action)
        # Compute an action locally or remotely (on server).
        # No need to log it here as the action
        else:
            action = client.get_action(eid, obs)

        # Perform a step in the external simulator (env).
        obs, reward, terminated, truncated, info = env.step(action)
        rewards += reward

        # Log next-obs, rewards, and infos.
        client.log_returns(eid, reward, info=info)

        # Reset the episode if done.
        if terminated or truncated:
            print("Total reward:", rewards)
            if rewards >= args.stop_reward:
                print("Target reward achieved, exiting")
                exit(0)

            rewards = 0.0

            # End the old episode.
            client.end_episode(eid, obs)

            # Start a new episode.
            obs, info = env.reset()
            eid = client.start_episode(training_enabled=not args.no_train)
