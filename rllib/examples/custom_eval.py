"""Example of customizing evaluation with RLlib.

Pass --custom-eval to run with a custom evaluation function too.

Here we define a custom evaluation method that runs a specific sweep of env
parameters (SimpleCorridor corridor lengths).

------------------------------------------------------------------------
Sample output for `python custom_eval.py`
------------------------------------------------------------------------

INFO trainer.py:623 -- Evaluating current policy for 10 episodes.
INFO trainer.py:650 -- Running round 0 of parallel evaluation (2/10 episodes)
INFO trainer.py:650 -- Running round 1 of parallel evaluation (4/10 episodes)
INFO trainer.py:650 -- Running round 2 of parallel evaluation (6/10 episodes)
INFO trainer.py:650 -- Running round 3 of parallel evaluation (8/10 episodes)
INFO trainer.py:650 -- Running round 4 of parallel evaluation (10/10 episodes)

Result for PG_SimpleCorridor_2c6b27dc:
  ...
  evaluation:
    custom_metrics: {}
    episode_len_mean: 15.864661654135338
    episode_reward_max: 1.0
    episode_reward_mean: 0.49624060150375937
    episode_reward_min: 0.0
    episodes_this_iter: 133
    off_policy_estimator: {}
    policy_reward_max: {}
    policy_reward_mean: {}
    policy_reward_min: {}
    sampler_perf:
      mean_env_wait_ms: 0.0362923321333299
      mean_inference_ms: 0.6319202064080927
      mean_processing_ms: 0.14143652169068222

------------------------------------------------------------------------
Sample output for `python custom_eval.py --custom-eval`
------------------------------------------------------------------------

INFO trainer.py:631 -- Running custom eval function <function ...>
Update corridor length to 4
Update corridor length to 7
Custom evaluation round 1
Custom evaluation round 2
Custom evaluation round 3
Custom evaluation round 4

Result for PG_SimpleCorridor_0de4e686:
  ...
  evaluation:
    custom_metrics: {}
    episode_len_mean: 9.15695067264574
    episode_reward_max: 1.0
    episode_reward_mean: 0.9596412556053812
    episode_reward_min: 0.0
    episodes_this_iter: 223
    foo: 1
    off_policy_estimator: {}
    policy_reward_max: {}
    policy_reward_mean: {}
    policy_reward_min: {}
    sampler_perf:
      mean_env_wait_ms: 0.03423667269562796
      mean_inference_ms: 0.5654563161491506
      mean_processing_ms: 0.14494765630060774
"""

import argparse
import numpy as np
import gym
from gym.spaces import Discrete, Box

import ray
from ray import tune
from ray.rllib.evaluation.metrics import collect_episodes, summarize_episodes

parser = argparse.ArgumentParser()
parser.add_argument("--custom-eval", action="store_true")
parser.add_argument("--num-cpus", type=int, default=0)
args = parser.parse_args()


def custom_eval_function(trainer, eval_workers):
    """Example of a custom evaluation function.

    Arguments:
        trainer (Trainer): trainer class to evaluate.
        eval_workers (WorkerSet): evaluation workers.

    Returns:
        metrics (dict): evaluation metrics dict.
    """

    # We configured 2 eval workers in the training config.
    worker_1, worker_2 = eval_workers.remote_workers()

    # Set different env settings for each worker. Here we use a fixed config,
    # which also could have been computed in each worker by looking at
    # env_config.worker_index (printed in SimpleCorridor class above).
    worker_1.foreach_env.remote(lambda env: env.set_corridor_length(4))
    worker_2.foreach_env.remote(lambda env: env.set_corridor_length(7))

    for i in range(5):
        print("Custom evaluation round", i)
        # Calling .sample() runs exactly one episode per worker due to how the
        # eval workers are configured.
        ray.get([w.sample.remote() for w in eval_workers.remote_workers()])

    # Collect the accumulated episodes on the workers, and then summarize the
    # episode stats into a metrics dict.
    episodes, _ = collect_episodes(
        remote_workers=eval_workers.remote_workers(), timeout_seconds=99999)
    # You can compute metrics from the episodes manually, or use the
    # convenient `summarize_episodes()` utility:
    metrics = summarize_episodes(episodes)
    # Note that the above two statements are the equivalent of:
    # metrics = collect_metrics(eval_workers.local_worker(),
    #                           eval_workers.remote_workers())

    # You can also put custom values in the metrics dict.
    metrics["foo"] = 1
    return metrics


class SimpleCorridor(gym.Env):
    """Custom env we use for this example."""

    def __init__(self, env_config):
        self.end_pos = env_config["corridor_length"]
        self.cur_pos = 0
        self.action_space = Discrete(2)
        self.observation_space = Box(0.0, 9999, shape=(1, ), dtype=np.float32)
        print("Created env for worker index", env_config.worker_index,
              "with corridor length", self.end_pos)

    def set_corridor_length(self, length):
        print("Update corridor length to", length)
        self.end_pos = length

    def reset(self):
        self.cur_pos = 0
        return [self.cur_pos]

    def step(self, action):
        assert action in [0, 1], action
        if action == 0 and self.cur_pos > 0:
            self.cur_pos -= 1
        elif action == 1:
            self.cur_pos += 1
        done = self.cur_pos >= self.end_pos
        return [self.cur_pos], 1 if done else 0, done, {}


if __name__ == "__main__":
    if args.custom_eval:
        eval_fn = custom_eval_function
    else:
        eval_fn = None

    ray.init(num_cpus=args.num_cpus or None)

    tune.run(
        "PG",
        stop={
            "training_iteration": 10,
        },
        config={
            "env": SimpleCorridor,
            "env_config": {
                "corridor_length": 10,
            },
            "horizon": 20,
            "log_level": "INFO",

            # Training rollouts will be collected using just the learner
            # process, but evaluation will be done in parallel with two
            # workers. Hence, this run will use 3 CPUs total (1 for the
            # learner + 2 more for evaluation workers).
            "num_workers": 0,
            "evaluation_num_workers": 2,

            # Optional custom eval function.
            "custom_eval_function": eval_fn,

            # Enable evaluation, once per training iteration.
            "evaluation_interval": 1,

            # Run 10 episodes each time evaluation runs.
            "evaluation_num_episodes": 10,

            # Override the env config for evaluation.
            "evaluation_config": {
                "env_config": {
                    # Evaluate using LONGER corridor than trained on.
                    "corridor_length": 5,
                },
            },
        })
