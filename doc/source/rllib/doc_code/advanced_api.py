# flake8: noqa

# __rllib-adv_api_counter_begin__
import ray


@ray.remote
class Counter:
    def __init__(self):
        self.count = 0

    def inc(self, n):
        self.count += n

    def get(self):
        return self.count


# on the driver
counter = Counter.options(name="global_counter").remote()
print(ray.get(counter.get.remote()))  # get the latest count

# in your envs
counter = ray.get_actor("global_counter")
counter.inc.remote(1)  # async call to increment the global count
# __rllib-adv_api_counter_end__


# __rllib-adv_api_explore_begin__
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig

config = AlgorithmConfig().env_runners(
    exploration_config={
        # Special `type` key provides class information
        "type": "StochasticSampling",
        # Add any needed constructor args here.
        "constructor_arg": "value",
    }
)
# __rllib-adv_api_explore_end__


# __rllib-adv_api_evaluation_1_begin__
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig

# Run one evaluation step on every 3rd `Algorithm.train()` call.
config = AlgorithmConfig().evaluation(
    evaluation_interval=3,
)
# __rllib-adv_api_evaluation_1_end__


# __rllib-adv_api_evaluation_2_begin__
# Every time we run an evaluation step, run it for exactly 10 episodes.
config = AlgorithmConfig().evaluation(
    evaluation_duration=10,
    evaluation_duration_unit="episodes",
)
# Every time we run an evaluation step, run it for (close to) 200 timesteps.
config = AlgorithmConfig().evaluation(
    evaluation_duration=200,
    evaluation_duration_unit="timesteps",
)
# __rllib-adv_api_evaluation_2_end__


# __rllib-adv_api_evaluation_3_begin__
# Every time we run an evaluation step, run it for exactly 10 episodes, no matter,
# how many eval workers we have.
config = AlgorithmConfig().evaluation(
    evaluation_duration=10,
    evaluation_duration_unit="episodes",
    # What if number of eval workers is non-dividable by 10?
    # -> Run 7 episodes (1 per eval worker), then run 3 more episodes only using
    #    evaluation workers 1-3 (evaluation workers 4-7 remain idle during that time).
    evaluation_num_env_runners=7,
)
# __rllib-adv_api_evaluation_3_end__


# __rllib-adv_api_evaluation_4_begin__
# Run evaluation and training at the same time via threading and make sure they roughly
# take the same time, such that the next `Algorithm.train()` call can execute
# immediately and not have to wait for a still ongoing (e.g. b/c of very long episodes)
# evaluation step:
config = AlgorithmConfig().evaluation(
    evaluation_interval=2,
    # run evaluation and training in parallel
    evaluation_parallel_to_training=True,
    # automatically end evaluation when train step has finished
    evaluation_duration="auto",
    evaluation_duration_unit="timesteps",  # <- this setting is ignored; RLlib
    # will always run by timesteps (not by complete
    # episodes) in this duration=auto mode
)
# __rllib-adv_api_evaluation_4_end__


# __rllib-adv_api_evaluation_5_begin__
# Switching off exploration behavior for evaluation workers
# (see rllib/algorithms/algorithm.py). Use any keys in this sub-dict that are
# also supported in the main Algorithm config.
config = AlgorithmConfig().evaluation(
    evaluation_config=AlgorithmConfig.overrides(explore=False),
)
# ... which is a more type-checked version of the old-style:
# config = AlgorithmConfig().evaluation(
#    evaluation_config={"explore": False},
# )
# __rllib-adv_api_evaluation_5_end__


# __rllib-adv_api_evaluation_6_begin__
# Having an environment that occasionally blocks completely for e.g. 10min would
# also affect (and block) training. Here is how you can defend your evaluation setup
# against oft-crashing or -stalling envs (or other unstable components on your evaluation
# workers).
config = AlgorithmConfig().evaluation(
    evaluation_interval=1,
    evaluation_parallel_to_training=True,
    evaluation_duration="auto",
    evaluation_duration_unit="timesteps",  # <- default anyway
    evaluation_force_reset_envs_before_iteration=True,  # <- default anyway
)
# __rllib-adv_api_evaluation_6_end__
