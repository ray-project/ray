from typing import Type, Union, Dict, Tuple

import numpy as np
from ray.rllib.algorithms import AlgorithmConfig
from ray.rllib.evaluation.worker_set import WorkerSet
from ray.rllib.examples.env.cliff_walking_wall_env import CliffWalkingWallEnv
from ray.rllib.examples.policy.cliff_walking_wall_policy import CliffWalkingWallPolicy
from ray.rllib.execution.rollout_ops import synchronous_parallel_sample
from ray.rllib.offline.estimators import (
    DirectMethod,
    DoublyRobust,
)
from ray.rllib.policy import Policy
from ray.rllib.policy.sample_batch import (
    SampleBatch,
    concat_samples,
    convert_ma_batch_to_sample_batch,
)
from ray.rllib.utils.debug import update_global_seed_if_necessary


def get_cliff_walking_wall_policy_and_data(
    num_episodes: int, gamma: float, epsilon: float, seed: int
) -> Tuple[Policy, SampleBatch, float, float]:
    """Collect a cliff_walking_wall policy and data with epsilon-greedy exploration.

    Args:
        num_episodes: Minimum number of episodes to collect
        gamma: discount factor
        epsilon: epsilon-greedy exploration value

    Returns:
        A Tuple consisting of:
          - A CliffWalkingWallPolicy with exploration parameter epsilon
          - A SampleBatch of at least `num_episodes` CliffWalkingWall episodes
          collected using epsilon-greedy exploration
          - The mean of the discounted return over the collected episodes
          - The stddev of the discounted return over the collected episodes

    """

    config = (
        AlgorithmConfig()
        .debugging(seed=seed)
        .rollouts(batch_mode="complete_episodes")
        .environment(disable_env_checking=True)
        .experimental(_disable_preprocessor_api=True)
    )
    config = config.to_dict()
    config["epsilon"] = epsilon

    env = CliffWalkingWallEnv(seed=seed)
    policy = CliffWalkingWallPolicy(
        env.observation_space, env.action_space, {"epsilon": epsilon, "seed": seed}
    )
    workers = WorkerSet(
        env_creator=lambda env_config: CliffWalkingWallEnv(),
        default_policy_class=CliffWalkingWallPolicy,
        config=config,
        num_workers=4,
    )
    ep_ret = []
    batches = []
    n_eps = 0
    while n_eps < num_episodes:
        batch = synchronous_parallel_sample(worker_set=workers)
        batch = convert_ma_batch_to_sample_batch(batch)

        for episode in batch.split_by_episode():
            ret = 0
            for r in episode[SampleBatch.REWARDS][::-1]:
                ret = r + gamma * ret
            ep_ret.append(ret)
            n_eps += 1
        batches.append(batch)
    workers.stop()

    return policy, concat_samples(batches), np.mean(ep_ret), np.std(ep_ret)


def check_estimate(
    *,
    estimator_cls: Type[Union[DirectMethod, DoublyRobust]],
    gamma: float,
    q_model_config: Dict,
    policy: Policy,
    batch: SampleBatch,
    mean_ret: float,
    std_ret: float,
    seed: int,
) -> None:
    """Compute off-policy estimates and compare them to the true discounted return.

    Args:
        estimator_cls: Off-Policy Estimator class to be used
        gamma: discount factor
        q_model_config: Optional config settings for the estimator's Q-model
        policy: The target policy we compute estimates for
        batch: The behavior data we use for off-policy estimation
        mean_ret: The mean discounted episode return over the batch
        std_ret: The standard deviation corresponding to mean_ret

    Raises:
        AssertionError if the estimated mean episode return computed by
        the off-policy estimator does not fall within one standard deviation of
        the values specified above i.e. [mean_ret - std_ret, mean_ret + std_ret]
    """
    # only torch is supported for now
    update_global_seed_if_necessary(framework="torch", seed=seed)
    estimator = estimator_cls(
        policy=policy,
        gamma=gamma,
        q_model_config=q_model_config,
    )
    loss = estimator.train(batch)["loss"]
    estimates = estimator.estimate(batch)
    est_mean = estimates["v_target"]
    est_std = estimates["v_target_std"]
    print(
        f"est_mean={est_mean:.2f}, "
        f"est_std={est_std:.2f}, "
        f"target_mean={mean_ret:.2f}, "
        f"target_std={std_ret:.2f}, "
        f"loss={loss:.2f}"
    )
    # Assert that the two mean +- stddev intervals overlap
    assert mean_ret - std_ret <= est_mean <= mean_ret + std_ret, (
        f"OPE estimate {est_mean:.2f} with stddev "
        f"{est_std:.2f} does not converge to true discounted return "
        f"{mean_ret:.2f} with stddev {std_ret:.2f}!"
    )
