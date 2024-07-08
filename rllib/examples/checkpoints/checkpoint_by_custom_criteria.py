"""Example extracting a checkpoint from n trials using one or more custom criteria.

This example:
    - runs a CartPole experiment with three different learning rates (three tune
    "trials"). During the experiment, for each trial, we create a checkpoint at each
    iteration.
    - at the end of the experiment, we compare the trials and pick the one that
    performed best, based on the criterion: Lowest episode count per single iteration
    (for CartPole, a low episode count means the episodes are very long and thus the
    reward is also very high).
    - from that best trial (with the lowest episode count), we then pick those
    checkpoints that a) have the lowest policy loss (good) and b) have the highest value
    function loss (bad).


How to run this script
----------------------
`python [script file name].py --enable-new-api-stack`

For debugging, use the following additional command line options
`--no-tune --num-env-runners=0`
which should allow you to set breakpoints anywhere in the RLlib code and
have the execution stop there for inspection and debugging.

For logging to your WandB account, use:
`--wandb-key=[your WandB API key] --wandb-project=[some project name]
--wandb-run-name=[optional: WandB run name (within the defined project)]`


Results to expect
-----------------
In the console output, you can see the performance of the three different learning
rates used here:

+-----------------------------+------------+-----------------+--------+--------+
| Trial name                  | status     | loc             |     lr |   iter |
|-----------------------------+------------+-----------------+--------+--------+
| PPO_CartPole-v1_d7dbe_00000 | TERMINATED | 127.0.0.1:98487 | 0.01   |     17 |
| PPO_CartPole-v1_d7dbe_00001 | TERMINATED | 127.0.0.1:98488 | 0.001  |      8 |
| PPO_CartPole-v1_d7dbe_00002 | TERMINATED | 127.0.0.1:98489 | 0.0001 |      9 |
+-----------------------------+------------+-----------------+--------+--------+

+------------------+-------+----------+----------------------+----------------------+
|   total time (s) |    ts |   reward |   episode_reward_max |   episode_reward_min |
|------------------+-------+----------+----------------------+----------------------+
|          28.1068 | 39797 |   151.11 |                  500 |                   12 |
|          13.304  | 18728 |   158.91 |                  500 |                   15 |
|          14.8848 | 21069 |   167.36 |                  500 |                   13 |
+------------------+-------+----------+----------------------+----------------------+

+--------------------+
|   episode_len_mean |
|--------------------|
|             151.11 |
|             158.91 |
|             167.36 |
+--------------------+
"""

from ray import tune
from ray.rllib.core import DEFAULT_MODULE_ID
from ray.rllib.utils.metrics import (
    ENV_RUNNER_RESULTS,
    EPISODE_RETURN_MEAN,
    LEARNER_RESULTS,
)
from ray.rllib.utils.test_utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)
from ray.tune.registry import get_trainable_cls

parser = add_rllib_example_script_args(
    default_reward=450.0, default_timesteps=100000, default_iters=200
)


if __name__ == "__main__":
    args = parser.parse_args()

    # Force-set `args.checkpoint_freq` to 1.
    args.checkpoint_freq = 1

    # Simple generic config.
    base_config = (
        get_trainable_cls(args.algo)
        .get_default_config()
        .environment("CartPole-v1")
        # Run 3 trials, each w/ a different learning rate.
        .training(lr=tune.grid_search([0.01, 0.001, 0.0001]), train_batch_size=2341)
    )
    # Run tune for some iterations and generate checkpoints.
    results = run_rllib_example_script_experiment(base_config, args)

    # Get the best of the 3 trials by using some metric.
    # NOTE: Choosing the min `episodes_this_iter` automatically picks the trial
    # with the best performance (over the entire run (scope="all")):
    # The fewer episodes, the longer each episode lasted, the more reward we
    # got each episode.
    # Setting scope to "last", "last-5-avg", or "last-10-avg" will only compare
    # (using `mode=min|max`) the average values of the last 1, 5, or 10
    # iterations with each other, respectively.
    # Setting scope to "avg" will compare (using `mode`=min|max) the average
    # values over the entire run.
    metric = "env_runners/num_episodes"
    # notice here `scope` is `all`, meaning for each trial,
    # all results (not just the last one) will be examined.
    best_result = results.get_best_result(metric=metric, mode="min", scope="all")
    value_best_metric = best_result.metrics_dataframe[metric].min()
    best_return_best = best_result.metrics_dataframe[
        f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}"
    ].max()
    print(
        f"Best trial was the one with lr={best_result.metrics['config']['lr']}. "
        f"Reached lowest episode count ({value_best_metric}) in a single iteration and "
        f"an average return of {best_return_best}."
    )

    # Confirm, we picked the right trial.

    assert (
        value_best_metric
        == results.get_dataframe(filter_metric=metric, filter_mode="min")[metric].min()
    )

    # Get the best checkpoints from the trial, based on different metrics.
    # Checkpoint with the lowest policy loss value:
    if args.enable_new_api_stack:
        policy_loss_key = f"{LEARNER_RESULTS}/{DEFAULT_MODULE_ID}/policy_loss"
    else:
        policy_loss_key = "info/learner/default_policy/learner_stats/policy_loss"
    best_result = results.get_best_result(metric=policy_loss_key, mode="min")
    ckpt = best_result.checkpoint
    lowest_policy_loss = best_result.metrics_dataframe[policy_loss_key].min()
    print(f"Checkpoint w/ lowest policy loss ({lowest_policy_loss}): {ckpt}")

    # Checkpoint with the highest value-function loss:
    if args.enable_new_api_stack:
        vf_loss_key = f"{LEARNER_RESULTS}/{DEFAULT_MODULE_ID}/vf_loss"
    else:
        vf_loss_key = "info/learner/default_policy/learner_stats/vf_loss"
    best_result = results.get_best_result(metric=vf_loss_key, mode="max")
    ckpt = best_result.checkpoint
    highest_value_fn_loss = best_result.metrics_dataframe[vf_loss_key].max()
    print(f"Checkpoint w/ highest value function loss: {ckpt}")
    print(f"Highest value function loss: {highest_value_fn_loss}")
