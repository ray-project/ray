import argparse
import os

from ray.rllib.examples.env.stateless_cartpole import StatelessCartPole
from ray.rllib.utils.test_utils import check_learning_achieved

parser = argparse.ArgumentParser()
parser.add_argument(
    "--run", type=str, default="PPO", help="The RLlib-registered algorithm to use."
)
parser.add_argument("--num-cpus", type=int, default=0)
parser.add_argument(
    "--framework",
    choices=["tf", "tf2", "torch"],
    default="tf",
    help="The DL framework specifier.",
)
parser.add_argument("--eager-tracing", action="store_true")
parser.add_argument("--use-prev-action", action="store_true")
parser.add_argument("--use-prev-reward", action="store_true")
parser.add_argument(
    "--as-test",
    action="store_true",
    help="Whether this script should be run as a test: --stop-reward must "
    "be achieved within --stop-timesteps AND --stop-iters.",
)
parser.add_argument(
    "--stop-iters", type=int, default=200, help="Number of iterations to train."
)
parser.add_argument(
    "--stop-timesteps", type=int, default=100000, help="Number of timesteps to train."
)
parser.add_argument(
    "--stop-reward", type=float, default=150.0, help="Reward at which we stop training."
)

if __name__ == "__main__":
    import ray
    from ray import air, tune

    args = parser.parse_args()

    ray.init(num_cpus=args.num_cpus or None)

    configs = {
        "PPO": {
            "num_sgd_iter": 5,
            "model": {
                "vf_share_layers": True,
            },
            "vf_loss_coeff": 0.0001,
        },
        "IMPALA": {
            "num_workers": 2,
            "num_gpus": 0,
            "vf_loss_coeff": 0.01,
        },
    }

    config = dict(
        configs[args.run],
        **{
            "env": StatelessCartPole,
            # Use GPUs iff `RLLIB_NUM_GPUS` env var set to > 0.
            "num_gpus": int(os.environ.get("RLLIB_NUM_GPUS", "0")),
            "model": {
                "use_lstm": True,
                "lstm_cell_size": 256,
                "lstm_use_prev_action": args.use_prev_action,
                "lstm_use_prev_reward": args.use_prev_reward,
            },
            "framework": args.framework,
            # Run with tracing enabled for tf2?
            "eager_tracing": args.eager_tracing,
        }
    )

    stop = {
        "training_iteration": args.stop_iters,
        "timesteps_total": args.stop_timesteps,
        "episode_reward_mean": args.stop_reward,
    }

    # To run the Algorithm without ``Tuner.fit``, using our LSTM model and
    # manual state-in handling, do the following:

    # Example (use `config` from the above code):
    # >> import numpy as np
    # >> from ray.rllib.algorithms.ppo import PPO
    # >>
    # >> algo = PPO(config)
    # >> lstm_cell_size = config["model"]["lstm_cell_size"]
    # >> env = StatelessCartPole()
    # >> obs = env.reset()
    # >>
    # >> # range(2) b/c h- and c-states of the LSTM.
    # >> init_state = state = [
    # ..     np.zeros([lstm_cell_size], np.float32) for _ in range(2)
    # .. ]
    # >> prev_a = 0
    # >> prev_r = 0.0
    # >>
    # >> while True:
    # >>     a, state_out, _ = algo.compute_single_action(
    # ..         obs, state, prev_a, prev_r)
    # >>     obs, reward, done, _ = env.step(a)
    # >>     if done:
    # >>         obs = env.reset()
    # >>         state = init_state
    # >>         prev_a = 0
    # >>         prev_r = 0.0
    # >>     else:
    # >>         state = state_out
    # >>         prev_a = a
    # >>         prev_r = reward

    tuner = tune.Tuner(
        args.run, param_space=config, run_config=air.RunConfig(stop=stop, verbose=2)
    )
    results = tuner.fit()

    if args.as_test:
        check_learning_achieved(results, args.stop_reward)
    ray.shutdown()
