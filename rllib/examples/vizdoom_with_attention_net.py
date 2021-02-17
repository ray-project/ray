import argparse
import os

parser = argparse.ArgumentParser()
parser.add_argument("--run", type=str, default="PPO")
parser.add_argument("--num-cpus", type=int, default=0)
parser.add_argument(
    "--framework", choices=["tf2", "tf", "tfe", "torch"], default="tf")
parser.add_argument("--use-prev-action", action="store_true")
parser.add_argument("--use-prev-reward", action="store_true")
parser.add_argument("--stop-iters", type=int, default=200)
parser.add_argument("--stop-timesteps", type=int, default=100000)
parser.add_argument("--stop-reward", type=float, default=150.0)

if __name__ == "__main__":
    import ray
    from ray import tune

    args = parser.parse_args()

    ray.init(num_cpus=args.num_cpus or None, local_mode=True)#TODO

    config = {
        "env": "VizdoomBasic-v0",
        # Use GPUs iff `RLLIB_NUM_GPUS` env var set to > 0.
        "num_gpus": int(os.environ.get("RLLIB_NUM_GPUS", "0")),
        "model": {
            "conv_filters": [],
            "use_attention": True,
            "attention_num_transformer_units": 1,
            "attention_dim": 64,
            "attention_num_heads": 2,
            "attention_memory_inference": 100,
            "attention_memory_training": 50,
            "vf_share_layers": True,
        },
        "framework": args.framework,
        # Run with tracing enabled for tfe/tf2.
        "eager_tracing": args.framework in ["tfe", "tf2"],

        "num_workers": 0,#TODO
    }

    stop = {
        "training_iteration": args.stop_iters,
        "timesteps_total": args.stop_timesteps,
        "episode_reward_mean": args.stop_reward,
    }

    results = tune.run(args.run, config=config, stop=stop, verbose=2)
    print(results)
    ray.shutdown()
