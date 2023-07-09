import argparse

import matplotlib.pyplot as plt

import gymnasium as gym
from pathlib import Path
import numpy as np
import json
import tqdm

import torch

from ray.rllib.algorithms.ppo.torch.ppo_torch_rl_module import PPOTorchRLModule
from ray.rllib.algorithms.ppo.ppo_catalog import PPOCatalog
from ray.rllib.benchmarks.torch_compile.utils import get_ppo_batch_for_env, timed
from ray.rllib.core.rl_module.rl_module import SingleAgentRLModuleSpec
from ray.rllib.core.rl_module.torch.torch_rl_module import TorchCompileConfig
from ray.rllib.env.wrappers.atari_wrappers import wrap_deepmind
from ray.rllib.models.catalog import MODEL_DEFAULTS
from ray.rllib.utils.torch_utils import convert_to_torch_tensor

# This is needed for performance reasons under inductor backend
torch.set_float32_matmul_precision("high")


def _parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-size", "-bs", type=int, default=1, help="Batch size")
    parser.add_argument(
        "--num-iters", "-n", type=int, default=5000, help="Number of iterations"
    )
    parser.add_argument(
        "--backend", type=str, default="cudagraphs", help="torch dynamo backend"
    )
    parser.add_argument("--mode", type=str, default=None, help="torch dynamo mode")
    parser.add_argument(
        "--output", type=str, default="./outputs", help="output directory"
    )
    parser.add_argument("--burn-in", type=int, default=500, help="burn-in iterations")
    parser.add_argument("--cpu", action="store_true", help="use CPU")
    parser.add_argument("--smoke-test", action="store_true", help="smoke test")

    return parser.parse_args()


def plot_results(*, results: dict, output: str, config: dict):

    eager_throughputs = results["eager_throughputs"]
    compiled_throughputs = results["compiled_throughputs"]
    batch_size = config["batch_size"]

    upper_limit = max(np.concatenate([eager_throughputs, compiled_throughputs]))

    fig, axes = plt.subplots(nrows=1, ncols=2, figsize=(10, 3))
    axes[0].plot(eager_throughputs)
    axes[0].set_title(f"Eager num_iters / sec, batch size {batch_size}")
    axes[0].set_ylim(0, upper_limit)
    axes[1].plot(compiled_throughputs)
    axes[1].set_title(f"Compile num_iters / sec, batch size {batch_size}")
    axes[1].set_ylim(0, upper_limit)
    fig.tight_layout()
    plt.savefig(output / "throughputs.png")
    plt.clf()


def main(pargs):

    use_cpu = pargs.cpu or pargs.smoke_test

    if not torch.cuda.is_available() and not (hasattr(torch.backends, 'mps') and torch.backends.mps.is_available()) and not use_cpu:
        raise RuntimeError(
            "GPU is required for this benchmark. Please run with --cpu flag."
        )

    # create the output directory and save the config
    exp_name_pargs = {
        "bs": pargs.batch_size,
        "backend": pargs.backend,
        "mode": pargs.mode,
    }

    suffix_list = [f"{k}-{v}" for k, v in exp_name_pargs.items()]
    if pargs.cpu:
        suffix_list.append("cpu")

    suffix = "-".join(suffix_list)
    output = Path(pargs.output) / f"inference_{suffix}"
    output.mkdir(parents=True, exist_ok=True)

    config = vars(pargs)
    with open(output / "args.json", "w") as f:
        json.dump(config, f)

    # create the environment
    env = wrap_deepmind(gym.make("GymV26Environment-v0", env_id="ALE/Breakout-v5"))

    # setup RLModule
    model_cfg = MODEL_DEFAULTS.copy()
    spec = SingleAgentRLModuleSpec(
        module_class=PPOTorchRLModule,
        observation_space=env.observation_space,
        action_space=env.action_space,
        catalog_class=PPOCatalog,
        model_config_dict=model_cfg,
    )
    device = torch.device("cuda" if not use_cpu else "cpu")

    eager_module = spec.build().to(device)
    compiled_module = spec.build().to(device)
    compile_config = TorchCompileConfig(
        torch_dynamo_backend=pargs.backend,
        torch_dynamo_mode=pargs.mode,
    )
    compiled_module = compiled_module.compile(compile_config)

    batch = get_ppo_batch_for_env(env, batch_size=pargs.batch_size)
    batch = convert_to_torch_tensor(batch, device=device)

    burn_in = 2 if pargs.smoke_test else pargs.burn_in
    num_iters = 10 if pargs.smoke_test else pargs.num_iters

    # Burn-in
    print("Burn-in...")
    for _ in tqdm.tqdm(range(burn_in)):
        with torch.no_grad():
            eager_module.forward_exploration(batch)
            compiled_module.forward_exploration(batch)
    print("Burn-in done.")

    # Eager
    print("Eager...")
    eager_times = []
    for _ in tqdm.tqdm(range(num_iters)):
        _, t = timed(
            lambda: eager_module.forward_exploration(batch), use_cuda=not use_cpu
        )
        eager_times.append(t)
    eager_throughputs = 1 / np.array(eager_times)
    print("Eager done.")

    # Compiled
    print("Compiled...")
    compiled_times = []
    for _ in tqdm.tqdm(range(num_iters)):
        _, t = timed(
            lambda: compiled_module.forward_exploration(batch), use_cuda=not use_cpu
        )
        compiled_times.append(t)
        pass
    compiled_throughputs = 1 / np.array(compiled_times)
    print("Compiled done.")

    # Results
    eager_throughput_median = np.median(eager_throughputs)
    compiled_throughput_median = np.median(compiled_throughputs)
    speedup = compiled_throughput_median / eager_throughput_median - 1
    results = {
        "eager_throughput_median": eager_throughput_median,
        "compiled_throughput_median": compiled_throughput_median,
        "speedup": speedup,
        "eager_throughputs": eager_throughputs.tolist(),
        "compiled_throughputs": compiled_throughputs.tolist(),
    }

    with open(output / "results.json", "w") as f:
        json.dump(results, f)

    # Plot
    plot_results(results=results, output=output, config=config)


if __name__ == "__main__":
    main(_parse_args())
