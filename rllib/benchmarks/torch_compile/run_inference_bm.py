import argparse


import numpy as np
import matplotlib.pyplot as plt
import numpy as np
from typing import Union
import pandas as pd
import gymnasium as gym
import torch
import seaborn as sns 
import torch._dynamo as dynamo
from pathlib import Path
import json


from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.algorithms.ppo.torch.ppo_torch_rl_module import PPOTorchRLModule
from ray.rllib.algorithms.ppo.ppo_catalog import PPOCatalog
from ray.rllib.core.rl_module.rl_module import SingleAgentRLModuleSpec
from ray.rllib.core.rl_module.torch.torch_rl_module import TorchCompileConfig
from ray.rllib.models.catalog import MODEL_DEFAULTS
from ray.rllib.core.learner.learner import FrameworkHyperparameters, LearnerHyperparameters
from ray.rllib.core.learner.scaling_config import LearnerGroupScalingConfig
from ray.rllib.core.testing.utils import get_module_spec
from ray.rllib.algorithms.ppo.torch.ppo_torch_learner import PPOTorchLearner
from ray.rllib.utils.test_utils import check
from ray.rllib.algorithms.ppo.ppo import PPOConfig
from ray.rllib.env.wrappers.atari_wrappers import wrap_deepmind
from ray.rllib.utils.torch_utils import convert_to_torch_tensor

from ray.rllib.benchmarks.torch_compile.utils import get_ppo_batch_for_env, timed

sns.set_style("darkgrid")

def _parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-size", "-bs", type=int, default=1, help="Batch size")
    parser.add_argument("--num-iters", "-n", type=int, default=600, help="Number of iterations")
    parser.add_argument("--backend", type=str, default="cudagraphs", help="torch dynamo backend")
    parser.add_argument("--mode", type=str, default=None, help="torch dynamo mode")
    parser.add_argument("--output", type=str, default="./outputs", help="output directory")
    parser.add_argument("--burn-in", type=int, default=100, help="burn-in iterations")

    return parser.parse_args()


def plot_results(*, results: dict, output: str, config: dict):
    pass


def main(pargs):
    
    if not torch.cuda.is_available():
        raise RuntimeError("CUDA is required for this benchmark.")
    
    # create the output directory and save the config
    exp_name_pargs = {
        "bs": pargs.batch_size,
        "backend": pargs.backend,
    }
    suffix_list = [f"{k}-{v}" for k, v in exp_name_pargs.items()]
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
        model_config_dict=model_cfg 
    )

    eager_module = spec.build().cuda()
    compiled_module = spec.build().cuda()
    compile_config = TorchCompileConfig(
        torch_dynamo_backend=pargs.backend,
        torch_dynamo_mode=pargs.mode
    )
    compiled_module = compiled_module.compile(compile_config)

    batch = get_ppo_batch_for_env(env, batch_size=pargs.batch_size)
    
    # Burn-in
    print("Burn-in...")
    for _ in range(pargs.burn_in):
        with torch.no_grad():
            eager_module.forward_exploration(batch)
            compiled_module.forward_exploration(batch)
    print("Burn-in done.")

    # Eager
    print("Eager...")
    eager_times = []
    for _ in range(pargs.num_iters):
        fn = lambda: eager_module.forward_exploration(batch)
        _, t = timed(fn)
        eager_times.append(t)
    eager_throughputs = 1 / np.array(eager_times)
    print("Eager done.")

    # Compiled
    print("Compiled...")
    compiled_times = []
    for _ in range(pargs.num_iters):
        fn = lambda: compiled_module.forward_exploration(batch)
        _, t = timed(fn)
        compiled_times.append(t)  
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
        "eager_throughputs": eager_throughputs,
        "compiled_throughputs": compiled_throughputs
    }

    with open(output / "results.json", "w") as f:
        json.dump(results, f)

    # Plot
    plot_results(results=results, output=output, config=config)
    


if __name__ == "__main__":
    main(_parse_args())
