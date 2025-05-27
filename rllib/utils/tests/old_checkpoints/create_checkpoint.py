"""Use this script to create "legacy" msgpack-checkpoints in the same directory.

Backward compatibility tests of future Ray and RLlib versions check for compatibility
with these legacy msgpack checkpoints.

Run this script roughly once per Ray release and save the resulting checkpoint
.zip file into a newly created `ray_[major]_[minor]` subdirectory.

The CI test in `test_checkpointable.py` can then loop through all the Ray version
subdirectories and try to restore the original Algo's state and continue training.
"""

import argparse
from pathlib import Path
import importlib
import random
import shutil

from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.core.rl_module.rl_module import RLModuleSpec

parser = argparse.ArgumentParser()
parser.add_argument("--ray-version", type=str, default="ray_2_40")

args = parser.parse_args()


# Import the config for building the algo.
old_config_module = importlib.import_module(
    f"ray.rllib.utils.tests.old_checkpoints.{args.ray_version}.old_config"
)
config = old_config_module.config

# Build the algo.
algo = config.build()
p0_module = algo.get_module("p0")

# Train for one iteration.
print(algo.train())

# Add a new RLModule to the algo.
algo.add_module(
    "p2",
    RLModuleSpec.from_module(p0_module),
    config_overrides=PPOConfig.overrides(lr=0.0002),
    new_agent_to_module_mapping_fn=(
        lambda aid, *arg, **kw: "p0" if aid == 0 else random.choice(["p1", "p2"])
    ),
)
print(algo.train())

# Add a new RLModule (non-trainable) to the algo.
algo.add_module(
    "p3",
    RLModuleSpec.from_module(p0_module),
    config_overrides=PPOConfig.overrides(lr=0.0003),
    new_agent_to_module_mapping_fn=(
        lambda aid, *arg, **kw: "p0" if aid == 0 else random.choice(["p1", "p2", "p3"])
    ),
    # p3 should NOT be trained.
    new_should_module_be_updated=["p0", "p1", "p2"],
)
print(algo.train())

# Remove one of the original RLModules of the algo.
algo.remove_module(
    "p0",
    new_agent_to_module_mapping_fn=lambda aid, *arg, **kw: f"p{aid+1}",
    # p0 (non-existent) and p3 should NOT be trained.
    new_should_module_be_updated=["p1", "p2"],
)
print(algo.train())

# Re-add the removed RLModule.
algo.add_module(
    "p0",
    RLModuleSpec.from_module(p0_module),
    config_overrides=PPOConfig.overrides(lr=0.00005),
    new_agent_to_module_mapping_fn=(
        lambda aid, *arg, **kw: "p0" if aid == 0 else random.choice(["p1", "p2", "p3"])
    ),
    # p3 should NOT be trained.
    new_should_module_be_updated=["p0", "p1", "p2"],
)
print(algo.train())

# Create the algo checkpoint to be tested (using msgpack) in this very directory.
this_path = Path(__file__).parent.resolve()
checkpoint_path = this_path / "tmp"
checkpoint_zip = this_path / "checkpoint.zip"
algo.save_to_path(path=checkpoint_path, use_msgpack=True)

# Zip up checkpoint contents.
shutil.make_archive(str(this_path / "checkpoint"), "zip", checkpoint_path)
shutil.rmtree(checkpoint_path)
shutil.move(checkpoint_zip, this_path / args.ray_version)

print(f"Algorithm checkpoint created at\n{checkpoint_zip}")
