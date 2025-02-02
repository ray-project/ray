"""Example showing how to customize an offline data pipeline.

This example:
    - demonstrates how you can customized your offline data pipeline.
    - shows how you can override the `OfflineData` to read raw image
    data and transform it into `numpy ` arrays.
    - explains how you can override the `OfflinePreLearner` to
    transform data further into `SingleAgentEpisode` instances that
    can be processes by the learner connector pipeline.

How to run this script
----------------------
`python [script file name].py --checkpoint-at-end`

For debugging, use the following additional command line options
`--no-tune --num-env-runners=0`
which should allow you to set breakpoints anywhere in the RLlib code and
have the execution stop there for inspection and debugging.

For logging to your WandB account, use:
`--wandb-key=[your WandB API key] --wandb-project=[some project name]
--wandb-run-name=[optional: WandB run name (within the defined project)]`

Results to expect
-----------------
2024-12-03 19:59:23,043 INFO streaming_executor.py:109 -- Execution plan
of Dataset: InputDataBuffer[Input] -> TaskPoolMapOperator[ReadBinary] ->
TaskPoolMapOperator[Map(map_to_numpy)] -> LimitOperator[limit=128]
✔️  Dataset execution finished in 10.01 seconds: 100%|███████████████████
███████████████████████████████████████████████████████████████████████|
3.00/3.00 [00:10<00:00, 3.34s/ row]
- ReadBinary->SplitBlocks(11): Tasks: 0; Queued blocks: 0; Resources: 0.0
CPU, 0.0B object store: 100%|█████████████████████████████████████████|
3.00/3.00 [00:10<00:00, 3.34s/ row]
- Map(map_to_numpy): Tasks: 0; Queued blocks: 0; Resources: 0.0 CPU,
0.0B object store: 100%|███████████████████████████████████████████████████|
3.00/3.00 [00:10<00:00, 3.34s/ row]
- limit=128: Tasks: 0; Queued blocks: 0; Resources: 0.0 CPU, 3.0KB object
store: 100%|██████████████████████████████████████████████████████████|
3.00/3.00 [00:10<00:00, 3.34s/ row]
Batch: {'batch': [MultiAgentBatch({}, env_steps=3)]}
"""

import gymnasium as gym
import numpy as np

from ray.rllib.algorithms.bc import BCConfig
from ray.rllib.algorithms.bc.bc_catalog import BCCatalog
from ray.rllib.algorithms.bc.torch.bc_torch_rl_module import BCTorchRLModule
from ray.rllib.core.rl_module.rl_module import RLModuleSpec, DefaultModelConfig
from ray.rllib.core.rl_module.multi_rl_module import MultiRLModuleSpec
from ray.rllib.examples.offline_rl.classes.image_offline_data import ImageOfflineData
from ray.rllib.examples.offline_rl.classes.image_offline_prelearner import (
    ImageOfflinePreLearner,
)

# Create an Algorithm configuration.
# TODO: Make this an actually running/learning example with RLunplugged
# data from S3 and add this to the CI.
config = (
    BCConfig()
    .environment(
        action_space=gym.spaces.Discrete(2),
        observation_space=gym.spaces.Box(0, 255, (32, 32, 3), np.float32),
    )
    .offline_data(
        input_=["s3://anonymous@ray-example-data/batoidea/JPEGImages/"],
        prelearner_class=ImageOfflinePreLearner,
    )
)

# Specify an `RLModule` and wrap it with a `MultiRLModuleSpec`. Note,
# on `Learner`` side any `RLModule` is an `MultiRLModule`.
module_spec = MultiRLModuleSpec(
    rl_module_specs={
        "default_policy": RLModuleSpec(
            model_config=DefaultModelConfig(
                conv_filters=[[16, 4, 2], [32, 4, 2], [64, 4, 2], [128, 4, 2]],
                conv_activation="relu",
            ),
            inference_only=False,
            module_class=BCTorchRLModule,
            catalog_class=BCCatalog,
            action_space=gym.spaces.Discrete(2),
            observation_space=gym.spaces.Box(0, 255, (32, 32, 3), np.float32),
        ),
    },
)

# Construct your `OfflineData` class instance.
offline_data = ImageOfflineData(config)

# Check, how the data is transformed. Note, the
# example dataset has only 3 such images.
batch = offline_data.data.take_batch(3)

# Construct your `OfflinePreLearner`.
offline_prelearner = ImageOfflinePreLearner(
    config=config,
    learner=None,
    spaces=(
        config.observation_space,
        config.action_space,
    ),
    module_spec=module_spec,
)

# Transform the raw data to `MultiAgentBatch` data.
batch = offline_prelearner(batch)

# Show the transformed batch.
print(f"Batch: {batch}")
