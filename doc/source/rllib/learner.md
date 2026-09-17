---
myst:
  html_meta:
    description: "The Learner API that applies gradient and non-gradient updates to RLModules, with DDP-based distribution, state access, and checkpointing."
---

(learner-guide)=

# Learner (Alpha)

The {py:class}`~ray.rllib.core.learner.learner.Learner` class abstracts the training logic of RLModules. It supports both gradient-based and non-gradient-based updates, such as polyak averaging. You can distribute the Learner with data-distributed parallel (DDP). The Learner does the following:

- Facilitates gradient-based updates on {ref}`RLModule <rlmodule-guide>`.
- Provides abstractions for non-gradient-based updates such as polyak averaging.
- Reports training statistics.
- Checkpoints the modules and optimizer states for durable training.

The {py:class}`~ray.rllib.core.learner.learner.Learner` class supports data-distributed parallel training through the {py:class}`~ray.rllib.core.learner.learner_group.LearnerGroup` API. The {py:class}`~ray.rllib.core.learner.learner_group.LearnerGroup` maintains multiple copies of the same {py:class}`~ray.rllib.core.learner.learner.Learner` with identical parameters and hyperparameters. Each {py:class}`~ray.rllib.core.learner.learner.Learner` instance computes the loss and gradients on a shard of a sample batch, then accumulates the gradients across instances. For more about data-distributed parallel learning, see the [PyTorch DDP tutorial](https://pytorch.org/tutorials/intermediate/ddp_tutorial.html).

The {py:class}`~ray.rllib.core.learner.learner_group.LearnerGroup` also supports asynchronous training and distributed checkpointing for durability during training.

# Enable the Learner API in RLlib experiments

Adjust the training resources through the `num_gpus_per_learner`, `num_cpus_per_learner`, and `num_learners` arguments in {py:class}`~ray.rllib.algorithms.algorithm_config.AlgorithmConfig`.

```{testcode}
:hide:

from ray.rllib.algorithms.ppo.ppo import PPOConfig
```

```{testcode}
config = (
    PPOConfig()
    .learners(
        num_learners=0,  # Set this to greater than 1 to allow for DDP style updates.
        num_gpus_per_learner=0,  # Set this to 1 to enable GPU training.
        num_cpus_per_learner=1,
    )
)
```

```{testcode}
:hide:

config = config.environment("CartPole-v1")
config.build()  # test that the algorithm can be built with the given resources
```

:::{note}

This feature is in alpha. If you migrate to this algorithm, enable the feature through `AlgorithmConfig.api_stack(enable_rl_module_and_learner=True, enable_env_runner_and_connector_v2=True)`.

The following algorithms support {py:class}`~ray.rllib.core.learner.learner.Learner` out of the box. To use this API with other algorithms, implement a custom {py:class}`~ray.rllib.core.learner.learner.Learner`.

```{list-table}
:header-rows: 1
:widths: 60 60

* - Algorithm
  - Supported framework
* - **PPO**
  - <img src="images/pytorch.png" class="inline-figure" width="16" alt="pytorch"> <img src="images/tensorflow.png" class="inline-figure" width="16" alt="tensorflow">
* - **IMPALA**
  - <img src="images/pytorch.png" class="inline-figure" width="16" alt="pytorch"> <img src="images/tensorflow.png" class="inline-figure" width="16" alt="tensorflow">
* - **APPO**
  - <img src="images/pytorch.png" class="inline-figure" width="16" alt="pytorch"> <img src="images/tensorflow.png" class="inline-figure" width="16" alt="tensorflow">
```

:::

# Basic usage

Use the {py:class}`~ray.rllib.core.learner.learner_group.LearnerGroup` utility to interact with multiple learners.

## Construction

If you enable the {ref}`RLModule <rlmodule-guide>` and {py:class}`~ray.rllib.core.learner.learner.Learner` APIs through {py:class}`~ray.rllib.algorithms.algorithm_config.AlgorithmConfig`, then calling {py:meth}`~ray.rllib.algorithms.algorithm_config.AlgorithmConfig.build_algo` constructs a {py:class}`~ray.rllib.core.learner.learner_group.LearnerGroup` for you. If you use these APIs standalone, construct the {py:class}`~ray.rllib.core.learner.learner_group.LearnerGroup` as follows:

```{testcode}
:hide:

# imports for the examples
import gymnasium as gym
import numpy as np

import ray
from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.core.rl_module.rl_module import RLModuleSpec
from ray.rllib.core.learner.learner_group import LearnerGroup
```

::::{tab-set}

:::{tab-item} Constructing a LearnerGroup

```{testcode}
env = gym.make("CartPole-v1")

# Create an AlgorithmConfig object from which we can build the
# LearnerGroup.
config = (
    PPOConfig()
    # Number of Learner workers (Ray actors).
    # Use 0 for no actors, only create a local Learner.
    # Use >=1 to create n DDP-style Learner workers (Ray actors).
    .learners(num_learners=1)
    # Specify the learner's hyperparameters.
    .training(
        use_kl_loss=True,
        kl_coeff=0.01,
        kl_target=0.05,
        clip_param=0.2,
        vf_clip_param=0.2,
        entropy_coeff=0.05,
        vf_loss_coeff=0.5
    )
)

# Construct a new LearnerGroup using our config object.
learner_group = config.build_learner_group(env=env)
```

:::

:::{tab-item} Constructing a Learner

```{testcode}
env = gym.make("CartPole-v1")

# Create an AlgorithmConfig object from which we can build the
# Learner.
config = (
    PPOConfig()
    # Specify the Learner's hyperparameters.
    .training(
        use_kl_loss=True,
        kl_coeff=0.01,
        kl_target=0.05,
        clip_param=0.2,
        vf_clip_param=0.2,
        entropy_coeff=0.05,
        vf_loss_coeff=0.5
    )
)
# Construct a new Learner using our config object.
learner = config.build_learner(env=env)

# Needs to be called on the learner before calling any functions.
learner.build()
```

:::

::::

## Updates

```{testcode}
:hide:

import time

from ray.rllib.core import DEFAULT_MODULE_ID
from ray.rllib.evaluation.postprocessing import Postprocessing
from ray.rllib.policy.sample_batch import SampleBatch, MultiAgentBatch

DUMMY_BATCH = {
    SampleBatch.OBS: np.array(
        [[0.1, 0.2, 0.3, 0.4], [0.5, 0.6, 0.7, 0.8], [0.9, 1.0, 1.1, 1.2]],
        dtype=np.float32,
    ),
    SampleBatch.NEXT_OBS: np.array(
        [[0.1, 0.2, 0.3, 0.4], [0.5, 0.6, 0.7, 0.8], [0.9, 1.0, 1.1, 1.2]],
        dtype=np.float32,
    ),
    SampleBatch.ACTIONS: np.array([0, 1, 1]),
    SampleBatch.PREV_ACTIONS: np.array([0, 1, 1]),
    SampleBatch.REWARDS: np.array([1.0, -1.0, 0.5], dtype=np.float32),
    SampleBatch.PREV_REWARDS: np.array([1.0, -1.0, 0.5], dtype=np.float32),
    SampleBatch.TERMINATEDS: np.array([False, False, True]),
    SampleBatch.TRUNCATEDS: np.array([False, False, False]),
    SampleBatch.VF_PREDS: np.array([0.5, 0.6, 0.7], dtype=np.float32),
    SampleBatch.ACTION_DIST_INPUTS: np.array(
        [[-2.0, 0.5], [-3.0, -0.3], [-0.1, 2.5]], dtype=np.float32
    ),
    SampleBatch.ACTION_LOGP: np.array([-0.5, -0.1, -0.2], dtype=np.float32),
    SampleBatch.EPS_ID: np.array([0, 0, 0]),
    SampleBatch.AGENT_INDEX: np.array([0, 0, 0]),
    Postprocessing.ADVANTAGES: np.array([0.1, 0.2, 0.3], dtype=np.float32),
    Postprocessing.VALUE_TARGETS: np.array([0.5, 0.6, 0.7], dtype=np.float32),
}
default_batch = SampleBatch(DUMMY_BATCH)
DUMMY_BATCH = default_batch.as_multi_agent()
# Make sure, we convert the batch to the correct framework (here: torch).
DUMMY_BATCH = learner._convert_batch_type(DUMMY_BATCH)
```

::::{tab-set}

:::{tab-item} Updating a LearnerGroup

```{testcode}
TIMESTEPS = {"num_env_steps_sampled_lifetime": 250}

# This is a blocking update.
results = learner_group.update(batch=DUMMY_BATCH, timesteps=TIMESTEPS)

# This is a non-blocking update. The results are returned in a future
# call to `update(..., async_update=True)`
_ = learner_group.update(batch=DUMMY_BATCH, async_update=True, timesteps=TIMESTEPS)

# Artificially wait for async request to be done to get the results
# in the next call to
# `LearnerGroup.update(..., async_update=True)`.
time.sleep(5)
results = learner_group.update(
    batch=DUMMY_BATCH, async_update=True, timesteps=TIMESTEPS
)
# `results` is a list of n result dicts from various Learner actors.
assert isinstance(results, list), results
assert isinstance(results[0], dict), results
```

When updating a {py:class}`~ray.rllib.core.learner.learner_group.LearnerGroup`, you can perform blocking or async updates on batches of data. Async updates are necessary for implementing async algorithms such as APPO or IMPALA.

:::

:::{tab-item} Updating a Learner

```{testcode}
# This is a blocking update (given a training batch).
result = learner.update(batch=DUMMY_BATCH, timesteps=TIMESTEPS)
```

When updating a {py:class}`~ray.rllib.core.learner.learner.Learner`, you can only perform blocking updates on batches of data. You can perform non-gradient-based updates before or after the gradient-based ones by overriding {py:meth}`~ray.rllib.core.learner.learner.Learner.before_gradient_based_update` and {py:meth}`~ray.rllib.core.learner.learner.Learner.after_gradient_based_update`.

:::

::::

## Getting and setting state

::::{tab-set}

:::{tab-item} Getting and setting state for a LearnerGroup

```{testcode}
# Get the LearnerGroup's RLModule weights and optimizer states.
state = learner_group.get_state()
learner_group.set_state(state)

# Only get the RLModule weights.
weights = learner_group.get_weights()
learner_group.set_weights(weights)
```

Set or get the state dict of all learners through `LearnerGroup.set_state` or `LearnerGroup.get_state`. The state includes the neural network weights and the optimizer states on each learner. For example, an Adam optimizer's state holds momentum information from recent gradients. To get or set only the weights of the RLModules of all learners, use the `LearnerGroup.get_weights` and `LearnerGroup.set_weights` APIs.

:::

:::{tab-item} Getting and setting state for a Learner

```{testcode}
from ray.rllib.core import COMPONENT_RL_MODULE

# Get the Learner's RLModule weights and optimizer states.
state = learner.get_state()
# Note that `state` is now a dict:
# {
#    COMPONENT_RL_MODULE: [RLModule's state],
#    COMPONENT_OPTIMIZER: [Optimizer states],
# }
learner.set_state(state)

# Only get the RLModule weights (as numpy, not torch/tf).
rl_module_only_state = learner.get_state(components=COMPONENT_RL_MODULE)
# Note that `rl_module_only_state` is now a dict:
# {COMPONENT_RL_MODULE: [RLModule's state]}
learner.module.set_state(rl_module_only_state)
```

Set and get the entire state of a {py:class}`~ray.rllib.core.learner.learner.Learner` with {py:meth}`~ray.rllib.core.learner.learner.Learner.set_state` and {py:meth}`~ray.rllib.core.learner.learner.Learner.get_state`. To get only the RLModule's weights without the optimizer states, use the `components=COMPONENT_RL_MODULE` argument in {py:meth}`~ray.rllib.core.learner.learner.Learner.get_state`, as the preceding code shows. To set only the RLModule's weights without touching the optimizer states, use {py:meth}`~ray.rllib.core.learner.learner.Learner.get_state` and pass in a dict, `{COMPONENT_RL_MODULE: [RLModule's state]}`, as the preceding code shows.

:::

::::

```{testcode}
:hide:

import tempfile

LEARNER_CKPT_DIR = tempfile.mkdtemp()
LEARNER_GROUP_CKPT_DIR = tempfile.mkdtemp()
```

## Checkpointing

::::{tab-set}

:::{tab-item} Checkpointing a LearnerGroup

```{testcode}
learner_group.save_to_path(LEARNER_GROUP_CKPT_DIR)
learner_group.restore_from_path(LEARNER_GROUP_CKPT_DIR)
```

Checkpoint the state of all learners in the {py:class}`~ray.rllib.core.learner.learner_group.LearnerGroup` through {py:meth}`~ray.rllib.core.learner.learner_group.LearnerGroup.save_to_path` and restore the state of a saved {py:class}`~ray.rllib.core.learner.learner_group.LearnerGroup` through {py:meth}`~ray.rllib.core.learner.learner_group.LearnerGroup.restore_from_path`. A LearnerGroup's state includes the neural network weights and all optimizer states. Because the state of all {py:class}`~ray.rllib.core.learner.learner.Learner` instances is identical, RLlib saves only the state from the first {py:class}`~ray.rllib.core.learner.learner.Learner`.

:::

:::{tab-item} Checkpointing a Learner

```{testcode}
learner.save_to_path(LEARNER_CKPT_DIR)
learner.restore_from_path(LEARNER_CKPT_DIR)
```

Checkpoint the state of a {py:class}`~ray.rllib.core.learner.learner.Learner` through {py:meth}`~ray.rllib.core.learner.learner.Learner.save_to_path` and restore the state of a saved {py:class}`~ray.rllib.core.learner.learner.Learner` through {py:meth}`~ray.rllib.core.learner.learner.Learner.restore_from_path`. A Learner's state includes the neural network weights and all optimizer states.

:::

::::

# Implementation

The {py:class}`~ray.rllib.core.learner.learner.Learner` class has many APIs for flexible implementation. The core ones you need to implement are:

```{list-table}
:widths: 60 60
:header-rows: 1

* - Method
  - Description
* - {py:meth}`~ray.rllib.core.learner.learner.Learner.configure_optimizers_for_module()`
  - Set up the optimizers for an RLModule.
* - {py:meth}`~ray.rllib.core.learner.learner.Learner.compute_loss_for_module()`
  - Calculate the loss for a gradient-based update to a module.
* - {py:meth}`~ray.rllib.core.learner.learner.Learner.before_gradient_based_update()`
  - Do non-gradient-based updates to an RLModule before the gradient-based ones, such as adding noise to your network.
* - {py:meth}`~ray.rllib.core.learner.learner.Learner.after_gradient_based_update()`
  - Do non-gradient-based updates to an RLModule after the gradient-based ones, such as updating a loss coefficient based on a schedule.
```

## Starter example

A {py:class}`~ray.rllib.core.learner.learner.Learner` that implements behavior cloning could look like the following:

```{testcode}
:hide:

from typing import Any, Dict, DefaultDict

from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.core.learner.learner import Learner
from ray.rllib.core.learner.torch.torch_learner import TorchLearner
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.typing import ModuleID, TensorType
```

```{testcode}
class BCTorchLearner(TorchLearner):

    @override(Learner)
    def compute_loss_for_module(
        self,
        *,
        module_id: ModuleID,
        config: AlgorithmConfig = None,
        batch: Dict[str, Any],
        fwd_out: Dict[str, TensorType],
    ) -> TensorType:

        # standard behavior cloning loss
        action_dist_inputs = fwd_out[SampleBatch.ACTION_DIST_INPUTS]
        action_dist_class = self._module[module_id].get_train_action_dist_cls()
        action_dist = action_dist_class.from_logits(action_dist_inputs)
        loss = -torch.mean(action_dist.logp(batch[SampleBatch.ACTIONS]))

        return loss
```
