---
myst:
  html_meta:
    description: "ConnectorV2 pipelines that transform data between environment, module, and learner, covering the three pipeline types and batch construction phases."
---

(connector-v2-docs)=

::::{grid} 1 2 3 4
:gutter: 1
:class-container: container pb-3

:::{grid-item-card}
:img-top: /rllib/images/connector_v2/connector_generic.svg
:class-img-top: pt-2 w-75 d-block mx-auto fixed-height-img

```{button-ref} connector-v2-docs

ConnectorV2 overview (this page)
```
:::

:::{grid-item-card}
:img-top: /rllib/images/connector_v2/env_to_module_connector.svg
:class-img-top: pt-2 w-75 d-block mx-auto fixed-height-img

```{button-ref} env-to-module-pipeline-docs

Env-to-module pipelines
```
:::

:::{grid-item-card}
:img-top: /rllib/images/connector_v2/learner_connector.svg
:class-img-top: pt-2 w-75 d-block mx-auto fixed-height-img

```{button-ref} learner-pipeline-docs

Learner connector pipelines
```
:::

::::

# ConnectorV2 and ConnectorV2 pipelines

```{toctree}
:hidden:

env-to-module-connector
learner-connector
```

RLlib stores and transports all trajectory data as {py:class}`~ray.rllib.env.single_agent_episode.SingleAgentEpisode` or {py:class}`~ray.rllib.env.multi_agent_episode.MultiAgentEpisode` objects. *Connector pipelines* translate this episode data into tensor batches that neural network models read right before the model forward pass.

```{figure} images/connector_v2/generic_connector_pipeline.svg
:width: 1000
:align: left

**Generic ConnectorV2 Pipeline**: All pipelines consist of one or more {py:class}`~ray.rllib.connectors.connector_v2.ConnectorV2` pieces.
When you call the pipeline, you pass in a list of episodes, the {py:class}`~ray.rllib.core.rl_module.rl_module.RLModule` instance,
and a batch, which might start as an empty dict.
Each {py:class}`~ray.rllib.connectors.connector_v2.ConnectorV2` piece takes its predecessor's output,
starting on the left with the batch, transforms the episodes, the batch, or both, and passes everything
to the next piece. Each {py:class}`~ray.rllib.connectors.connector_v2.ConnectorV2` piece can read from and write to the
provided episodes, add data from these episodes to the batch, or change data that's already in the batch.
The pipeline returns the output batch of the last piece.
```

:::{note}
The batch output of the pipeline lives only as long as the succeeding {py:class}`~ray.rllib.core.rl_module.rl_module.RLModule` forward pass or `Env.step()` call. RLlib discards the data afterward. The list of episodes, however, might persist longer. For example, if an env-to-module pipeline reads an observation from an episode, mutates that observation, and writes it back into the episode, the subsequent module-to-env pipeline can see the changed observation. The Learner pipeline operates on the same episodes that already passed through both the env-to-module and module-to-env pipelines, so those episodes might have changed.
:::


## Three ConnectorV2 pipeline types

RLlib has three types of connector pipelines:

1. {ref}`Env-to-module pipeline <env-to-module-pipeline-docs>`, which creates tensor batches for the forward passes that compute actions.
1. Module-to-env pipeline, which translates a model's output into RL environment actions. Documentation for this pipeline is pending.
1. {ref}`Learner connector pipeline <learner-pipeline-docs>`, which creates the train batch for a model update.

The {py:class}`~ray.rllib.connectors.connector_v2.ConnectorV2` API is a tool for customizing your RLlib experiments and algorithms. With it, you take full control over how RLlib accesses, changes, and reassembles the episode data collected from your RL environments or offline RL input files. You also control the exact shape and content of the tensor batches that RLlib feeds into your models to compute actions or losses.

```{figure} images/connector_v2/location_of_connector_pipelines_in_rllib.svg
:width: 900
:align: left

**ConnectorV2 Pipelines**: The env-to-module and Learner pipelines convert episodes into batched data that your model can process.
The module-to-env pipeline converts your model's output into action batches that your possibly vectorized RL environment needs for
stepping.
The env-to-module pipeline, located on an {py:class}`~ray.rllib.env.env_runner.EnvRunner`, takes a list of
episodes as input and outputs a batch for an {py:class}`~ray.rllib.core.rl_module.rl_module.RLModule` forward pass
that computes the next action. The module-to-env pipeline on the same {py:class}`~ray.rllib.env.env_runner.EnvRunner`
takes the output of that {py:class}`~ray.rllib.core.rl_module.rl_module.RLModule` and converts it into actions
for the next call to your RL environment's `step()` method.
Lastly, a Learner connector pipeline, located on a {py:class}`~ray.rllib.core.learner.learner.Learner`
worker, converts a list of episodes into a train batch for the next {py:class}`~ray.rllib.core.rl_module.rl_module.RLModule` update.
```

The following pages discuss the three pipeline types in more detail. All three share these characteristics:

* All connector pipelines are sequences of one or more {py:class}`~ray.rllib.connectors.connector_v2.ConnectorV2` pieces. You can nest these, so some pieces might themselves be connector pipelines.
* All connector pieces and pipelines are Python callables that override the {py:meth}`~ray.rllib.connectors.connector_v2.ConnectorV2.__call__` method.
* The call signatures are uniform across the pipeline types. The main required arguments are the list of episodes, the batch to build, and the {py:class}`~ray.rllib.core.rl_module.rl_module.RLModule` instance. See the {py:meth}`~ray.rllib.connectors.connector_v2.ConnectorV2.__call__` method for details.
* All connector pipelines can read from and write to the provided list of episodes and the batch, performing data transforms as needed.


## Batch construction phases and formats

When you push a list of input episodes through a connector pipeline, the pipeline constructs a batch from that data. The batch always starts as an empty Python dictionary and passes through several formats and phases as it moves through the pipeline's pieces.

The following applies to all {ref}`env-to-module <env-to-module-pipeline-docs>` and learner connector pipelines. Documentation for the learner connector pipeline is in progress.

```{figure} images/connector_v2/pipeline_batch_phases_single_agent.svg
:width: 1000
:align: left

**Batch construction phases and formats**: In the standard single-agent case, where only one ModuleID, `DEFAULT_MODULE_ID`, exists,
the batch starts as an empty dictionary on the left, then undergoes a "collect data" phase, in which connector pieces add individual items
to the batch. Each piece stores an item under two keys: the column name, such as `obs` or `rewards`, and the episode ID it extracted
the item from.
In most cases, your custom connector pieces operate during this phase. Once all custom pieces finish their data insertions and transforms,
the {py:class}`~ray.rllib.connectors.common.agent_to_module_mapping.AgentToModuleMapping` default piece performs a
"reorganize by ModuleID" operation in the center, during which the batch's dictionary hierarchy changes to put the ModuleID `DEFAULT_MODULE_ID` at
the top level and the column names below it. At the lowest level of the batch, data items still reside in Python lists.
Finally, the {py:class}`~ray.rllib.connectors.common.batch_individual_items.BatchIndividualItems` default piece creates NumPy arrays
from the Python lists, batching all data on the right.
```


For multi-agent setups, where more than one ModuleID exists, the {py:class}`~ray.rllib.connectors.common.agent_to_module_mapping.AgentToModuleMapping` default connector piece ensures that the output batch maps each module ID to that module's forward batch:

```{figure} images/connector_v2/pipeline_batch_phases_multi_agent.svg
:width: 1100
:align: left

**Batch construction for multi-agent**: In a multi-agent setup, the default {py:class}`~ray.rllib.connectors.common.agent_to_module_mapping.AgentToModuleMapping`
connector piece reorganizes the batch by `ModuleID`, then by column names, so that a
{py:class}`~ray.rllib.core.rl_module.multi_rl_module.MultiRLModule` can loop through its submodules and give each one a batch
for the forward pass.
```

RLlib's {py:class}`~ray.rllib.core.rl_module.multi_rl_module.MultiRLModule` splits the forward pass into individual submodule forward passes, using the batch under each `ModuleID`. See {ref}`how to write your own multi-module or multi-agent forward logic <implementing-custom-multi-rl-modules>` to override this default behavior.

If you have a stateful {py:class}`~ray.rllib.core.rl_module.rl_module.RLModule`, such as an LSTM, RLlib adds two more default connector pieces to the pipeline, {py:class}`~ray.rllib.connectors.common.add_time_dim_to_batch_and_zero_pad.AddTimeDimToBatchAndZeroPad` and {py:class}`~ray.rllib.connectors.common.add_states_from_episodes_to_batch.AddStatesFromEpisodesToBatch`:

```{figure} images/connector_v2/pipeline_batch_phases_single_agent_w_states.svg
:width: 900
:align: left

**Batch construction for stateful models**: For stateful {py:class}`~ray.rllib.core.rl_module.rl_module.RLModule` instances,
RLlib adds two more default connector pieces to the pipeline. The
{py:class}`~ray.rllib.connectors.common.add_time_dim_to_batch_and_zero_pad.AddTimeDimToBatchAndZeroPad` piece converts all lists of individual data
items on the lowest batch level into sequences of a fixed length, `max_seq_len`, and zero-pads
these when it encounters an episode end. To set `max_seq_len`, see the note below.
The {py:class}`~ray.rllib.connectors.common.add_states_from_episodes_to_batch.AddStatesFromEpisodesToBatch` piece adds the
`state_out` values that your {py:class}`~ray.rllib.core.rl_module.rl_module.RLModule` previously generated to the batch under the `state_in` column name. RLlib adds
the `state_in` values only for the first timestep in each sequence, so it doesn't add a time dimension to the data in the
`state_in` column.
```

:::{note}

To change the zero-padded sequence length for the {py:class}`~ray.rllib.connectors.common.add_time_dim_to_batch_and_zero_pad.AddTimeDimToBatchAndZeroPad` connector, set `max_seq_len` in your config. For custom models:

```python
config.rl_module(model_config={"max_seq_len": ...})
```

For RLlib's default models:

```python
from ray.rllib.core.rl_module.default_model_config import DefaultModelConfig

config.rl_module(model_config=DefaultModelConfig(max_seq_len=...))
```
:::


% Debugging ConnectorV2 Pipelines % ===============================

% TODO (sven): Move the following to the "how to contribute to RLlib" page and rename that page "how to develop, debug and contribute to RLlib?"

% You can debug your custom ConnectorV2 pipelines (and any RLlib component in general) through the following simple steps:

% Run without any remote :py:class:`~ray.rllib.env.env_runner.EnvRunner` workers. After defining your :py:class:`~ray.rllib.algorithms.algorithm_config.AlgorithmConfig` object, do: `config.env_runners(num_env_runners=0)`. % Run without any remote :py:class:`~ray.rllib.core.learner.learner.Learner` workers. After defining your :py:class:`~ray.rllib.algorithms.algorithm_config.AlgorithmConfig` object, do: `config.learners(num_learners=0)`. % Switch off Ray Tune, if applicable. After defining your :py:class:`~ray.rllib.algorithms.algorithm_config.AlgorithmConfig` object, do: `algo = config.build()`, then `while True: algo.train()`. % Set a breakpoint in the ConnectorV2 piece (or any other RLlib component) you would like to debug and start the experiment script in your favorite IDE in debugging mode.

% .. figure:: images/debugging_rllib_in_ide.png
