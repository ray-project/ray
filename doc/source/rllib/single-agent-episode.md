---
myst:
  html_meta:
    description: "SingleAgentEpisode, RLlib's trajectory data container: construction, getter APIs, numpy'ized episodes, and cut() with lookback buffers."
---

(single-agent-episode-docs)=

# Episodes

RLlib stores and transports all trajectory data in the form of `Episodes`, in particular {py:class}`~ray.rllib.env.single_agent_episode.SingleAgentEpisode` for single-agent setups and {py:class}`~ray.rllib.env.multi_agent_episode.MultiAgentEpisode` for multi-agent setups. So-called {ref}`connector pipelines <connector-v2-docs>` translate the data from this `Episode` format to tensor batches, possibly moving it to the GPU, only immediately before a neural network forward pass.

```{figure} images/episodes/usage_of_episodes.svg
:width: 750
:align: left

**Episodes** are the main vehicle for storing and transporting trajectory data across the components
of RLlib, for example from `EnvRunner` to `Learner` or from `ReplayBuffer` to `Learner`.
One of the main design principles of RLlib's new API stack is to keep all trajectory data in such episodic form
for as long as possible. Only immediately before the neural network passes, {ref}`connector pipelines <connector-v2-docs>`
translate lists of Episodes into tensor batches. See {ref}`Connectors and Connector pipelines <connector-v2-docs>`
for more details.
```

The main advantage of collecting and moving data in this trajectory-as-a-whole format, rather than in tensor batches, is 360° visibility and full access to the RL environment's history. You can extract arbitrary pieces of information from episodes for your custom components to process further. Consider a transformer model that needs not only the most recent observation to compute the next action, but the whole sequence of the last n observations. With {py:meth}`~ray.rllib.env.single_agent_episode.SingleAgentEpisode.get_observations`, you can extract this information inside your custom {py:class}`~ray.rllib.connectors.connector_v2.ConnectorV2` pipeline and add the data to the neural network batch.

Another advantage of episodes over batches is a more efficient memory footprint. For example, an algorithm such as DQN needs both observations and next observations in the train batch to compute the TD error-based loss, which duplicates an already large observation tensor. Using episode objects most of the time reduces the memory need to a single observation track that contains all observations, from reset to terminal.

This page explains in detail how to work with RLlib's Episode APIs.

# SingleAgentEpisode

This page describes the single-agent case only.

:::{note}
The Ray team is working on a detailed description of the multi-agent case, analogous to this page but for {py:class}`~ray.rllib.env.multi_agent_episode.MultiAgentEpisode`.
:::

## Creating a SingleAgentEpisode

RLlib normally creates {py:class}`~ray.rllib.env.single_agent_episode.SingleAgentEpisode` instances and moves them around, for example from {py:class}`~ray.rllib.env.env_runner.EnvRunner` to {py:class}`~ray.rllib.core.learner.learner.Learner`. To manually generate and fill an initially empty episode with dummy data, follow this example:

```{literalinclude} doc_code/sa_episode.py
:language: python
:start-after: rllib-sa-episode-01-begin
:end-before: rllib-sa-episode-01-end
```

The {py:class}`~ray.rllib.env.single_agent_episode.SingleAgentEpisode` you constructed and filled looks roughly like this:

```{figure} images/episodes/sa_episode.svg
:width: 750
:align: left

**(Single-agent) Episode**: The episode starts with a single observation, the reset observation, then
continues on each timestep with a 3-tuple of `(observation, action, reward)`. Because of the reset observation,
at each timestep every episode always contains one more observation than actions or rewards.
Important additional properties of an Episode are its `id_` string and its `terminated` and `truncated` boolean flags.
See below for a detailed description of the {py:class}`~ray.rllib.env.single_agent_episode.SingleAgentEpisode`
APIs.
```

## Using the getter APIs of SingleAgentEpisode

With a {py:class}`~ray.rllib.env.single_agent_episode.SingleAgentEpisode` to work with, you can explore and extract information from it using its different getter methods:

```{figure} images/episodes/sa_episode_getters.svg
:width: 750
:align: left

**SingleAgentEpisode getter APIs**: Getter methods exist for all five of the Episode's fields: `observations`,
`actions`, `rewards`, `infos`, and `extra_model_outputs`. This figure shows only the getters for observations, actions, and rewards.
Each getter returns a single item when you provide a single index, and a list of items
when you provide a list of indices or a slice of indices. The list case applies to non-numpy'ized episodes, described below.
```

For `extra_model_outputs`, the getter is slightly more complicated because this data has sub-keys, such as `action_logp`. See {py:meth}`~ray.rllib.env.single_agent_episode.SingleAgentEpisode.get_extra_model_outputs` for more information.

The following code snippet summarizes the capabilities of the getter methods:

```{literalinclude} doc_code/sa_episode.py
:language: python
:start-after: rllib-sa-episode-02-begin
:end-before: rllib-sa-episode-02-end
```

## Numpy'ized and non-numpy'ized episodes

The data in a {py:class}`~ray.rllib.env.single_agent_episode.SingleAgentEpisode` can exist in two states: non-numpy'ized and numpy'ized. A non-numpy'ized episode stores its data items in plain Python lists and appends new timestep data to these. A numpy'ized episode converts these lists into possibly complex structures with NumPy arrays at their leaves. A numpy'ized episode isn't necessarily terminated or truncated: the underlying RL environment need not have declared the episode over or reached a maximum number of timesteps.

```{figure} images/episodes/sa_episode_non_finalized_vs_finalized.svg
:width: 900
:align: left
```

{py:class}`~ray.rllib.env.single_agent_episode.SingleAgentEpisode` objects start in the non-numpy'ized state, which stores data in Python lists and makes appending data from an ongoing episode fast:

```{literalinclude} doc_code/sa_episode.py
:language: python
:start-after: rllib-sa-episode-03-begin
:end-before: rllib-sa-episode-03-end
```

To illustrate the difference between data stored in a non-numpy'ized episode and the same data in a numpy'ized one, consider the following complex observation example. It shows the same observation data in two episodes, one non-numpy'ized and the other numpy'ized:

```{figure} images/episodes/sa_episode_non_finalized.svg
:width: 800
:align: left

**Complex observations in a non-numpy'ized episode**: Each observation is a complex dict matching the
gymnasium environment's observation space. The episode stores three such observation items so far.
```

```{figure} images/episodes/sa_episode_finalized.svg
:width: 600
:align: left

**Complex observations in a numpy'ized episode**: The entire observation record is a single complex dict matching the
gymnasium environment's observation space. At the leaves of the structure are `NDArrays` holding the individual values of the leaf.
These `NDArrays` have an extra batch dim at axis 0, whose length matches the length of the stored episode, here three.
```

## Episode.cut() and lookback buffers

During sample collection from an RL environment, the {py:class}`~ray.rllib.env.env_runner.EnvRunner` sometimes has to stop appending data to an ongoing {py:class}`~ray.rllib.env.single_agent_episode.SingleAgentEpisode` and return the data collected so far. The `EnvRunner` then calls {py:meth}`~ray.rllib.env.single_agent_episode.SingleAgentEpisode.cut` on the {py:class}`~ray.rllib.env.single_agent_episode.SingleAgentEpisode` object, which returns a new episode chunk. Collection continues with this chunk in the next round of sampling.

```{literalinclude} doc_code/sa_episode.py
:language: python
:start-after: rllib-sa-episode-04-begin
:end-before: rllib-sa-episode-04-end
```

A lookback mechanism gives connectors access to the `H` previous timesteps of the cut episode from within the continuation chunk, where `H` is a configurable parameter.

```{figure} images/episodes/sa_episode_cut_and_lookback.svg
:width: 800
:align: left
```

The default lookback horizon `H` is 1. After a `cut()`, you can still access the most recent action with `get_actions(-1)`, the most recent reward with `get_rewards(-1)`, and the two most recent observations with `get_observations([-2, -1])`. To access data further in the past, change this setting in your {py:class}`~ray.rllib.algorithms.algorithm_config.AlgorithmConfig`:

```{testcode}
:hide:

from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
```

```{testcode}
config = AlgorithmConfig()
# Change the lookback horizon setting, in case your connector (pipelines) need
# to access data further in the past.
config.env_runners(episode_lookback_horizon=10)
```

### Lookback buffers and getters in more detail

The following code demonstrates more options for accessing information further in the past, inside the lookback buffers, through the {py:class}`~ray.rllib.env.single_agent_episode.SingleAgentEpisode` getter APIs. Imagine writing a connector piece that adds the last five rewards to the tensor batch that your model's action-computing forward pass uses:

```{literalinclude} doc_code/sa_episode.py
:language: python
:start-after: rllib-sa-episode-05-begin
:end-before: rllib-sa-episode-05-end
```

Another useful getter argument besides `fill` is the `neg_index_as_lookback` boolean argument. When set to True, negative indices mean "into the lookback buffer" rather than "from the end." With this argument, you can loop over a range of global timesteps while looking back a certain number of timesteps from each one:

```{literalinclude} doc_code/sa_episode.py
:language: python
:start-after: rllib-sa-episode-06-begin
:end-before: rllib-sa-episode-06-end
```
