---
myst:
  html_meta:
    description: "Replay buffers in RLlib: sampling and eviction strategies, the ReplayBuffer base class, configuration, and building a custom buffer."
---

(replay-buffer-reference-docs)=

# Replay buffers

## Quick intro to replay buffers in RL

In reinforcement learning (RL), a replay buffer stores and replays experiences that agents collect from interactions with the environment. In Python, you can implement a simple buffer as a list that you add elements to and later sample from. Off-policy learning algorithms use these buffers most. This makes intuitive sense because these algorithms can learn from experiences in the buffer that a previous version of the policy, or even a completely different behavior policy, produced.

### Sampling strategy

When you sample from a replay buffer, you choose which experiences to train your agent with. A straightforward strategy that works well for many algorithms is to pick samples uniformly at random. A more advanced strategy, which performs better in many cases, is [Prioritized Experience Replay (PER)](https://arxiv.org/abs/1511.05952). PER assigns each item in the buffer a scalar priority value that denotes its significance, or how much you expect to learn from it. PER samples experiences with a higher priority more often.

### Eviction strategy

A buffer has a limited capacity to hold experiences. As an algorithm runs, a buffer eventually reaches its capacity, and to make room for new experiences, it deletes, or evicts, older ones. Eviction generally happens on a first-in, first-out basis. For your algorithms, this means a buffer with a high capacity can learn from older samples, while a smaller buffer makes the learning process more on-policy. Buffers that implement reservoir sampling are an exception to this strategy.

## Replay buffers in RLlib

RLlib comes with a set of extendable replay buffers built in. All of them support the two basic methods `add()` and `sample()`. RLlib provides a base {py:class}`~ray.rllib.utils.replay_buffers.replay_buffer.ReplayBuffer` class that you build your own buffer from. Most algorithms require {py:class}`~ray.rllib.utils.replay_buffers.multi_agent_replay_buffer.MultiAgentReplayBuffer`s so that they generalize to the multi-agent case. These buffers' `add()` and `sample()` methods require a `policy_id` to handle experiences per policy. See the {py:class}`~ray.rllib.utils.replay_buffers.multi_agent_replay_buffer.MultiAgentReplayBuffer` for how it extends the base class. You can find buffer types and arguments to modify their behavior in RLlib's default parameters, as part of the `replay_buffer_config`.

### Basic usage

When running an experiment, you rarely define your own replay buffer subclass. Instead, you configure existing buffers. The following example [from RLlib's examples section](https://github.com/ray-project/ray/blob/master/rllib/examples/_old_api_stack/replay_buffer_api.py) runs the R2D2 algorithm with [PER](https://arxiv.org/abs/1511.05952), which R2D2 doesn't use by default. The highlighted lines focus on the PER configuration.

:::{dropdown} **Executable example script**
:animate: fade-in-slide-down

```{literalinclude} ../../../rllib/examples/_old_api_stack/replay_buffer_api.py
:emphasize-lines: 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70
:language: python
:start-after: __sphinx_doc_replay_buffer_api_example_script_begin__
:end-before: __sphinx_doc_replay_buffer_api_example_script_end__
```
:::

:::{tip}
Because PER is so common, most Q-learning algorithms support it. Their training iteration functions embed the required priority update step.
:::

:::{warning}
If your custom buffer requires extra interaction, you also have to change the training iteration function.
:::

Specifying a buffer type works the same way as specifying an exploration type. The following example shows three ways to specify a type:

:::{dropdown} **Changing a replay buffer configuration**
:animate: fade-in-slide-down

```{literalinclude} doc_code/replay_buffer_demo.py
:language: python
:start-after: __sphinx_doc_replay_buffer_type_specification__begin__
:end-before: __sphinx_doc_replay_buffer_type_specification__end__
```
:::

Apart from the `type`, you can specify the `capacity` and other parameters. These parameters are mostly constructor arguments for the buffer. They fall into three categories:

1. Parameters that define how algorithms interact with replay buffers. For example, `worker_side_prioritization` decides where to compute priorities.

1. Constructor arguments that instantiate the replay buffer. For example, `capacity` limits the buffer's size.

1. Call arguments for underlying replay buffer methods. For example, the {py:class}`~ray.rllib.utils.replay_buffers.multi_agent_prioritized_replay_buffer.MultiAgentPrioritizedReplayBuffer` uses `prioritized_replay_beta` to call the `sample()` method of every underlying {py:class}`~ray.rllib.utils.replay_buffers.prioritized_replay_buffer.PrioritizedReplayBuffer`.

:::{tip}
Most of the time, only the first two categories are of interest. The third is an advanced feature that supports use cases where a {py:class}`~ray.rllib.utils.replay_buffers.multi_agent_replay_buffer.MultiAgentReplayBuffer` instantiates underlying buffers that need constructor or default call arguments.
:::

### ReplayBuffer base class

The base {py:class}`~ray.rllib.utils.replay_buffers.replay_buffer.ReplayBuffer` class only supports storing and replaying experiences in different {py:class}`~ray.rllib.utils.replay_buffers.replay_buffer.StorageUnit`s. Add data to the buffer's storage with the `add()` method, and replay it with the `sample()` method. Advanced buffer types add features while trying to retain compatibility through inheritance. The following example shows the most basic scheme of interaction with a {py:class}`~ray.rllib.utils.replay_buffers.replay_buffer.ReplayBuffer`.

```{literalinclude} doc_code/replay_buffer_demo.py
:language: python
:start-after: __sphinx_doc_replay_buffer_basic_interaction__begin__
:end-before: __sphinx_doc_replay_buffer_basic_interaction__end__
```

### Build your own ReplayBuffer

The following example implements a toy ReplayBuffer class and makes SimpleQ use it:

```{literalinclude} doc_code/replay_buffer_demo.py
:language: python
:start-after: __sphinx_doc_replay_buffer_own_buffer__begin__
:end-before: __sphinx_doc_replay_buffer_own_buffer__end__
```

For a full implementation, consider other methods such as `get_state()` and `set_state()`. For a more extensive example, see RLlib's [implementation of reservoir sampling](https://github.com/ray-project/ray/blob/master/rllib/utils/replay_buffers/reservoir_replay_buffer.py), the {py:class}`~ray.rllib.utils.replay_buffers.reservoir_replay_buffer.ReservoirReplayBuffer`.

## Advanced usage

In RLlib, all replay buffers implement the {py:class}`~ray.rllib.utils.replay_buffers.replay_buffer.ReplayBuffer` interface. They therefore support different {py:class}`~ray.rllib.utils.replay_buffers.replay_buffer.StorageUnit`s whenever possible. A replay buffer's `storage_unit` constructor argument defines how it stores experiences, and therefore the unit in which it samples them. When you later call the `sample()` method, `num_items` relates to that `storage_unit`.

The following example modifies the `storage_unit` and interacts with a custom buffer:

```{literalinclude} doc_code/replay_buffer_demo.py
:language: python
:start-after: __sphinx_doc_replay_buffer_advanced_usage_storage_unit__begin__
:end-before: __sphinx_doc_replay_buffer_advanced_usage_storage_unit__end__
```

As described earlier, RLlib's {py:class}`~ray.rllib.utils.replay_buffers.multi_agent_replay_buffer.MultiAgentReplayBuffer`s support modifying underlying replay buffers. The {py:class}`~ray.rllib.utils.replay_buffers.multi_agent_replay_buffer.MultiAgentReplayBuffer` stores experiences per policy in separate underlying replay buffers. Modify their behavior by specifying an underlying `replay_buffer_config` that works the same way as the parent's config.

The following example creates a {py:class}`~ray.rllib.utils.replay_buffers.multi_agent_replay_buffer.MultiAgentReplayBuffer` with an alternative underlying {py:class}`~ray.rllib.utils.replay_buffers.replay_buffer.ReplayBuffer`. The {py:class}`~ray.rllib.utils.replay_buffers.multi_agent_replay_buffer.MultiAgentReplayBuffer` can stay the same. You only specify your own buffer along with a default call argument:

```{literalinclude} doc_code/replay_buffer_demo.py
:language: python
:start-after: __sphinx_doc_replay_buffer_advanced_usage_underlying_buffers__begin__
:end-before: __sphinx_doc_replay_buffer_advanced_usage_underlying_buffers__end__
```
