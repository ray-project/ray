---
myst:
  html_meta:
    description: "Implement hierarchical reinforcement learning in RLlib as a multi-agent problem, with a top-level policy issuing tasks to lower-level policies."
---

(rllib-hierarchical-environments-doc)=

# Hierarchical environments

You can implement hierarchical training as a special case of multi-agent RL. For example, consider a two-level hierarchy of policies, where a top-level policy issues high-level tasks that one or more low-level policies execute at a finer timescale. The following timeline shows one step of the top-level policy, which corresponds to four low-level actions:

```text
top-level: action_0 -------------------------------------> action_1 ->
low-level: action_0 -> action_1 -> action_2 -> action_3 -> action_4 ->
```

Alternatively, you can implement an environment in which the two agent types don't act at the same time. The low-level agents wait for the high-level agent to issue an action, then act n times before handing control back to the high-level agent:

```text
top-level: action_0 -----------------------------------> action_1 ->
low-level: ---------> action_0 -> action_1 -> action_2 ------------>
```

You can implement any of these hierarchical action patterns as a multi-agent environment with various types of agents, such as a high-level agent and a low-level agent. When you set up the correct agent-to-module mapping functions, RLlib sees an independent multi-agent problem with different types of policies.

Your configuration might look like the following:

```{testcode}
from ray.rllib.algorithms.ppo import PPOConfig

config = (
    PPOConfig()
    .multi_agent(
        policies={"top_level", "low_level"},
        policy_mapping_fn=(
            lambda aid, eps, **kw: "low_level" if aid.startswith("low_level") else "top_level"
        ),
        policies_to_train=["top_level"],
    )
)
```

In this setup, the multi-agent environment implementation should provide the appropriate rewards at any hierarchy level. The environment class is also responsible for routing between agents. For example, it conveys [goals](https://arxiv.org/pdf/1703.01161.pdf) from higher-level agents to lower-level agents as part of the lower-level agent's observation.

See [this runnable example of a hierarchical environment](https://github.com/ray-project/ray/blob/master/rllib/examples/hierarchical/hierarchical_training.py).
