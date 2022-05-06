from collections import OrderedDict

from ray.rllib.env.multi_agent_env import MultiAgentEnv

# info key for the individual rewards of an agent, for example:
# info: {
#   group_1: {
#      _group_rewards: [5, -1, 1],  # 3 agents in this group
#   }
# }
GROUP_REWARDS = "_group_rewards"

# info key for the individual infos of an agent, for example:
# info: {
#   group_1: {
#      _group_infos: [{"foo": ...}, {}],  # 2 agents in this group
#   }
# }
GROUP_INFO = "_group_info"


class GroupAgentsWrapper(MultiAgentEnv):
    """Wraps a MultiAgentEnv environment with agents grouped as specified.

    See multi_agent_env.py for the specification of groups.

    This API is experimental.
    """

    def __init__(self, env, groups, obs_space=None, act_space=None):
        """Wrap an existing multi-agent env to group agents together.

        See MultiAgentEnv.with_agent_groups() for usage info.

        Args:
            env (MultiAgentEnv): env to wrap
            groups (dict): Grouping spec as documented in MultiAgentEnv.
            obs_space (Space): Optional observation space for the grouped
                env. Must be a tuple space.
            act_space (Space): Optional action space for the grouped env.
                Must be a tuple space.
        """
        super().__init__()
        self.env = env
        self.groups = groups
        self.agent_id_to_group = {}
        for group_id, agent_ids in groups.items():
            for agent_id in agent_ids:
                if agent_id in self.agent_id_to_group:
                    raise ValueError(
                        "Agent id {} is in multiple groups".format(agent_id)
                    )
                self.agent_id_to_group[agent_id] = group_id
        if obs_space is not None:
            self.observation_space = obs_space
        if act_space is not None:
            self.action_space = act_space
        for group_id in groups.keys():
            self._agent_ids.add(group_id)

    def seed(self, seed=None):
        if not hasattr(self.env, "seed"):
            # This is a silent fail. However, OpenAI gyms also silently fail
            # here.
            return

        self.env.seed(seed)

    def reset(self):
        obs = self.env.reset()
        return self._group_items(obs)

    def step(self, action_dict):
        # Ungroup and send actions
        action_dict = self._ungroup_items(action_dict)
        obs, rewards, dones, infos = self.env.step(action_dict)

        # Apply grouping transforms to the env outputs
        obs = self._group_items(obs)
        rewards = self._group_items(rewards, agg_fn=lambda gvals: list(gvals.values()))
        dones = self._group_items(dones, agg_fn=lambda gvals: all(gvals.values()))
        infos = self._group_items(
            infos, agg_fn=lambda gvals: {GROUP_INFO: list(gvals.values())}
        )

        # Aggregate rewards, but preserve the original values in infos
        for agent_id, rew in rewards.items():
            if isinstance(rew, list):
                rewards[agent_id] = sum(rew)
                if agent_id not in infos:
                    infos[agent_id] = {}
                infos[agent_id][GROUP_REWARDS] = rew

        return obs, rewards, dones, infos

    def _ungroup_items(self, items):
        out = {}
        for agent_id, value in items.items():
            if agent_id in self.groups:
                assert len(value) == len(self.groups[agent_id]), (
                    agent_id,
                    value,
                    self.groups,
                )
                for a, v in zip(self.groups[agent_id], value):
                    out[a] = v
            else:
                out[agent_id] = value
        return out

    def _group_items(self, items, agg_fn=lambda gvals: list(gvals.values())):
        grouped_items = {}
        for agent_id, item in items.items():
            if agent_id in self.agent_id_to_group:
                group_id = self.agent_id_to_group[agent_id]
                if group_id in grouped_items:
                    continue  # already added
                group_out = OrderedDict()
                for a in self.groups[group_id]:
                    if a in items:
                        group_out[a] = items[a]
                    else:
                        raise ValueError(
                            "Missing member of group {}: {}: {}".format(
                                group_id, a, items
                            )
                        )
                grouped_items[group_id] = agg_fn(group_out)
            else:
                grouped_items[agent_id] = item
        return grouped_items
