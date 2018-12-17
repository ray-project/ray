from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

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
#      _group_infos: [{"_avail_actions": ...}, {}],  # 2 agents in this group
#   }
# }
GROUP_INFO = "_group_info"

# info key for the available actions mask, for example:
# info: {
#   agent_1: {
#      _avail_actions: [1, 1, 1, 0],  # actions 0-2 allowed, 3 disallowed
#   }
# }
AVAIL_ACTIONS = "_avail_actions"

# dones dict key that signifies the episode has ended for all agents, e.g.:
# dones: {
#   agent_1: True,
#   agent_2: False,
#   __all__: False,
# }
ALL_DONE = "__all__"
