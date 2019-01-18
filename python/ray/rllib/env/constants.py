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
#      _group_infos: [{"foo": ...}, {}],  # 2 agents in this group
#   }
# }
GROUP_INFO = "_group_info"
