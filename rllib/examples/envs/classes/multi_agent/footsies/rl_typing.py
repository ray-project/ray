from typing import NewType

from pettingzoo.utils import env

AgentID = NewType("AgentID", env.AgentID)
ObsType = NewType("ObsType", env.ObsType)
ActionType = NewType("ActionType", env.ActionType)
