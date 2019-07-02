from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.rllib.agents.trainer import Trainer
from ray.rllib.utils import renamed_agent

Agent = renamed_agent(Trainer)
