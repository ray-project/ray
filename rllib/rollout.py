#!/usr/bin/env python

from ray.rllib.utils.deprecation import deprecation_warning

deprecation_warning(old="rllib rollout", new="rllib evaluate", error=True)
