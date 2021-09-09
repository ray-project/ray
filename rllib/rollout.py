#!/usr/bin/env python

from ray.rllib import evaluate
from ray.rllib.utils.deprecation import deprecation_warning

deprecation_warning(old="rllib rollout", new="rllib evaluate", error=False)

if __name__ == "__main__":
    evaluate.main()
