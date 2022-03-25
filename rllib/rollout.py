#!/usr/bin/env python

from ray.rllib import evaluate
from ray.rllib.evaluate import rollout, RolloutSaver, run
from ray.rllib.utils.deprecation import deprecation_warning

deprecation_warning(old="rllib rollout", new="rllib evaluate", error=False)

# For backward compatibility
rollout = rollout
RolloutSaver = RolloutSaver
run = run

if __name__ == "__main__":
    evaluate.main()
