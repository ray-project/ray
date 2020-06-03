from ray.rllib.utils import renamed_class
from ray.rllib.evaluation import RolloutWorker

PolicyEvaluator = renamed_class(
    RolloutWorker, old_name="rllib.evaluation.PolicyEvaluator")
