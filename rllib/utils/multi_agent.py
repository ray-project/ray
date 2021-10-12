from ray.rllib.policy.policy import PolicySpec
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID
from ray.rllib.utils.typing import PartialTrainerConfigDict


def check_multi_agent(config: PartialTrainerConfigDict):
    """Checks, whether a (partial) config defines a multi-agent setup.

    Args:
        config (PartialTrainerConfigDict): The user/Trainer/Policy config
            to check for multi-agent.

    Returns:
        Tuple[MultiAgentPolicyConfigDict, bool]: The resulting (all
            fixed) multi-agent policy dict and whether we have a
            multi-agent setup or not.
    """
    multiagent_config = config["multiagent"]
    policies = multiagent_config.get("policies")
    if not policies:
        policies = {DEFAULT_POLICY_ID}
    if isinstance(policies, set):
        policies = multiagent_config["policies"] = {
            pid: PolicySpec()
            for pid in policies
        }
    is_multiagent = len(policies) > 1 or DEFAULT_POLICY_ID not in policies
    return policies, is_multiagent
