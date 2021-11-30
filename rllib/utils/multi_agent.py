from typing import Tuple

from ray.rllib.policy.policy import PolicySpec
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID
from ray.rllib.utils.typing import MultiAgentPolicyConfigDict, \
    PartialTrainerConfigDict


def check_multi_agent(config: PartialTrainerConfigDict) -> \
        Tuple[MultiAgentPolicyConfigDict, bool]:
    """Checks, whether a (partial) config defines a multi-agent setup.

    Args:
        config: The user/Trainer/Policy config to check for multi-agent.

    Returns:
        Tuple consisting of the resulting (all fixed) multi-agent policy
        dict and bool indicating whether we have a multi-agent setup or not.
    """
    multiagent_config = config["multiagent"]
    policies = multiagent_config.get("policies")

    # Nothing specified in config dict -> Assume simple single agent setup
    # with DEFAULT_POLICY_ID as only policy.
    if not policies:
        policies = {DEFAULT_POLICY_ID}
    # Policies given as set (of PolicyIDs) -> Setup each policy automatically
    # via empty PolicySpec (will make RLlib infer obs- and action spaces
    # as well as the Policy's class).
    if isinstance(policies, set):
        policies = multiagent_config["policies"] = {
            pid: PolicySpec()
            for pid in policies
        }
    # Is this a multi-agent setup? True, iff DEFAULT_POLICY_ID is only
    # PolicyID found in policies dict.
    is_multiagent = len(policies) > 1 or DEFAULT_POLICY_ID not in policies
    return policies, is_multiagent
