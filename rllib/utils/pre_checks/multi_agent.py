from typing import Tuple

from ray.rllib.policy.policy import PolicySpec
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID
from ray.rllib.utils.typing import MultiAgentPolicyConfigDict, PartialTrainerConfigDict


def check_multi_agent(
    config: PartialTrainerConfigDict,
) -> Tuple[MultiAgentPolicyConfigDict, bool]:
    """Checks, whether a (partial) config defines a multi-agent setup.

    Args:
        config: The user/Trainer/Policy config to check for multi-agent.

    Returns:
        Tuple consisting of the resulting (all fixed) multi-agent policy
        dict and bool indicating whether we have a multi-agent setup or not.

    Raises:
        KeyError: If `config` does not contain a "multiagent" key or if there
            is an invalid key inside the "multiagent" config or if any policy
            in the "policies" dict has a non-str ID (key).
        ValueError: If any subkey of the "multiagent" dict has an invalid
            value.
    """
    if "multiagent" not in config:
        raise KeyError(
            "Your `config` to be checked for a multi-agent setup must have "
            "the 'multiagent' key defined!"
        )
    multiagent_config = config["multiagent"]

    policies = multiagent_config.get("policies")

    # Check for invalid sub-keys of multiagent config.
    from ray.rllib.agents.trainer import COMMON_CONFIG

    allowed = list(COMMON_CONFIG["multiagent"].keys())
    if any(k not in allowed for k in multiagent_config.keys()):
        raise KeyError(
            f"You have invalid keys in your 'multiagent' config dict! "
            f"The only allowed keys are: {allowed}."
        )

    # Nothing specified in config dict -> Assume simple single agent setup
    # with DEFAULT_POLICY_ID as only policy.
    if not policies:
        policies = {DEFAULT_POLICY_ID}
    # Policies given as set (of PolicyIDs) -> Setup each policy automatically
    # via empty PolicySpec (will make RLlib infer obs- and action spaces
    # as well as the Policy's class).
    if isinstance(policies, set):
        policies = multiagent_config["policies"] = {
            pid: PolicySpec() for pid in policies
        }

    # Check each defined policy ID and spec.
    for pid, policy_spec in policies.copy().items():
        # Policy IDs must be strings.
        if not isinstance(pid, str):
            raise KeyError(f"Policy IDs must always be of type `str`, got {type(pid)}")
        # Convert to PolicySpec if plain list/tuple.
        if not isinstance(policy_spec, PolicySpec):
            # Values must be lists/tuples of len 4.
            if not isinstance(policy_spec, (list, tuple)) or len(policy_spec) != 4:
                raise ValueError(
                    "Policy specs must be tuples/lists of "
                    "(cls or None, obs_space, action_space, config), "
                    f"got {policy_spec}"
                )
            policies[pid] = PolicySpec(*policy_spec)

        # Config is None -> Set to {}.
        if policies[pid].config is None:
            policies[pid] = policies[pid]._replace(config={})
        # Config not a dict.
        elif not isinstance(policies[pid].config, dict):
            raise ValueError(
                f"Multiagent policy config for {pid} must be a dict, "
                f"but got {type(policies[pid].config)}!"
            )

    # Check other "multiagent" sub-keys' values.
    if multiagent_config.get("count_steps_by", "env_steps") not in [
        "env_steps",
        "agent_steps",
    ]:
        raise ValueError(
            "config.multiagent.count_steps_by must be "
            "[env_steps|agent_steps], not "
            f"{multiagent_config['count_steps_by']}!"
        )
    if multiagent_config.get("replay_mode", "independent") not in [
        "independent",
        "lockstep",
    ]:
        raise ValueError(
            "config.multiagent.replay_mode must be "
            "[independent|lockstep], not "
            f"{multiagent_config['replay_mode']}!"
        )

    # Is this a multi-agent setup? True, iff DEFAULT_POLICY_ID is only
    # PolicyID found in policies dict.
    is_multiagent = len(policies) > 1 or DEFAULT_POLICY_ID not in policies
    return policies, is_multiagent
