"""Registry of algorithm names for tune.Tuner(trainable=[..])."""

import importlib
import re


def _import_appo():
    import ray.rllib.algorithms.appo as appo

    return appo.APPO, appo.APPO.get_default_config()


def _import_bc():
    import ray.rllib.algorithms.bc as bc

    return bc.BC, bc.BC.get_default_config()


def _import_cql():
    import ray.rllib.algorithms.cql as cql

    return cql.CQL, cql.CQL.get_default_config()


def _import_dqn():
    import ray.rllib.algorithms.dqn as dqn

    return dqn.DQN, dqn.DQN.get_default_config()


def _import_dreamerv3():
    import ray.rllib.algorithms.dreamerv3 as dreamerv3

    return dreamerv3.DreamerV3, dreamerv3.DreamerV3.get_default_config()


def _import_impala():
    import ray.rllib.algorithms.impala as impala

    return impala.IMPALA, impala.IMPALA.get_default_config()


def _import_marwil():
    import ray.rllib.algorithms.marwil as marwil

    return marwil.MARWIL, marwil.MARWIL.get_default_config()


def _import_ppo():
    import ray.rllib.algorithms.ppo as ppo

    return ppo.PPO, ppo.PPO.get_default_config()


def _import_sac():
    import ray.rllib.algorithms.sac as sac

    return sac.SAC, sac.SAC.get_default_config()


ALGORITHMS = {
    "APPO": _import_appo,
    "BC": _import_bc,
    "CQL": _import_cql,
    "DQN": _import_dqn,
    "DreamerV3": _import_dreamerv3,
    "IMPALA": _import_impala,
    "MARWIL": _import_marwil,
    "PPO": _import_ppo,
    "SAC": _import_sac,
}


ALGORITHMS_CLASS_TO_NAME = {
    "APPO": "APPO",
    "BC": "BC",
    "CQL": "CQL",
    "DQN": "DQN",
    "DreamerV3": "DreamerV3",
    "Impala": "IMPALA",
    "IMPALA": "IMPALA",
    "MARWIL": "MARWIL",
    "PPO": "PPO",
    "SAC": "SAC",
}


def _get_algorithm_class(alg: str) -> type:
    # This helps us get around a circular import (tune calls rllib._register_all when
    # checking if a rllib Trainable is registered)
    if alg in ALGORITHMS:
        return ALGORITHMS[alg]()[0]
    elif alg == "script":
        from ray.tune import script_runner

        return script_runner.ScriptRunner
    elif alg == "__fake":
        from ray.rllib.algorithms.mock import _MockTrainer

        return _MockTrainer
    elif alg == "__sigmoid_fake_data":
        from ray.rllib.algorithms.mock import _SigmoidFakeData

        return _SigmoidFakeData
    elif alg == "__parameter_tuning":
        from ray.rllib.algorithms.mock import _ParameterTuningTrainer

        return _ParameterTuningTrainer
    else:
        raise Exception("Unknown algorithm {}.".format(alg))


# Dict mapping policy names to where the class is located, relative to rllib.algorithms.
# TODO(jungong) : Finish migrating all the policies to PolicyV2, so we can list
# all the TF eager policies here.
POLICIES = {
    "APPOTF1Policy": "appo.appo_tf_policy",
    "APPOTF2Policy": "appo.appo_tf_policy",
    "APPOTorchPolicy": "appo.appo_torch_policy",
    "CQLTFPolicy": "cql.cql_tf_policy",
    "CQLTorchPolicy": "cql.cql_torch_policy",
    "DQNTFPolicy": "dqn.dqn_tf_policy",
    "DQNTorchPolicy": "dqn.dqn_torch_policy",
    "ImpalaTF1Policy": "impala.impala_tf_policy",
    "ImpalaTF2Policy": "impala.impala_tf_policy",
    "ImpalaTorchPolicy": "impala.impala_torch_policy",
    "MARWILTF1Policy": "marwil.marwil_tf_policy",
    "MARWILTF2Policy": "marwil.marwil_tf_policy",
    "MARWILTorchPolicy": "marwil.marwil_torch_policy",
    "SACTFPolicy": "sac.sac_tf_policy",
    "SACTorchPolicy": "sac.sac_torch_policy",
    "PPOTF1Policy": "ppo.ppo_tf_policy",
    "PPOTF2Policy": "ppo.ppo_tf_policy",
    "PPOTorchPolicy": "ppo.ppo_torch_policy",
}


def get_policy_class_name(policy_class: type):
    """Returns a string name for the provided policy class.

    Args:
        policy_class: RLlib policy class, e.g. A3CTorchPolicy, DQNTFPolicy, etc.

    Returns:
        A string name uniquely mapped to the given policy class.
    """
    # TF2 policy classes may get automatically converted into new class types
    # that have eager tracing capability.
    # These policy classes have the "_traced" postfix in their names.
    # When checkpointing these policy classes, we should save the name of the
    # original policy class instead. So that users have the choice of turning
    # on eager tracing during inference time.
    name = re.sub("_traced$", "", policy_class.__name__)
    if name in POLICIES:
        return name
    return None


def get_policy_class(name: str):
    """Return an actual policy class given the string name.

    Args:
        name: string name of the policy class.

    Returns:
        Actual policy class for the given name.
    """
    if name not in POLICIES:
        return None

    path = POLICIES[name]
    module = importlib.import_module("ray.rllib.algorithms." + path)

    if not hasattr(module, name):
        return None

    return getattr(module, name)
