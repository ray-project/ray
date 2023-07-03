"""Registry of algorithm names for `rllib train --run=<alg_name>`"""

import importlib
import re
import traceback
from typing import Tuple, Type, TYPE_CHECKING, Union

from ray.rllib.utils.deprecation import Deprecated

if TYPE_CHECKING:
    from ray.rllib.algorithms.algorithm import Algorithm
    from ray.rllib.algorithms.algorithm_config import AlgorithmConfig


def _import_a2c():
    import ray.rllib.algorithms.a2c as a2c

    return a2c.A2C, a2c.A2C.get_default_config()


def _import_a3c():
    import ray.rllib.algorithms.a3c as a3c

    return a3c.A3C, a3c.A3C.get_default_config()


def _import_alpha_star():
    import ray.rllib.algorithms.alpha_star as alpha_star

    return alpha_star.AlphaStar, alpha_star.AlphaStar.get_default_config()


def _import_alpha_zero():
    import ray.rllib.algorithms.alpha_zero as alpha_zero

    return alpha_zero.AlphaZero, alpha_zero.AlphaZero.get_default_config()


def _import_apex():
    import ray.rllib.algorithms.apex_dqn as apex_dqn

    return apex_dqn.ApexDQN, apex_dqn.ApexDQN.get_default_config()


def _import_apex_ddpg():
    import ray.rllib.algorithms.apex_ddpg as apex_ddpg

    return apex_ddpg.ApexDDPG, apex_ddpg.ApexDDPG.get_default_config()


def _import_appo():
    import ray.rllib.algorithms.appo as appo

    return appo.APPO, appo.APPO.get_default_config()


def _import_ars():
    import ray.rllib.algorithms.ars as ars

    return ars.ARS, ars.ARS.get_default_config()


def _import_bandit_lints():
    from ray.rllib.algorithms.bandit.bandit import BanditLinTS

    return BanditLinTS, BanditLinTS.get_default_config()


def _import_bandit_linucb():
    from ray.rllib.algorithms.bandit.bandit import BanditLinUCB

    return BanditLinUCB, BanditLinUCB.get_default_config()


def _import_bc():
    import ray.rllib.algorithms.bc as bc

    return bc.BC, bc.BC.get_default_config()


def _import_cql():
    import ray.rllib.algorithms.cql as cql

    return cql.CQL, cql.CQL.get_default_config()


def _import_crr():
    from ray.rllib.algorithms import crr

    return crr.CRR, crr.CRR.get_default_config()


def _import_ddpg():
    import ray.rllib.algorithms.ddpg as ddpg

    return ddpg.DDPG, ddpg.DDPG.get_default_config()


def _import_ddppo():
    import ray.rllib.algorithms.ddppo as ddppo

    return ddppo.DDPPO, ddppo.DDPPO.get_default_config()


def _import_dqn():
    import ray.rllib.algorithms.dqn as dqn

    return dqn.DQN, dqn.DQN.get_default_config()


def _import_dreamer():
    import ray.rllib.algorithms.dreamer as dreamer

    return dreamer.Dreamer, dreamer.Dreamer.get_default_config()


def _import_dreamerv3():
    import ray.rllib.algorithms.dreamerv3 as dreamerv3

    return dreamerv3.DreamerV3, dreamerv3.DreamerV3.get_default_config()


def _import_dt():
    import ray.rllib.algorithms.dt as dt

    return dt.DT, dt.DT.get_default_config()


def _import_es():
    import ray.rllib.algorithms.es as es

    return es.ES, es.ES.get_default_config()


def _import_impala():
    import ray.rllib.algorithms.impala as impala

    return impala.Impala, impala.Impala.get_default_config()


def _import_maddpg():
    import ray.rllib.algorithms.maddpg as maddpg

    return maddpg.MADDPG, maddpg.MADDPG.get_default_config()


def _import_maml():
    import ray.rllib.algorithms.maml as maml

    return maml.MAML, maml.MAML.get_default_config()


def _import_marwil():
    import ray.rllib.algorithms.marwil as marwil

    return marwil.MARWIL, marwil.MARWIL.get_default_config()


def _import_mbmpo():
    import ray.rllib.algorithms.mbmpo as mbmpo

    return mbmpo.MBMPO, mbmpo.MBMPO.get_default_config()


def _import_pg():
    import ray.rllib.algorithms.pg as pg

    return pg.PG, pg.PG.get_default_config()


def _import_ppo():
    import ray.rllib.algorithms.ppo as ppo

    return ppo.PPO, ppo.PPO.get_default_config()


def _import_qmix():
    import ray.rllib.algorithms.qmix as qmix

    return qmix.QMix, qmix.QMix.get_default_config()


def _import_r2d2():
    import ray.rllib.algorithms.r2d2 as r2d2

    return r2d2.R2D2, r2d2.R2D2.get_default_config()


def _import_random_agent():
    import ray.rllib.algorithms.random_agent as random_agent

    return random_agent.RandomAgent, random_agent.RandomAgent.get_default_config()


def _import_rnnsac():
    from ray.rllib.algorithms import sac

    return sac.RNNSAC, sac.RNNSAC.get_default_config()


def _import_sac():
    import ray.rllib.algorithms.sac as sac

    return sac.SAC, sac.SAC.get_default_config()


def _import_simple_q():
    import ray.rllib.algorithms.simple_q as simple_q

    return simple_q.SimpleQ, simple_q.SimpleQ.get_default_config()


def _import_slate_q():
    import ray.rllib.algorithms.slateq as slateq

    return slateq.SlateQ, slateq.SlateQ.get_default_config()


def _import_td3():
    import ray.rllib.algorithms.td3 as td3

    return td3.TD3, td3.TD3.get_default_config()


def _import_leela_chess_zero():
    import ray.rllib.algorithms.leela_chess_zero as lc0

    return lc0.LeelaChessZero, lc0.LeelaChessZero.get_default_config()


ALGORITHMS = {
    "A2C": _import_a2c,
    "A3C": _import_a3c,
    "AlphaZero": _import_alpha_zero,
    "APEX": _import_apex,
    "APEX_DDPG": _import_apex_ddpg,
    "ARS": _import_ars,
    "BanditLinTS": _import_bandit_lints,
    "BanditLinUCB": _import_bandit_linucb,
    "BC": _import_bc,
    "CQL": _import_cql,
    "CRR": _import_crr,
    "ES": _import_es,
    "DDPG": _import_ddpg,
    "DDPPO": _import_ddppo,
    "DQN": _import_dqn,
    "Dreamer": _import_dreamer,
    "DreamerV3": _import_dreamerv3,
    "DT": _import_dt,
    "IMPALA": _import_impala,
    "APPO": _import_appo,
    "AlphaStar": _import_alpha_star,
    "MADDPG": _import_maddpg,
    "MAML": _import_maml,
    "MARWIL": _import_marwil,
    "MBMPO": _import_mbmpo,
    "PG": _import_pg,
    "PPO": _import_ppo,
    "QMIX": _import_qmix,
    "R2D2": _import_r2d2,
    "Random": _import_random_agent,
    "RNNSAC": _import_rnnsac,
    "SAC": _import_sac,
    "SimpleQ": _import_simple_q,
    "SlateQ": _import_slate_q,
    "TD3": _import_td3,
    "LeelaChessZero": _import_leela_chess_zero,
}


ALGORITHMS_CLASS_TO_NAME = {
    "A2C": "A2C",
    "A3C": "A3C",
    "AlphaZero": "AlphaZero",
    "ApexDQN": "APEX",
    "ApexDDPG": "APEX_DDPG",
    "ARS": "ARS",
    "BanditLinTS": "BanditLinTS",
    "BanditLinUCB": "BanditLinUCB",
    "BC": "BC",
    "CQL": "CQL",
    "CRR": "CRR",
    "ES": "ES",
    "DDPG": "DDPG",
    "DDPPO": "DDPPO",
    "DQN": "DQN",
    "Dreamer": "Dreamer",
    "DreamerV3": "DreamerV3",
    "DT": "DT",
    "Impala": "IMPALA",
    "APPO": "APPO",
    "AlphaStar": "AlphaStar",
    "MADDPG": "MADDPG",
    "MAML": "MAML",
    "MARWIL": "MARWIL",
    "MBMPO": "MBMPO",
    "PG": "PG",
    "PPO": "PPO",
    "QMix": "QMIX",
    "R2D2": "R2D2",
    "RandomAgent": "Random",
    "RNNSAC": "RNNSAC",
    "SAC": "SAC",
    "SimpleQ": "SimpleQ",
    "SlateQ": "SlateQ",
    "TD3": "TD3",
    "LeelaChessZero": "LeelaChessZero",
}


@Deprecated(
    new="ray.tune.registry.get_trainable_cls([algo name], return_config=False) and cls="
    "ray.tune.registry.get_trainable_cls([algo name]); cls.get_default_config();",
    error=False,
)
def get_algorithm_class(
    alg: str,
    return_config=False,
) -> Union[Type["Algorithm"], Tuple[Type["Algorithm"], "AlgorithmConfig"]]:
    """Returns the class of a known Algorithm given its name."""

    try:
        return _get_algorithm_class(alg, return_config=return_config)
    except ImportError:
        from ray.rllib.algorithms.mock import _algorithm_import_failed

        class_ = _algorithm_import_failed(traceback.format_exc())
        config = class_.get_default_config()
        if return_config:
            return class_, config
        return class_


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


# Mapping from policy name to where it is located, relative to rllib.algorithms.
# TODO(jungong) : Finish migrating all the policies to PolicyV2, so we can list
# all the TF eager policies here.
POLICIES = {
    "A3CTF1Policy": "a3c.a3c_tf_policy",
    "A3CTF2Policy": "a3c.a3c_tf_policy",
    "A3CTorchPolicy": "a3c.a3c_torch_policy",
    "AlphaZeroPolicy": "alpha_zero.alpha_zero_policy",
    "APPOTF1Policy": "appo.appo_tf_policy",
    "APPOTF2Policy": "appo.appo_tf_policy",
    "APPOTorchPolicy": "appo.appo_torch_policy",
    "ARSTFPolicy": "ars.ars_tf_policy",
    "ARSTorchPolicy": "ars.ars_torch_policy",
    "BanditTFPolicy": "bandit.bandit_tf_policy",
    "BanditTorchPolicy": "bandit.bandit_torch_policy",
    "CQLTFPolicy": "cql.cql_tf_policy",
    "CQLTorchPolicy": "cql.cql_torch_policy",
    "CRRTorchPolicy": "crr.torch.crr_torch_policy",
    "DDPGTF1Policy": "ddpg.ddpg_tf_policy",
    "DDPGTF2Policy": "ddpg.ddpg_tf_policy",
    "DDPGTorchPolicy": "ddpg.ddpg_torch_policy",
    "DQNTFPolicy": "dqn.dqn_tf_policy",
    "DQNTorchPolicy": "dqn.dqn_torch_policy",
    "DreamerTorchPolicy": "dreamer.dreamer_torch_policy",
    "DTTorchPolicy": "dt.dt_torch_policy",
    "ESTFPolicy": "es.es_tf_policy",
    "ESTorchPolicy": "es.es_torch_policy",
    "ImpalaTF1Policy": "impala.impala_tf_policy",
    "ImpalaTF2Policy": "impala.impala_tf_policy",
    "ImpalaTorchPolicy": "impala.impala_torch_policy",
    "MADDPGTFPolicy": "maddpg.maddpg_tf_policy",
    "MAMLTF1Policy": "maml.maml_tf_policy",
    "MAMLTF2Policy": "maml.maml_tf_policy",
    "MAMLTorchPolicy": "maml.maml_torch_policy",
    "MARWILTF1Policy": "marwil.marwil_tf_policy",
    "MARWILTF2Policy": "marwil.marwil_tf_policy",
    "MARWILTorchPolicy": "marwil.marwil_torch_policy",
    "MBMPOTorchPolicy": "mbmpo.mbmpo_torch_policy",
    "PGTF1Policy": "pg.pg_tf_policy",
    "PGTF2Policy": "pg.pg_tf_policy",
    "PGTorchPolicy": "pg.pg_torch_policy",
    "QMixTorchPolicy": "qmix.qmix_policy",
    "R2D2TFPolicy": "r2d2.r2d2_tf_policy",
    "R2D2TorchPolicy": "r2d2.r2d2_torch_policy",
    "SACTFPolicy": "sac.sac_tf_policy",
    "SACTorchPolicy": "sac.sac_torch_policy",
    "RNNSACTorchPolicy": "sac.rnnsac_torch_policy",
    "SimpleQTF1Policy": "simple_q.simple_q_tf_policy",
    "SimpleQTF2Policy": "simple_q.simple_q_tf_policy",
    "SimpleQTorchPolicy": "simple_q.simple_q_torch_policy",
    "SlateQTFPolicy": "slateq.slateq_tf_policy",
    "SlateQTorchPolicy": "slateq.slateq_torch_policy",
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
