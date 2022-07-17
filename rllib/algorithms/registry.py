"""Registry of algorithm names for `rllib train --run=<alg_name>`"""

import traceback

from ray.rllib.contrib.registry import CONTRIBUTED_ALGORITHMS


def _import_a2c():
    import ray.rllib.algorithms.a2c as a2c

    return a2c.A2C, a2c.A2CConfig().to_dict()


def _import_a3c():
    import ray.rllib.algorithms.a3c as a3c

    return a3c.A3C, a3c.A3CConfig().to_dict()


def _import_alpha_star():
    import ray.rllib.algorithms.alpha_star as alpha_star

    return alpha_star.AlphaStar, alpha_star.AlphaStarConfig().to_dict()


def _import_alpha_zero():
    import ray.rllib.algorithms.alpha_zero as alpha_zero

    return alpha_zero.AlphaZero, alpha_zero.AlphaZeroConfig().to_dict()


def _import_apex():
    import ray.rllib.algorithms.apex_dqn as apex_dqn

    return apex_dqn.ApexDQN, apex_dqn.ApexDQNConfig().to_dict()


def _import_apex_ddpg():
    import ray.rllib.algorithms.apex_ddpg as apex_ddpg

    return apex_ddpg.ApexDDPG, apex_ddpg.ApexDDPGConfig().to_dict()


def _import_appo():
    import ray.rllib.algorithms.appo as appo

    return appo.APPO, appo.APPOConfig().to_dict()


def _import_ars():
    import ray.rllib.algorithms.ars as ars

    return ars.ARS, ars.ARSConfig().to_dict()


def _import_bandit_lints():
    from ray.rllib.algorithms.bandit.bandit import BanditLinTS

    return BanditLinTS, BanditLinTS.get_default_config()


def _import_bandit_linucb():
    from ray.rllib.algorithms.bandit.bandit import BanditLinUCB

    return BanditLinUCB, BanditLinUCB.get_default_config()


def _import_bc():
    import ray.rllib.algorithms.bc as bc

    return bc.BC, bc.BCConfig().to_dict()


def _import_cql():
    import ray.rllib.algorithms.cql as cql

    return cql.CQL, cql.CQLConfig().to_dict()


def _import_crr():
    from ray.rllib.algorithms import crr

    return crr.CRR, crr.CRRConfig


def _import_ddpg():
    import ray.rllib.algorithms.ddpg as ddpg

    return ddpg.DDPG, ddpg.DDPGConfig().to_dict()


def _import_ddppo():
    import ray.rllib.algorithms.ddppo as ddppo

    return ddppo.DDPPO, ddppo.DDPPOConfig().to_dict()


def _import_dqn():
    import ray.rllib.algorithms.dqn as dqn

    return dqn.DQN, dqn.DQNConfig().to_dict()


def _import_dreamer():
    import ray.rllib.algorithms.dreamer as dreamer

    return dreamer.Dreamer, dreamer.DreamerConfig().to_dict()


def _import_es():
    import ray.rllib.algorithms.es as es

    return es.ES, es.ESConfig().to_dict()


def _import_impala():
    import ray.rllib.algorithms.impala as impala

    return impala.Impala, impala.ImpalaConfig().to_dict()


def _import_maddpg():
    import ray.rllib.algorithms.maddpg as maddpg

    return maddpg.MADDPG, maddpg.MADDPGConfig().to_dict()


def _import_maml():
    import ray.rllib.algorithms.maml as maml

    return maml.MAML, maml.MAMLConfig().to_dict()


def _import_marwil():
    import ray.rllib.algorithms.marwil as marwil

    return marwil.MARWIL, marwil.MARWILConfig().to_dict()


def _import_mbmpo():
    import ray.rllib.algorithms.mbmpo as mbmpo

    return mbmpo.MBMPO, mbmpo.MBMPOConfig().to_dict()


def _import_pg():
    import ray.rllib.algorithms.pg as pg

    return pg.PG, pg.PGConfig().to_dict()


def _import_ppo():
    import ray.rllib.algorithms.ppo as ppo

    return ppo.PPO, ppo.PPOConfig().to_dict()


def _import_qmix():
    import ray.rllib.algorithms.qmix as qmix

    return qmix.QMix, qmix.QMixConfig().to_dict()


def _import_r2d2():
    import ray.rllib.algorithms.r2d2 as r2d2

    return r2d2.R2D2, r2d2.R2D2Config().to_dict()


def _import_sac():
    import ray.rllib.algorithms.sac as sac

    return sac.SAC, sac.SACConfig().to_dict()


def _import_rnnsac():
    from ray.rllib.algorithms import sac

    return sac.RNNSAC, sac.RNNSACConfig().to_dict()


def _import_simple_q():
    import ray.rllib.algorithms.simple_q as simple_q

    return simple_q.SimpleQ, simple_q.SimpleQConfig().to_dict()


def _import_slate_q():
    import ray.rllib.algorithms.slateq as slateq

    return slateq.SlateQ, slateq.SlateQConfig().to_dict()


def _import_td3():
    import ray.rllib.algorithms.td3 as td3

    return td3.TD3, td3.TD3Config().to_dict()


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
    "RNNSAC": _import_rnnsac,
    "SAC": _import_sac,
    "SimpleQ": _import_simple_q,
    "SlateQ": _import_slate_q,
    "TD3": _import_td3,
}


def get_algorithm_class(alg: str, return_config=False) -> type:
    """Returns the class of a known Trainer given its name."""

    try:
        return _get_algorithm_class(alg, return_config=return_config)
    except ImportError:
        from ray.rllib.algorithms.mock import _algorithm_import_failed

        class_ = _algorithm_import_failed(traceback.format_exc())
        config = class_.get_default_config()
        if return_config:
            return class_, config
        return class_


# Backward compat alias.
get_trainer_class = get_algorithm_class


def _get_algorithm_class(alg: str, return_config=False) -> type:
    if alg in ALGORITHMS:
        class_, config = ALGORITHMS[alg]()
    elif alg in CONTRIBUTED_ALGORITHMS:
        class_, config = CONTRIBUTED_ALGORITHMS[alg]()
    elif alg == "script":
        from ray.tune import script_runner

        class_, config = script_runner.ScriptRunner, {}
    elif alg == "__fake":
        from ray.rllib.algorithms.mock import _MockTrainer

        class_, config = _MockTrainer, _MockTrainer.get_default_config()
    elif alg == "__sigmoid_fake_data":
        from ray.rllib.algorithms.mock import _SigmoidFakeData

        class_, config = _SigmoidFakeData, _SigmoidFakeData.get_default_config()
    elif alg == "__parameter_tuning":
        from ray.rllib.algorithms.mock import _ParameterTuningTrainer

        class_, config = (
            _ParameterTuningTrainer,
            _ParameterTuningTrainer.get_default_config(),
        )
    else:
        raise Exception("Unknown algorithm {}.".format(alg))

    if return_config:
        return class_, config
    return class_
