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

    return alpha_star.AlphaStarTrainer, alpha_star.AlphaStarConfig().to_dict()


def _import_alpha_zero():
    import ray.rllib.algorithms.alpha_zero as alpha_zero

    return alpha_zero.AlphaZeroTrainer, alpha_zero.AlphaZeroConfig().to_dict()


def _import_apex():
    from ray.rllib.agents import dqn

    return dqn.ApexTrainer, dqn.apex.ApexConfig().to_dict()


def _import_apex_ddpg():
    from ray.rllib.algorithms import ddpg

    return ddpg.ApexDDPGTrainer, ddpg.apex.ApexDDPGConfig().to_dict()


def _import_appo():
    import ray.rllib.algorithms.appo as appo

    return appo.APPO, appo.APPOConfig().to_dict()


def _import_ars():
    from ray.rllib.algorithms import ars

    return ars.ARSTrainer, ars.ARSConfig().to_dict()


def _import_bandit_lints():
    from ray.rllib.algorithms.bandit.bandit import BanditLinTSTrainer

    return BanditLinTSTrainer, BanditLinTSTrainer.get_default_config()


def _import_bandit_linucb():
    from ray.rllib.algorithms.bandit.bandit import BanditLinUCBTrainer

    return BanditLinUCBTrainer, BanditLinUCBTrainer.get_default_config()


def _import_bc():
    from ray.rllib.algorithms import marwil

    return marwil.BCTrainer, marwil.BCConfig().to_dict()


def _import_cql():
    from ray.rllib.algorithms import cql

    return cql.CQLTrainer, cql.CQLConfig().to_dict()


def _import_ddpg():
    from ray.rllib.algorithms import ddpg

    return ddpg.DDPGTrainer, ddpg.DDPGConfig().to_dict()


def _import_ddppo():
    import ray.rllib.algorithms.ddppo as ddppo

    return ddppo.DDPPO, ddppo.DDPPOConfig().to_dict()


def _import_dqn():
    from ray.rllib.algorithms import dqn

    return dqn.DQNTrainer, dqn.DQNConfig().to_dict()


def _import_dreamer():
    from ray.rllib.algorithms import dreamer

    return dreamer.DREAMERTrainer, dreamer.DREAMERConfig().to_dict()


def _import_es():
    from ray.rllib.algorithms import es

    return es.ESTrainer, es.ESConfig().to_dict()


def _import_impala():
    import ray.rllib.algorithms.impala as impala

    return impala.Impala, impala.ImpalaConfig().to_dict()


def _import_maddpg():
    import ray.rllib.algorithms.maddpg as maddpg

    return maddpg.MADDPGTrainer, maddpg.MADDPGConfig().to_dict()


def _import_maml():
    from ray.rllib.algorithms import maml

    return maml.MAMLTrainer, maml.MAMLConfig().to_dict()


def _import_marwil():
    from ray.rllib.algorithms import marwil

    return marwil.MARWILTrainer, marwil.MARWILConfig().to_dict()


def _import_mbmpo():
    from ray.rllib.algorithms import mbmpo

    return mbmpo.MBMPOTrainer, mbmpo.MBMPOConfig().to_dict()


def _import_pg():
    from ray.rllib.algorithms import pg

    return pg.PGTrainer, pg.PGConfig().to_dict()


def _import_ppo():
    import ray.rllib.algorithms.ppo as ppo

    return ppo.PPO, ppo.PPOConfig().to_dict()


def _import_qmix():
    from ray.rllib.algorithms import qmix

    return qmix.QMixTrainer, qmix.QMixConfig().to_dict()


def _import_r2d2():
    from ray.rllib.agents import dqn

    return dqn.R2D2Trainer, dqn.R2D2Config().to_dict()


def _import_sac():
    from ray.rllib.algorithms import sac

    return sac.SACTrainer, sac.SACConfig().to_dict()


def _import_rnnsac():
    from ray.rllib.algorithms import sac

    return sac.RNNSACTrainer, sac.RNNSACConfig().to_dict()


def _import_simple_q():
    from ray.rllib.algorithms import dqn

    return dqn.SimpleQTrainer, dqn.simple_q.SimpleQConfig().to_dict()


def _import_slate_q():
    from ray.rllib.algorithms import slateq

    return slateq.SlateQTrainer, slateq.SlateQConfig().to_dict()


def _import_td3():
    from ray.rllib.algorithms import ddpg

    return ddpg.TD3Trainer, ddpg.td3.TD3Config().to_dict()


ALGORITHMS = {
    "A2C": _import_a2c,
    "A3C": _import_a3c,
    "AlphaStar": _import_alpha_star,
    "AlphaZero": _import_alpha_zero,
    "APPO": _import_appo,
    "APEX": _import_apex,
    "APEX_DDPG": _import_apex_ddpg,
    "ARS": _import_ars,
    "BanditLinTS": _import_bandit_lints,
    "BanditLinUCB": _import_bandit_linucb,
    "BC": _import_bc,
    "CQL": _import_cql,
    "ES": _import_es,
    "DDPG": _import_ddpg,
    "DDPPO": _import_ddppo,
    "DQN": _import_dqn,
    "DREAMER": _import_dreamer,
    "IMPALA": _import_impala,
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


def get_trainer_class(alg: str, return_config=False) -> type:
    """Returns the class of a known Trainer given its name."""

    try:
        return _get_trainer_class(alg, return_config=return_config)
    except ImportError:
        from ray.rllib.agents.mock import _trainer_import_failed

        class_ = _trainer_import_failed(traceback.format_exc())
        config = class_.get_default_config()
        if return_config:
            return class_, config
        return class_


def _get_trainer_class(alg: str, return_config=False) -> type:
    if alg in ALGORITHMS:
        class_, config = ALGORITHMS[alg]()
    elif alg in CONTRIBUTED_ALGORITHMS:
        class_, config = CONTRIBUTED_ALGORITHMS[alg]()
    elif alg == "script":
        from ray.tune import script_runner

        class_, config = script_runner.ScriptRunner, {}
    elif alg == "__fake":
        from ray.rllib.agents.mock import _MockTrainer

        class_, config = _MockTrainer, _MockTrainer.get_default_config()
    elif alg == "__sigmoid_fake_data":
        from ray.rllib.agents.mock import _SigmoidFakeData

        class_, config = _SigmoidFakeData, _SigmoidFakeData.get_default_config()
    elif alg == "__parameter_tuning":
        from ray.rllib.agents.mock import _ParameterTuningTrainer

        class_, config = (
            _ParameterTuningTrainer,
            _ParameterTuningTrainer.get_default_config(),
        )
    else:
        raise Exception(("Unknown algorithm {}.").format(alg))

    if return_config:
        return class_, config
    return class_
