"""Registry of algorithm names for `rllib train --run=<alg_name>`"""

import traceback

from ray.rllib.contrib.registry import CONTRIBUTED_ALGORITHMS
from ray.rllib.utils.deprecation import Deprecated


def _import_a2c():
    from ray.rllib.agents import a3c

    return a3c.A2CTrainer, a3c.a2c.A2C_DEFAULT_CONFIG


def _import_a3c():
    from ray.rllib.agents import a3c

    return a3c.A3CTrainer, a3c.DEFAULT_CONFIG


def _import_apex():
    from ray.rllib.agents import dqn

    return dqn.ApexTrainer, dqn.apex.APEX_DEFAULT_CONFIG


def _import_apex_ddpg():
    from ray.rllib.agents import ddpg

    return ddpg.ApexDDPGTrainer, ddpg.apex.APEX_DDPG_DEFAULT_CONFIG


def _import_appo():
    from ray.rllib.agents import ppo

    return ppo.APPOTrainer, ppo.appo.DEFAULT_CONFIG


def _import_ars():
    from ray.rllib.agents import ars

    return ars.ARSTrainer, ars.DEFAULT_CONFIG


def _import_bandit_lints():
    from ray.rllib.agents.bandit.bandit import BanditLinTSTrainer

    return BanditLinTSTrainer, BanditLinTSTrainer.get_default_config()


def _import_bandit_linucb():
    from ray.rllib.agents.bandit.bandit import BanditLinUCBTrainer

    return BanditLinUCBTrainer, BanditLinUCBTrainer.get_default_config()


def _import_bc():
    from ray.rllib.agents import marwil

    return marwil.BCTrainer, marwil.DEFAULT_CONFIG


def _import_cql():
    from ray.rllib.agents import cql

    return cql.CQLTrainer, cql.CQL_DEFAULT_CONFIG


def _import_ddpg():
    from ray.rllib.agents import ddpg

    return ddpg.DDPGTrainer, ddpg.DEFAULT_CONFIG


def _import_ddppo():
    from ray.rllib.agents import ppo

    return ppo.DDPPOTrainer, ppo.DEFAULT_CONFIG


def _import_dqn():
    from ray.rllib.agents import dqn

    return dqn.DQNTrainer, dqn.DEFAULT_CONFIG


def _import_dreamer():
    from ray.rllib.agents import dreamer

    return dreamer.DREAMERTrainer, dreamer.DEFAULT_CONFIG


def _import_es():
    from ray.rllib.agents import es

    return es.ESTrainer, es.DEFAULT_CONFIG


def _import_impala():
    from ray.rllib.agents import impala

    return impala.ImpalaTrainer, impala.DEFAULT_CONFIG


def _import_maml():
    from ray.rllib.agents import maml

    return maml.MAMLTrainer, maml.DEFAULT_CONFIG


def _import_marwil():
    from ray.rllib.agents import marwil

    return marwil.MARWILTrainer, marwil.DEFAULT_CONFIG


def _import_mbmpo():
    from ray.rllib.agents import mbmpo

    return mbmpo.MBMPOTrainer, mbmpo.DEFAULT_CONFIG


def _import_pg():
    from ray.rllib.agents import pg

    return pg.PGTrainer, pg.DEFAULT_CONFIG


def _import_ppo():
    from ray.rllib.agents import ppo

    return ppo.PPOTrainer, ppo.DEFAULT_CONFIG


def _import_qmix():
    from ray.rllib.agents import qmix

    return qmix.QMixTrainer, qmix.DEFAULT_CONFIG


def _import_r2d2():
    from ray.rllib.agents import dqn

    return dqn.R2D2Trainer, dqn.R2D2_DEFAULT_CONFIG


def _import_sac():
    from ray.rllib.agents import sac

    return sac.SACTrainer, sac.DEFAULT_CONFIG


def _import_rnnsac():
    from ray.rllib.agents import sac

    return sac.RNNSACTrainer, sac.RNNSAC_DEFAULT_CONFIG


def _import_simple_q():
    from ray.rllib.agents import dqn

    return dqn.SimpleQTrainer, dqn.simple_q.DEFAULT_CONFIG


def _import_slate_q():
    from ray.rllib.agents import slateq

    return slateq.SlateQTrainer, slateq.DEFAULT_CONFIG


def _import_td3():
    from ray.rllib.agents import ddpg

    return ddpg.TD3Trainer, ddpg.td3.TD3_DEFAULT_CONFIG


ALGORITHMS = {
    "A2C": _import_a2c,
    "A3C": _import_a3c,
    "APEX": _import_apex,
    "APEX_DDPG": _import_apex_ddpg,
    "APPO": _import_appo,
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


@Deprecated(new="ray.rllib.agents.registry::get_trainer_class()", error=True)
def get_agent_class(alg: str) -> type:
    return get_trainer_class(alg)


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
