"""Registry of algorithm names for `rllib train --run=<alg_name>`"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import traceback

from ray.rllib.contrib.registry import CONTRIBUTED_ALGORITHMS


def _import_sac():
    from ray.rllib.agents import sac
    return sac.SACTrainer


def _import_appo():
    from ray.rllib.agents import ppo
    return ppo.APPOTrainer


def _import_qmix():
    from ray.rllib.agents import qmix
    return qmix.QMixTrainer


def _import_apex_qmix():
    from ray.rllib.agents import qmix
    return qmix.ApexQMixTrainer


def _import_ddpg():
    from ray.rllib.agents import ddpg
    return ddpg.DDPGTrainer


def _import_apex_ddpg():
    from ray.rllib.agents import ddpg
    return ddpg.ApexDDPGTrainer


def _import_td3():
    from ray.rllib.agents import ddpg
    return ddpg.TD3Trainer


def _import_ppo():
    from ray.rllib.agents import ppo
    return ppo.PPOTrainer


def _import_es():
    from ray.rllib.agents import es
    return es.ESTrainer


def _import_ars():
    from ray.rllib.agents import ars
    return ars.ARSTrainer


def _import_dqn():
    from ray.rllib.agents import dqn
    return dqn.DQNTrainer


def _import_simple_q():
    from ray.rllib.agents import dqn
    return dqn.SimpleQTrainer


def _import_apex():
    from ray.rllib.agents import dqn
    return dqn.ApexTrainer


def _import_a3c():
    from ray.rllib.agents import a3c
    return a3c.A3CTrainer


def _import_a2c():
    from ray.rllib.agents import a3c
    return a3c.A2CTrainer


def _import_pg():
    from ray.rllib.agents import pg
    return pg.PGTrainer


def _import_impala():
    from ray.rllib.agents import impala
    return impala.ImpalaTrainer


def _import_marwil():
    from ray.rllib.agents import marwil
    return marwil.MARWILTrainer


ALGORITHMS = {
    "SAC": _import_sac,
    "DDPG": _import_ddpg,
    "APEX_DDPG": _import_apex_ddpg,
    "TD3": _import_td3,
    "PPO": _import_ppo,
    "ES": _import_es,
    "ARS": _import_ars,
    "DQN": _import_dqn,
    "SimpleQ": _import_simple_q,
    "APEX": _import_apex,
    "A3C": _import_a3c,
    "A2C": _import_a2c,
    "PG": _import_pg,
    "IMPALA": _import_impala,
    "QMIX": _import_qmix,
    "APEX_QMIX": _import_apex_qmix,
    "APPO": _import_appo,
    "MARWIL": _import_marwil,
}


def get_agent_class(alg):
    """Returns the class of a known agent given its name."""

    try:
        return _get_agent_class(alg)
    except ImportError:
        from ray.rllib.agents.mock import _agent_import_failed
        return _agent_import_failed(traceback.format_exc())


def _get_agent_class(alg):
    if alg in ALGORITHMS:
        return ALGORITHMS[alg]()
    elif alg in CONTRIBUTED_ALGORITHMS:
        return CONTRIBUTED_ALGORITHMS[alg]()
    elif alg == "script":
        from ray.tune import script_runner
        return script_runner.ScriptRunner
    elif alg == "__fake":
        from ray.rllib.agents.mock import _MockTrainer
        return _MockTrainer
    elif alg == "__sigmoid_fake_data":
        from ray.rllib.agents.mock import _SigmoidFakeData
        return _SigmoidFakeData
    elif alg == "__parameter_tuning":
        from ray.rllib.agents.mock import _ParameterTuningTrainer
        return _ParameterTuningTrainer
    else:
        raise Exception(("Unknown algorithm {}.").format(alg))
