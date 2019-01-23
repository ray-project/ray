"""Registry of algorithm names for `rllib train --run=<alg_name>`"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import traceback

from ray.rllib.contrib.registry import CONTRIBUTED_ALGORITHMS


def _import_appo():
    from ray.rllib.agents import ppo
    return ppo.APPOAgent


def _import_qmix():
    from ray.rllib.agents import qmix
    return qmix.QMixAgent


def _import_apex_qmix():
    from ray.rllib.agents import qmix
    return qmix.ApexQMixAgent


def _import_ddpg():
    from ray.rllib.agents import ddpg
    return ddpg.DDPGAgent


def _import_apex_ddpg():
    from ray.rllib.agents import ddpg
    return ddpg.ApexDDPGAgent


def _import_ppo():
    from ray.rllib.agents import ppo
    return ppo.PPOAgent


def _import_es():
    from ray.rllib.agents import es
    return es.ESAgent


def _import_ars():
    from ray.rllib.agents import ars
    return ars.ARSAgent


def _import_dqn():
    from ray.rllib.agents import dqn
    return dqn.DQNAgent


def _import_apex():
    from ray.rllib.agents import dqn
    return dqn.ApexAgent


def _import_a3c():
    from ray.rllib.agents import a3c
    return a3c.A3CAgent


def _import_a2c():
    from ray.rllib.agents import a3c
    return a3c.A2CAgent


def _import_pg():
    from ray.rllib.agents import pg
    return pg.PGAgent


def _import_impala():
    from ray.rllib.agents import impala
    return impala.ImpalaAgent


def _import_marwil():
    from ray.rllib.agents import marwil
    return marwil.MARWILAgent


ALGORITHMS = {
    "DDPG": _import_ddpg,
    "APEX_DDPG": _import_apex_ddpg,
    "PPO": _import_ppo,
    "ES": _import_es,
    "ARS": _import_ars,
    "DQN": _import_dqn,
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
        from ray.rllib.agents.mock import _MockAgent
        return _MockAgent
    elif alg == "__sigmoid_fake_data":
        from ray.rllib.agents.mock import _SigmoidFakeData
        return _SigmoidFakeData
    elif alg == "__parameter_tuning":
        from ray.rllib.agents.mock import _ParameterTuningAgent
        return _ParameterTuningAgent
    else:
        raise Exception(("Unknown algorithm {}.").format(alg))
