"""Registry of algorithm names for `rllib train --run=<alg_name>`"""

import traceback

from ray.rllib.contrib.registry import CONTRIBUTED_ALGORITHMS


def _import_a2c():
    from ray.rllib.agents import a3c
    return a3c.A2CTrainer


def _import_a3c():
    from ray.rllib.agents import a3c
    return a3c.A3CTrainer


def _import_apex():
    from ray.rllib.agents import dqn
    return dqn.ApexTrainer


def _import_apex_ddpg():
    from ray.rllib.agents import ddpg
    return ddpg.ApexDDPGTrainer


def _import_appo():
    from ray.rllib.agents import ppo
    return ppo.APPOTrainer


def _import_ars():
    from ray.rllib.agents import ars
    return ars.ARSTrainer


def _import_bc():
    from ray.rllib.agents import marwil
    return marwil.BCTrainer


def _import_cql():
    from ray.rllib.agents import cql
    return cql.CQLTrainer


def _import_ddpg():
    from ray.rllib.agents import ddpg
    return ddpg.DDPGTrainer


def _import_ddppo():
    from ray.rllib.agents import ppo
    return ppo.DDPPOTrainer


def _import_dqn():
    from ray.rllib.agents import dqn
    return dqn.DQNTrainer


def _import_dreamer():
    from ray.rllib.agents import dreamer
    return dreamer.DREAMERTrainer


def _import_es():
    from ray.rllib.agents import es
    return es.ESTrainer


def _import_impala():
    from ray.rllib.agents import impala
    return impala.ImpalaTrainer


def _import_maml():
    from ray.rllib.agents import maml
    return maml.MAMLTrainer


def _import_marwil():
    from ray.rllib.agents import marwil
    return marwil.MARWILTrainer


def _import_mbmpo():
    from ray.rllib.agents import mbmpo
    return mbmpo.MBMPOTrainer


def _import_pg():
    from ray.rllib.agents import pg
    return pg.PGTrainer


def _import_ppo():
    from ray.rllib.agents import ppo
    return ppo.PPOTrainer


def _import_qmix():
    from ray.rllib.agents import qmix
    return qmix.QMixTrainer


def _import_sac():
    from ray.rllib.agents import sac
    return sac.SACTrainer


def _import_simple_q():
    from ray.rllib.agents import dqn
    return dqn.SimpleQTrainer


def _import_slate_q():
    from ray.rllib.agents import slateq
    return slateq.SlateQTrainer


def _import_td3():
    from ray.rllib.agents import ddpg
    return ddpg.TD3Trainer


ALGORITHMS = {
    "A2C": _import_a2c,
    "A3C": _import_a3c,
    "APEX": _import_apex,
    "APEX_DDPG": _import_apex_ddpg,
    "APPO": _import_appo,
    "ARS": _import_ars,
    "BC": _import_bc,
    "CQL": _import_cql,
    "ES": _import_es,
    "DDPG": _import_ddpg,
    "DDPPO": _import_ddppo,
    "DQN": _import_dqn,
    "SlateQ": _import_slate_q,
    "DREAMER": _import_dreamer,
    "IMPALA": _import_impala,
    "MAML": _import_maml,
    "MARWIL": _import_marwil,
    "MBMPO": _import_mbmpo,
    "PG": _import_pg,
    "PPO": _import_ppo,
    "QMIX": _import_qmix,
    "SAC": _import_sac,
    "SimpleQ": _import_simple_q,
    "TD3": _import_td3,
}


def get_agent_class(alg: str) -> type:
    """Returns the class of a known agent given its name."""

    try:
        return _get_agent_class(alg)
    except ImportError:
        from ray.rllib.agents.mock import _agent_import_failed
        return _agent_import_failed(traceback.format_exc())


def _get_agent_class(alg: str) -> type:
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
