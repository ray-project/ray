import sys
from ray.rllib.agents.trainer import Trainer, with_common_config
import ray.rllib.agents.evolutionary.ars as ARS
import ray.rllib.agents.evolutionary.es as ES
import ray.rllib.agents.meta.maml as MAML
import ray.rllib.agents.misc.slateq as SlateQ
import ray.rllib.agents.model_based.mbmpo as MBMPO
import ray.rllib.agents.model_based.dreamer as Dreamer
import ray.rllib.agents.multi_agent.qmix as QMIX
import ray.rllib.agents.offline.cql as CQL
import ray.rllib.agents.offline.marwil as MARWIL
from ray.rllib.utils.deprecation import deprecation_warning

# Will be deprecated soon, old path names will still give the right class
sys.modules["ray.rllib.agents.ars"] = ARS
sys.modules["ray.rllib.agents.es"] = ES
sys.modules["ray.rllib.agents.maml"] = MAML
sys.modules["ray.rllib.agents.slateq"] = SlateQ
sys.modules["ray.rllib.agents.mbmpo"] = MBMPO
sys.modules["ray.rllib.agents.dreamer"] = Dreamer
sys.modules["ray.rllib.agents.qmix"] = QMIX
sys.modules["ray.rllib.agents.cql"] = CQL
sys.modules["ray.rllib.agents.marwil"] = MARWIL

deprecation_warning(
    old="ray.rllib.agents.ars",
    new="ray.rllib.agents.evolutionary.ars",
    error=False,
)

deprecation_warning(
    old="ray.rllib.agents.es",
    new="ray.rllib.agents.evolutionary.es",
    error=False,
)

deprecation_warning(
    old="ray.rllib.agents.maml",
    new="ray.rllib.agents.meta.maml",
    error=False,
)

deprecation_warning(
    old="ray.rllib.agents.slateq",
    new="ray.rllib.agents.misc.slateq",
    error=False,
)

deprecation_warning(
    old="ray.rllib.agents.mbmpo",
    new="ray.rllib.agents.model_based.mbmpo",
    error=False,
)

deprecation_warning(
    old="ray.rllib.agents.dreamer",
    new="ray.rllib.agents.model_based.dreamer",
    error=False,
)

deprecation_warning(
    old="ray.rllib.agents.qmix",
    new="ray.rllib.agents.multi_agent.qmix",
    error=False,
)

deprecation_warning(
    old="ray.rllib.agents.cql",
    new="ray.rllib.agents.offline.cql",
    error=False,
)

deprecation_warning(
    old="ray.rllib.agents.marwil",
    new="ray.rllib.agents.offline.marwil",
    error=False,
)

__all__ = [
    "Trainer",
    "with_common_config",
]
