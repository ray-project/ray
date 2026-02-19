import abc

from ray.rllib.core.rl_module.apis import ValueFunctionAPI
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.examples.algorithms.mappo.default_mappo_rl_module import (
    _setup_mappo_encoder,
)
from ray.rllib.utils.annotations import override
from ray.util.annotations import DeveloperAPI


@DeveloperAPI
class SharedCriticRLModule(RLModule, ValueFunctionAPI, abc.ABC):
    """Framework-agnostic shared critic for MAPPO (value head, no policy head).

    The critic receives concatenated observations from all agents and outputs
    one value prediction per agent.  ``compute_values`` must be implemented by
    the framework-specific subclass.

    ``_forward_train`` intentionally returns ``{}`` because the critic's
    forward pass is performed explicitly in the GAE connector and in the
    critic loss function via ``compute_values``.
    """

    @override(RLModule)
    def setup(self):
        _setup_mappo_encoder(self)
        self.vf = self.catalog.build_vf_head(framework=self.framework)

    @override(RLModule)
    def get_initial_state(self) -> dict:
        if hasattr(self.encoder, "get_initial_state"):
            return self.encoder.get_initial_state()
        return {}
