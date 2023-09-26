from ray.rllib.algorithms.dqn.dqn_rl_module import DQNRLModule
from ray.rllib.algorithms.sac.sac_catalog import SACCatalog
from ray.rllib.utils.annotations import ExperimentalAPI


@ExperimentalAPI
class SACRLModule(DQNRLModule):
    def setup(self):
        # __sphinx_doc_begin__
        super().setup()
        catalog: SACCatalog = self.config.get_catalog()

        # SAC needs next to the Q function estimator also a
        # value function estimator and the pi network.
        self.v_and_pi_encoder = catalog.build_actor_critic_encoder(
            framework=self.framework
        )
        self.q_encoder = catalog.build_encoder(framework=self.framework)
        self.pi = catalog.build_pi_head(framework=self.framework)
        self.vf = catalog.build_vf_head(framework=self.framework)
        self.qf = catalog.build_qf_head(framework=self.framework)

        self.action_dist_cls = catalog.get_action_dist_cls(framework=self.framework)
        # __sphinx_doc_end__
