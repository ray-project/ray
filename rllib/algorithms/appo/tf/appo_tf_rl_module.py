from ray.rllib.algorithms.ppo.tf.ppo_tf_rl_module import PPOTfRLModule


class APPOTfRLModule(PPOTfRLModule):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        catalog = self.config.get_catalog()
        self.target_pi = catalog.build_pi_head(framework=self.framework)
    
    def _forward_train(self, batch: NestedDict):
        return super()._forward_train(batch)
