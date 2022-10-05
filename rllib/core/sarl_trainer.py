class SARLTrainer:
    def train(self, batch):
        # this is the function is called by the user to train the model
        for queue in self.queues:
            queue.put(batch)
        results = next(self.training_iterator)
        self.curr_weights = results[0]["module_weights"]
        return results

    @staticmethod
    def compute_loss(batch, fwd_out, **kwargs):
        raise NotImplementedError

    @staticmethod
    def compute_grads_and_apply_if_needed(batch, fwd_out, loss_out, **kwargs):
        raise NotImplementedError

    @staticmethod
    def init_rl_module(module_config):
        raise NotImplementedError

    @staticmethod
    def init_optimizer(module, optimizer_config):
        raise NotImplementedError

    def get_weights(self):
        raise NotImplementedError

    @staticmethod
    def _training_func(config):
        raise NotImplementedError

    def _make_ray_train_trainer(self, scaling_config, init_rl_module_fn, module_config):
        raise NotImplementedError
