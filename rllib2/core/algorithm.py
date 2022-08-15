class Algorithm(...):
    @property
    def rl_trainer(self) -> MARLTrainer:
        return self.workers.local_worker().rl_trainer
