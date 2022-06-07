from rllib2.core.algorithm import Algorithm
from .torch.unit_trainer import PPOTorchUnitTrainer


class PPO(Algorithm):

    def setup(self, config):
        ...
        """
        a container for holding individual trainUnits indexed by string keys for
        cases like multi-agent environments
        by default we have default as the key in case of single agent envs 
        * Each TrainUnit is responsible for updating a set of NNs. 
        * The TrainUnits may share trainable parameters but the updates are independent 
        (e.g. this case may come up when we have a shared encoder to embed the current 
        state of the world) 
        """
        self.unit_trainer_map: Dict[str, TrainUnit] = ...
        ...

    def training_step(self) -> TrainResults:
        """
        Implements the when and what based on the modules that it has access to, e.g.
        self.replay_buffer, self.sampler, .etc

        Returns:
            TrainResults object
        """

        """
        Run all remote workers in parallel and collect samples and put them inside 
        one batch.
        Under the hood the algorithm will run local copies of the policy under each 
        TrainUnit on each worker and collect the on-policy samples and put them inside 
        a MASampleBatch object.
        
        Questions: 
            * Is trajectory post-processing (e.g. advantage standardization) 
            abstracted as a callback or no?
        """
        train_batch: MASampleBatch = ...

        """
        On the local process, update all the NNs based on the collected on-policy 
        trajectories.
        
        Under the hood, the algorithm will loop through each agent's key and call its
        corresponding TrainUnit update method.
        
        Note: we do not do any update_kl or 
        """
        train_results = {}
        for unit_trainer_id, unit_trainer in self.unit_trainer_map.items():
            train_results[unit_trainer_id] = unit_trainer.update(train_batch)

        # TODO: Does this have to be here?
        """
        PPO requires KL update every iteration.
        We basically have to go through each unit-trainer and update the local kl value?
        """
        for unit_trainer_id, unit_trainer in self.unit_trainer_map.items():
            kl_div = train_results[unit_trainer_id]
            unit_trainer.update_kl(kl_div)

        """
        Sync up the updated weights to all remote workers and loop.
        """
        self.synchronize_weights()