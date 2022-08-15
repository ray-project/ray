from rllib2.core.algorithm import Algorithm


class PPO(Algorithm):
    def setup(self, config):
        """
        RLLib already sets this up for us.
        self.rl_trainer = MARLTrainer(...)
        self.callbacks = CallbackList(...)
        pass
        """

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
        policyID on each worker and collect the on-policy samples and put them inside 
        a MASampleBatch object.

        Note: unlike before where the postprocess_trajectory inside the policy got 
        called it won't get impliciltly called here. We can either provide a callback 
        for env/architecture specific postprocessings or if it is an algorithm specific 
        postprocessings (e.g. ppo needs compute_gae) we will be explicit about it 
        during the loss computation inside RLModule. 
        """

        train_batch: MultiAgentSampleBatch = ...

        """
        On the local process, update all the NNs based on the collected on-policy 
        trajectories. In the multi-agent case it is assumed we will have a collection 
        of PPOtrainers. So they all have the same logic for updating the NNs.
        
        Under the hood, the algorithm will loop through each RLModuleID key and call its
        corresponding RLTrainer update method. ppo also updates kl divergence on every 
        iteration. 
        
        Note: We can control some event-based updates by setting specific kwargs flags 
        to true. These flags will be broadcasted to each individual trainers update() 
        method. It can also be a dictionary like update_kl = {'policy_1': True, 
        'policy_2': False} to provide different update rules for different policies.
        """
        self.rl_trainer.update(train_batch, update_kl=True)

        """
        Sync up the updated weights to all remote workers and loop (same as before)
        """
        self.synchronize_weights(...)
