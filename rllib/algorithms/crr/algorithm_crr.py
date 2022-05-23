
from ray.rllib.utils.typing import ResultDict, TrainerConfigDict
from ray.rllib.algorithms.cql import CQLTrainer

from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID

class CRR(CQLTrainer):

    def training_iteration(self) -> ResultDict:
        return super().training_iteration()
        # # Sample training batch from replay buffer.
        # train_batch = self.local_replay_buffer.sample(self.config["train_batch_size"])
        #
        # # TODO: is sample batch always gonna be a MultiAgentSampleBatch?
        # train_batch = train_batch.policy_batches[DEFAULT_POLICY_ID]
        #
        # # train batch is Sample Batch of form (s_t, a_t, s_tp1, r_t)
        #
        # # compute the advantage
        # if self.config.get("simple_optimizer") is True:
        #     train_results = train_one_step(self, train_batch)
        # else:
        #     train_results = multi_gpu_train_one_step(self, train_batch)
        #
        # # Update replay buffer priorities.
        # update_priorities_in_replay_buffer(
        #     self.local_replay_buffer,
        #     self.config,
        #     train_batch,
        #     train_results,
        # )
        #
        # return {}
