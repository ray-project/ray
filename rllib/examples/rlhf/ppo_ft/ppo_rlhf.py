
import torch
import numpy as np
from ray.rllib.algorithms import Algorithm, AlgorithmConfig
from ray.rllib.algorithms.ppo import PPO
from ray.rllib.policy.sample_batch import SampleBatch, concat_samples, DEFAULT_POLICY_ID
from ray.rllib.core.learner.learner_group import LearnerGroup
from ray.rllib.examples.rlhf.ppo_ft.rlhf_env import generate_response
from ray.rllib.evaluation.postprocessing import Postprocessing
from ray.rllib.utils.metrics import (
    NUM_AGENT_STEPS_SAMPLED, NUM_ENV_STEPS_SAMPLED, LEARNER_STATS_KEY
)
from ray.rllib.examples.rlhf.ppo_ft.rlhf_buffer import Buffer, BufferItem

class RLHFSampler:
    """This sampler is a local sampler for LLMEnv. 
    
    The underlying env is an LLMEnv which creates a batch of prompts and the agent has 
    to generate a response for each prompt. Then the env evaluate those responses and 
    returns a reward signal. 
    """

    def __init__(self, module, env):
        
        self._env = env
        self._module = module
        self.max_generation_length = self._env.max_generation_length


    def sample(self, batch_size: int, **kwargs) -> SampleBatch:
        
        # TODO (Kourosh): Can we use batch inference here? 
        batches = Buffer()
        for i in range(batch_size):
            obs, _ = self._env.reset(seed=i + 44)

            output = generate_response(
                self._module.actor, 
                input_ids=torch.tensor(obs['input_ids'])[None],
                max_length=self.max_generation_length,
                eos_token_id=self._env.tokenizer.eos_token_id,
            )

            # construct the action
            n_generated_tokens = output["n_generated_tokens"]
            n_input_tokens = output["n_input_tokens"]
            generated_tokens = output["sequence"][-n_generated_tokens:]

            value = self._module.critic(output["sequence"]).detach().item()
                                        
            action = {
                "sequence": generated_tokens.detach().numpy()[0],
                "response_mask": np.array([0]*n_input_tokens + [1]*n_generated_tokens),
                "probs": output["probs"].detach().numpy()[0], # remove batch dimension
                "attention_mask": np.array([1]*(n_input_tokens + n_generated_tokens)),
            }

            next_obs, reward, terminated, truncated, info = self._env.step(action)

            assert terminated == True, "The env should be terminated after each step."

            # value and reward should be both float scalars here.
            advantages = value - reward

            batches.append(
                BufferItem(
                    obs=obs,
                    action=action,
                    reward=reward,
                    value=value,
                    advantage=advantages,
                )
            )


        return batches.convert_to_sample_batch()


class PPORLHF(PPO):
    def setup(self, config: AlgorithmConfig) -> None:
        super().setup(config)

        # TODO (Kourosh): This is the easiest setup, i.e. we only have one replica of the model in the entire workload, so the learner has to be setup in local mode.
        self.rlhf_module = self.learner_group._learner.module[DEFAULT_POLICY_ID]

        # Another option is to use the module_spec to build the module and keep 
        # separate copy in learner. Everytime we update the weights on the learner the 
        # weights have to be broadcasted back to the sampler.
        # module_spec = self.workers.local_worker().marl_module_spec
        # self.rlhf_module = module_spec.build()[DEFAULT_POLICY_ID]

        self.env = self.workers.local_worker().env
        
        # create a copy of module and env in the algorithm.
        self.sampler = RLHFSampler(self.rlhf_module, self.env)

    def training_step(self):

        train_batch = self.sampler.sample(batch_size=2)
        breakpoint()
        train_batch = train_batch.as_multi_agent()
        self._counters[NUM_AGENT_STEPS_SAMPLED] += train_batch.agent_steps()
        self._counters[NUM_ENV_STEPS_SAMPLED] += train_batch.env_steps()

        train_results = self.learner_group.update(
            train_batch,
            minibatch_size=self.config.sgd_minibatch_size,
            num_iters=self.config.num_sgd_iter,
        )

        policies_to_update = {DEFAULT_POLICY_ID}
        kl_dict = {
            pid: train_results[pid][LEARNER_STATS_KEY].get("kl")
            for pid in policies_to_update
        }
        self.learner_group.additional_update(
            module_ids_to_update=policies_to_update,
            sampled_kl_values=kl_dict,
            timestep=self._counters[NUM_AGENT_STEPS_SAMPLED],
        )

        self.sampler.sync_weights_from_learner_group(self.learner_group)
