import numpy as np
import gym
import ray
from gym.spaces import Box
from ray.rllib.policy import Policy
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.typing import ModelWeights
from ray.rllib.policy.view_requirement import ViewRequirement
from ray.rllib.agents.trainer_template import build_trainer

# This policy does not much with the state, but shows, 
# how the training# Trajectory View API can be used to
# pass user-specific view requirements to RLlib. 
class MyPolicy(Policy):
    def __init__(self, observation_space, action_space, model_config, *args, **kwargs):
        super(MyPolicy, self).__init__(observation_space, action_space, model_config, *args, **kwargs)
        self.observation_space = observation_space
        self.action_space = action_space
        self.model_config = model_config or {}
        space = Box(low=-np.inf, high=np.inf, shape=(10,), dtype=np.float64)
        # Set view requirements such that the policy state is held in
        # memory for 2 environment steps. 
        self.view_requirements['state_in_0'] = \
            ViewRequirement('state_out_0',
                            shift="-{}:-1".format(2),
                            used_for_training=False,
                            used_for_compute_actions=True,
                            batch_repeat_value=1)
        self.view_requirements['state_out_0'] = \
            ViewRequirement(
                    space=space, 
                    used_for_training=False,
                    used_for_compute_actions=True,
                    batch_repeat_value=1)
    # Set the initial state. This is necessary for starting
    # the policy. 
    def get_initial_state(self):
        return [
            np.zeros((10,), dtype=np.float32)
        ]

    def compute_actions(self,
                        obs_batch=None,
                        state_batches=None,
                        prev_action_batch=None,
                        prev_reward_batch=None,
                        info_batch=None,
                        episodes=None,
                        **kwargs):
        # First dimension is the batch (in list), second is the number of states,
        # and third is the shift. Fourth is the size of the state.
        batch_size = state_batches[0].shape[0]        
        actions = np.random.random(batch_size) 
        new_state_batches = [state_batch for state_batch in state_batches[0][0]]
        return actions, [new_state_batches], {}
    
    def compute_actions_from_input_dict(self,
                                        input_dict,
                                        explore = None,
                                        timestep = None,
                                        episodes = None,
                                        **kwargs):
        # Default implementation just passes obs, prev-a/r, and states on to
        # `self.compute_actions()`.  
        state_batches = [
            s for k, s in input_dict.items() if k[:9] == "state_in_"
        ]
        # Make sure that two (shift="-2:-1") past states are contained in the
        # state_batch.
        assert state_batches[0].shape[1] == 2
        return self.compute_actions(
            input_dict[SampleBatch.OBS],
            state_batches,
            prev_action_batch=input_dict.get(SampleBatch.PREV_ACTIONS),
            prev_reward_batch=input_dict.get(SampleBatch.PREV_REWARDS),
            info_batch=input_dict.get(SampleBatch.INFOS),
            explore=explore,
            timestep=timestep,
            episodes=episodes,
            **kwargs,
        )

    def learn_on_batch(self, samples):
        return

    @override(Policy)
    def get_weights(self) -> ModelWeights:
        """No weights to save."""
        return {}

    @override(Policy)
    def set_weights(self, weights: ModelWeights) -> None:
        """No weights to set."""
        pass

MyTrainer = build_trainer(
    name="MyPolicy",
    default_policy=MyPolicy)

class MyEnv(gym.Env):  
    def __init__(self, env_config=None):
        super(MyEnv, self).__init__()
        self.config = env_config or {}
        self.observation_space = Box(low=-1.0, high=1.0, shape=(), dtype=np.float32)
        self.action_space = Box(low=-np.Inf, high=np.Inf, shape=(), dtype=np.float32)

    def reset(self):
        self.timestep = 0        
        return self.observation_space.sample()

    def step(self, action):
        self.timestep += 1 
        done = True if self.timestep >= 10 else False
        chance = np.random.random()
        reward = np.absolute(chance) if chance > action else 0.0
        new_obs = np.array(np.random.random())
        return new_obs, reward, done, {}

config = {
    "env": MyEnv,
    "model": {
        "max_seq_len": 1, # Necessary to get the whole trajectory of 'state_in_0' in the sample batch
    },
    "num_workers": 1, 
    "framework": None,  # NOTE: Does this have consequences? I use it for not loading tensorflow/pytorch
    "log_level": "DEBUG",
    "create_env_on_driver": True, 
    "evaluation_num_episodes": 0, # No evaluation for deterministic policy. 
    "normalize_actions": False, # Actions are explicit and no logits. 
}
# Start ray and train a trainer with our policy. 
ray.init(ignore_reinit_error=True, local_mode=True)
my_trainer = MyTrainer(config=config)
results = my_trainer.train()
print(results)