import gymnasium as gym
from ray.rllib.core.rl_module.rl_module import RLModuleSpec
from ray.rllib.core.rl_module.multi_rl_module import MultiRLModuleSpec

from ray.rllib.examples.algorithms.classes.vpg import VPGConfig
from ray.rllib.examples.learners.classes.vpg_torch_learner_shared_encoder import VPGTorchLearnerSharedEncoder
from ray.rllib.examples.envs.classes.multi_agent import MultiAgentCartPole
from ray.rllib.examples.rl_modules.classes.vpg_using_shared_encoder_rlm import (
    SHARED_ENCODER_ID,
    SharedEncoder,
    VPGPolicyAfterSharedEncoder,
    VPGMultiRLModuleWithSharedEncoder,
)

# TODO: Add some command line arguments.

# TODO: Modify VPGLearner to include only one optimizer.

single_agent_env = gym.make("CartPole-v1")

EMBEDDING_DIM = 64  # encoder output dim

config = (
    VPGConfig()
    .environment(MultiAgentCartPole, env_config={"num_agents": 2})
    .training(
        learner_class=VPGTorchLearnerSharedEncoder,
        train_batch_size=200, # temporary
    )
    .env_runners(
        num_env_runners=0,
        num_envs_per_env_runner=1,
    )
    .multi_agent(
        # Declare the two policies trained.
        policies={"p0", "p1"},
        # Agent IDs of `MultiAgentCartPole` are 0 and 1. They are mapped to
        # the two policies with ModuleIDs "p0" and "p1", respectively.
        policy_mapping_fn=lambda agent_id, episode, **kw: f"p{agent_id}"
    )
    .rl_module(
        rl_module_spec=MultiRLModuleSpec(
            multi_rl_module_class=VPGMultiRLModuleWithSharedEncoder,
            rl_module_specs={
                # Shared encoder.
                SHARED_ENCODER_ID: RLModuleSpec(
                    module_class=SharedEncoder,
                    model_config={"embedding_dim": EMBEDDING_DIM},
                    observation_space=single_agent_env.observation_space,
                    action_space=single_agent_env.action_space,  # <-- Added
                ),
                # Large policy net.
                "p0": RLModuleSpec(
                    module_class=VPGPolicyAfterSharedEncoder,
                    model_config={
                        "embedding_dim": EMBEDDING_DIM,
                        "hidden_dim": 1024,
                    },
                ),
                # Small policy net.
                "p1": RLModuleSpec(
                    module_class=VPGPolicyAfterSharedEncoder,
                    model_config={
                        "embedding_dim": EMBEDDING_DIM,
                        "hidden_dim": 64,
                    },
                ),
            },
        ),
    )
)
algo = config.build_algo()

# TODO: replace with the experiment running code.
from ray.rllib.utils.metrics import (
    ENV_RUNNER_RESULTS,
    EPISODE_RETURN_MEAN,
)
import numpy as np

num_iters = 100

for i in range(num_iters):
  results = algo.train()
  if ENV_RUNNER_RESULTS in results:
      mean_return = results[ENV_RUNNER_RESULTS].get(
          EPISODE_RETURN_MEAN, np.nan
      )
      print(f"iter={i} R={mean_return}")