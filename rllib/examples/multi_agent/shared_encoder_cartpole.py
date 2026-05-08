"""A runnable example involving the use of a shared encoder module.

How to run this script
----------------------
`python [script file name].py --num-agents=2`

Control the number of agents and policies (RLModules) via --num-agents.
--encoder-emb-dim sets the encoder output dimension, and --no-shared-encoder
runs the experiment with independent encoders.

For debugging, use the following additional command line options
`--no-tune --num-env-runners=0`
which should allow you to set breakpoints anywhere in the RLlib code and
have the execution stop there for inspection and debugging.

For logging to your WandB account, use:
`--wandb-key=[your WandB API key] --wandb-project=[some project name]
--wandb-run-name=[optional: WandB run name (within the defined project)]`


Results to expect
-----------------
Under the shared encoder architecture, the target reward of 700 will typically be reached well before 100,000 iterations. A trial concludes as below:

+---------------------+------------+-----------------+--------+------------------+-------+-------------------+-------------+-------------+
| Trial name          | status     | loc             |   iter |   total time (s) |    ts |   combined return |   return p1 |   return p0 |
|---------------------+------------+-----------------+--------+------------------+-------+-------------------+-------------+-------------|
| VPG_env_ab318_00000 | TERMINATED | 127.0.0.1:37375 |     33 |          44.2689 | 74197 |            611.35 |      191.71 |      419.64 |
+---------------------+------------+-----------------+--------+------------------+-------+-------------------+-------------+-------------+

Without a shared encoder, a lower reward is typically achieved after training for the full 100,000 timesteps:

+---------------------+------------+-----------------+--------+------------------+--------+-------------------+-------------+-------------+
| Trial name          | status     | loc             |   iter |   total time (s) |     ts |   combined return |   return p0 |   return p1 |
|---------------------+------------+-----------------+--------+------------------+--------+-------------------+-------------+-------------|
| VPG_env_2e79e_00000 | TERMINATED | 127.0.0.1:39076 |     37 |           52.127 | 103894 |            526.66 |       85.78 |      440.88 |
+---------------------+------------+-----------------+--------+------------------+--------+-------------------+-------------+-------------+


"""

import gymnasium as gym

from ray.rllib.core.rl_module.multi_rl_module import MultiRLModuleSpec
from ray.rllib.core.rl_module.rl_module import RLModuleSpec
from ray.rllib.examples.algorithms.classes.vpg import VPGConfig
from ray.rllib.examples.envs.classes.multi_agent import MultiAgentCartPole
from ray.rllib.examples.learners.classes.vpg_torch_learner_shared_optimizer import (
    VPGTorchLearnerSharedOptimizer,
)
from ray.rllib.examples.rl_modules.classes.vpg_using_shared_encoder_rlm import (
    SHARED_ENCODER_ID,
    SharedEncoder,
    VPGMultiRLModuleWithSharedEncoder,
    VPGPolicyAfterSharedEncoder,
    VPGPolicyNoSharedEncoder,
)
from ray.rllib.examples.utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)
from ray.tune.registry import register_env

parser = add_rllib_example_script_args(
    default_iters=200,
    default_timesteps=100000,
    default_reward=600.0,
)
parser.set_defaults(
    algo="VPG",
    num_agents=2,
)
parser.add_argument("--encoder-emb-dim", type=int, default=64)
parser.add_argument("--no-shared-encoder", action="store_true")

if __name__ == "__main__":
    args = parser.parse_args()
    assert args.algo == "VPG", "The shared encoder example is meant for VPG agents."
    assert args.num_agents == 2, "This example makes use of two agents."

    single_agent_env = gym.make(
        "CartPole-v1"
    )  # To allow instantiation of shared encoder

    EMBEDDING_DIM = args.encoder_emb_dim  # encoder output dim

    if args.no_shared_encoder:
        print("Running experiment without shared encoder")
        specs = MultiRLModuleSpec(
            rl_module_specs={
                # Large policy net.
                "p0": RLModuleSpec(
                    module_class=VPGPolicyNoSharedEncoder,
                    model_config={
                        "embedding_dim": EMBEDDING_DIM,
                        "hidden_dim": 64,
                    },
                ),
                # Small policy net.
                "p1": RLModuleSpec(
                    module_class=VPGPolicyNoSharedEncoder,
                    model_config={
                        "embedding_dim": EMBEDDING_DIM,
                        "hidden_dim": 64,
                    },
                ),
            }
        )
    else:
        specs = MultiRLModuleSpec(
            multi_rl_module_class=VPGMultiRLModuleWithSharedEncoder,
            rl_module_specs={
                # Shared encoder.
                SHARED_ENCODER_ID: RLModuleSpec(
                    module_class=SharedEncoder,
                    model_config={"embedding_dim": EMBEDDING_DIM},
                    observation_space=single_agent_env.observation_space,
                    action_space=single_agent_env.action_space,
                ),
                # Large policy net.
                "p0": RLModuleSpec(
                    module_class=VPGPolicyAfterSharedEncoder,
                    model_config={
                        "embedding_dim": EMBEDDING_DIM,
                        "hidden_dim": 64,
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
        )

    # Register our environment with tune.
    register_env(
        "env",
        lambda _: MultiAgentCartPole(config={"num_agents": args.num_agents}),
    )

    base_config = (
        VPGConfig()
        .environment("env" if args.num_agents > 0 else "CartPole-v1")
        .training(
            learner_class=VPGTorchLearnerSharedOptimizer
            if not args.no_shared_encoder
            else None,
            train_batch_size=2048,
            lr=1e-2,
        )
        .multi_agent(
            policies={"p0", "p1"},
            policy_mapping_fn=lambda agent_id, episode, **kw: f"p{agent_id}",
        )
        .rl_module(
            rl_module_spec=specs,
        )
    )

    run_rllib_example_script_experiment(base_config, args)
