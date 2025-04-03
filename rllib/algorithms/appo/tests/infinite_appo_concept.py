import random
import time

import gymnasium as gym
import numpy as np

import ray
from ray.rllib.algorithms.appo import APPOConfig
from ray.rllib.algorithms.infinite_appo.infinite_appo_multi_agent_env_runner import InfiniteAPPOMultiAgentEnvRunner
from ray.rllib.algorithms.infinite_appo.torch.infinite_appo_torch_learner import InfiniteAPPOTorchLearner
from ray.rllib.algorithms.infinite_appo.utils import MetricsActor, WeightsServerActor, BatchDispatcher
from ray.rllib.connectors.env_to_module.frame_stacking import FrameStackingEnvToModule
from ray.rllib.connectors.learner.frame_stacking import FrameStackingLearner
from ray.rllib.core import ALL_MODULES
from ray.rllib.core.rl_module.default_model_config import DefaultModelConfig
from ray.rllib.core.rl_module.multi_rl_module import MultiRLModuleSpec
from ray.rllib.core.rl_module.rl_module import RLModuleSpec
from ray.rllib.env.multi_agent_env import make_multi_agent
from ray.rllib.env.wrappers.atari_wrappers import wrap_atari_for_new_api_stack
from ray.rllib.policy.sample_batch import MultiAgentBatch, SampleBatch
from ray.rllib.utils.metrics.metrics_logger import MetricsLogger


def _make_fake(B, *, return_ray_ref=False, observation_space, action_space):
    _fake_batch = MultiAgentBatch(
        policy_batches={
            "p0": SampleBatch({
                "obs": np.random.random(size=(B,) + tuple(observation_space.shape)),
                "actions": np.random.randint(0, action_space.n, size=(B,)),
                "terminateds": np.random.random(size=(B,)).astype(bool),
                "truncateds": np.random.random(size=(B,)).astype(bool),
                "loss_mask": np.ones(shape=(B,)).astype(bool),
                "rewards": np.random.random(size=(B,)),
                "action_logp": np.random.random(size=(B,)),
                "action_probs": np.random.random(size=(B,)),
            }),
        },
        env_steps=B,
    )
    if return_ray_ref:
        return ray.put(_fake_batch)
    else:
        return _fake_batch


if __name__ == "__main__":
    NUM_ENV_RUNNERS = 1
    NUM_ENVS_PER_ENV_RUNNER = 5
    NUM_AGG_ACTORS_PER_LEARNER = 2
    NUM_LEARNERS = 2
    NUM_WEIGHTS_SERVER_ACTORS = 1
    NUM_BATCH_DISPATCHERS = 1
    NUM_GPUS_PER_LEARNER = 0


    MultiAgentPong = make_multi_agent(_env_creator)
    NUM_AGENTS = 1
    NUM_POLICIES = 1
    main_spec = RLModuleSpec(
        model_config=DefaultModelConfig(
            vf_share_layers=True,
            conv_filters=[(16, 4, 2), (32, 4, 2), (64, 4, 2), (128, 4, 2)],
            conv_activation="relu",
            head_fcnet_hiddens=[256],
        ),
    )

    config = (
        APPOConfig()
        .framework(torch_skip_nan_gradients=True)
        .environment(
            MultiAgentPong,
            env_config={
                "num_agents": NUM_AGENTS,
                # Make analogous to old v4 + NoFrameskip.
                "frameskip": 1,
                "full_action_space": False,
                "repeat_action_probability": 0.0,
            },
            clip_rewards=True,
        )
        .env_runners(
            env_to_module_connector=_make_env_to_module_connector,
            num_env_runners=NUM_ENV_RUNNERS,
            rollout_fragment_length=50,
            num_envs_per_env_runner=NUM_ENVS_PER_ENV_RUNNER,
        )
        .learners(
            num_learners=NUM_LEARNERS,
            num_gpus_per_learner=NUM_GPUS_PER_LEARNER,
            num_cpus_per_learner=1,
            num_aggregator_actors_per_learner=NUM_AGG_ACTORS_PER_LEARNER,
        )
        .training(
            learner_class=InfiniteAPPOTorchLearner,
            learner_connector=_make_learner_connector,
            train_batch_size_per_learner=500,
            target_network_update_freq=2,
            lr=0.0005 * ((NUM_LEARNERS or 1) ** 0.5),
            vf_loss_coeff=1.0,
            entropy_coeff=[[0, 0.01], [3000000, 0.0]],  # <- crucial parameter to finetune
            # Only update connector states and model weights every n training_step calls.
            broadcast_interval=5,
            # learner_queue_size=1,
            circular_buffer_num_batches=4,
            circular_buffer_iterations_per_batch=2,
            num_gpu_loader_threads=1,
        )
        .rl_module(
            rl_module_spec=MultiRLModuleSpec(
                rl_module_specs=(
                    {f"p{i}": main_spec for i in range(NUM_POLICIES)}
                    #| {"random": RLModuleSpec(module_class=RandomRLModule)}
                ),
            ),
        )
        .multi_agent(
            policies={f"p{i}" for i in range(NUM_POLICIES)},# | {"random"},
            policy_mapping_fn=lambda aid, eps, **kw: (
                random.choice([f"p{i}" for i in range(NUM_POLICIES)]) # + ["random"]
            ),
            policies_to_train=[f"p{i}" for i in range(NUM_POLICIES)],
        )
    )

    #pg = placement_group(
    #    # 1: -> skip main-process bundle (not needed b/c we are running w/o Tune).
    #    APPO.default_resource_request(config).bundles[1:] + [
    #        {"CPU": 1}
    #        # +1=metrics actor
    #        for _ in range(NUM_BATCH_DISPATCHERS + NUM_WEIGHTS_SERVER_ACTORS + 1)
    #    ],
    #    strategy=config.placement_strategy,
    #)
    #ray.get(pg.ready())

    algo = Algo(
        config=config,
        observation_space=gym.spaces.Box(-1.0, 1.0, (64, 64, 4), np.float32),
        action_space=gym.spaces.Discrete(6),
        num_weights_server_actors=NUM_WEIGHTS_SERVER_ACTORS,
        num_batch_dispatchers=NUM_BATCH_DISPATCHERS,
        #placement_group=pg,
    )
    time.sleep(1.0)

    for iteration in range(10000000000):
        results = algo.train()
        msg = f"{iteration}) "
        if "env_runners" in results:
            env_steps_sampled = results['env_runners']['num_env_steps_sampled_lifetime']
            msg += (
                f"sampled={env_steps_sampled.peek()} "
                f"({env_steps_sampled.peek(throughput=True):.0f}/sec) "
            )
        if "aggregator_actors" in results:
            env_steps_aggregated = results['aggregator_actors'][
                'num_env_steps_aggregated_lifetime']
            msg += (
                f"aggregated={env_steps_aggregated.peek()} "
                f"({env_steps_aggregated.peek(throughput=True):.0f}/sec) "
            )
        if "learners" in results:
            learner_results = results["learners"]
            if ALL_MODULES in learner_results and "num_env_steps_trained_lifetime" in learner_results[ALL_MODULES]:
                env_steps_trained = learner_results[ALL_MODULES]["num_env_steps_trained_lifetime"]
                msg += (
                    f"trained={env_steps_trained.peek()} "
                    f"({env_steps_trained.peek(throughput=True):.0f}/sec) "
                )
            #if "p0" in learner_results:
            #    msg += f"grad-update-delta={learner_results['p0']['diff_num_grad_updates_vs_sampler_policy'].peek()} "

        print(msg)
