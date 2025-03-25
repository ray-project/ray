import random
import time
import threading

import gymnasium as gym
import numpy as np

import ray
from ray.rllib.algorithms.appo import APPOConfig
from ray.rllib.algorithms.appo.torch.appo_torch_learner import APPOTorchLearner
from ray.rllib.algorithms.impala.impala_learner import (
    _CURRENT_GLOBAL_TIMESTEPS,
    LEARNER_THREAD_ENV_STEPS_DROPPED,
    QUEUE_SIZE_GPU_LOADER_QUEUE,
)
from ray.rllib.algorithms.utils import AggregatorActor
from ray.rllib.connectors.env_to_module.frame_stacking import FrameStackingEnvToModule
from ray.rllib.connectors.learner.frame_stacking import FrameStackingLearner
from ray.rllib.core import ALL_MODULES, COMPONENT_RL_MODULE
from ray.rllib.core.rl_module.default_model_config import DefaultModelConfig
from ray.rllib.core.rl_module.multi_rl_module import MultiRLModuleSpec
from ray.rllib.core.rl_module.rl_module import RLModuleSpec
from ray.rllib.env.multi_agent_env import make_multi_agent
from ray.rllib.env.multi_agent_env_runner import MultiAgentEnvRunner
from ray.rllib.env.wrappers.atari_wrappers import wrap_atari_for_new_api_stack
from ray.rllib.examples.rl_modules.classes.random_rlm import RandomRLModule
from ray.rllib.policy.sample_batch import MultiAgentBatch, SampleBatch
from ray.rllib.utils.metrics.metrics_logger import MetricsLogger
from ray.rllib.utils.test_utils import add_rllib_example_script_args


class Algo:
    def __init__(
        self,
        *,
        config: APPOConfig,
        observation_space,
        action_space,
        num_weights_server_actors=1,
        num_aggregator_actors,
        num_batch_dispatchers=1,
    ):
        self.observation_space = observation_space
        self.action_space = action_space
        self.config = config
        self.metrics = MetricsLogger()

        # Create weights server actors.
        self.weights_server_actors = [
            WeightsServerActor.remote() for _ in range(num_weights_server_actors)
        ]
        for aid, actor in enumerate(self.weights_server_actors):
            actor.add_peers.remote(
                self.weights_server_actors[:aid] + self.weights_server_actors[aid + 1:])
        # Create metrics actor.
        self.metrics_actor = MetricsActor.remote()
        # Create batch dispatcher actors.
        self.batch_dispatcher_actors = [
            BatchDispatcher.remote() for _ in range(num_batch_dispatchers)
        ]

        # Create the env runners.
        self.env_runners = [
            ray.remote(InfiniteAPPOMultiAgentEnvRunner).remote(
                weights_server_actors=self.weights_server_actors,
                sync_freq=10,
                config=self.config,
            ) for _ in range(self.config.num_env_runners)
        ]
        print(f"Created {self.config.num_env_runners} EnvRunners.")

        # Create the agg. actors.
        spaces = ray.get(self.env_runners[0].get_spaces.remote())
        rl_module_spec = self.config.get_multi_rl_module_spec(
            spaces=spaces,
            inference_only=False,
        )
        self.aggregator_actors = [
            InfiniteAPPOAggregatorActor.remote(
                config=self.config,
                rl_module_spec=rl_module_spec,
                sync_freq=10,
                batch_dispatchers=self.batch_dispatcher_actors,
                metrics_actor=self.metrics_actor,
            ) for _ in range(num_aggregator_actors)
        ]
        print(f"Created {num_aggregator_actors} AggregatorActors.")

        # Add agg. actors to env runners.
        for aid, er in enumerate(self.env_runners):
            er.add_aggregator_actors.remote(self.aggregator_actors)

        # Create the Learner actors.
        #self.learner_group = self.config.build_learner_group(
        #    env=None,
        #    spaces=spaces,
        #    rl_module_spec=rl_module_spec,
        #)
        learner_config = config.copy()
        learner_config.num_learners = 1
        self.learners = [ #list(self.learner_group._worker_manager.actors().values())
            ray.remote(
                num_gpus=self.config.num_gpus_per_learner,
                num_cpus=1 if self.config.num_cpus_per_learner == "auto" else self.config.num_cpus_per_learner,
            )(InfiniteAPPOLearner).remote(
                config=learner_config,
                module_spec=rl_module_spec,
            )
            for _ in range(self.config.num_learners)
        ]
        for i in range(self.config.num_learners):
            ray.get(self.learners[i].build.remote())
        # Let Learner w/ idx 0 know that it's responsible for pushing the weights.
        ray.get(self.learners[0].set_other_actors.remote(
            weights_server_actors=self.weights_server_actors,
            metrics_actor=self.metrics_actor,
        ))
        for learner in self.learners[1:]:
            ray.get(learner.set_other_actors.remote(metrics_actor=self.metrics_actor))
        print(f"Created {self.config.num_learners} Learners.")

        for i in range(num_batch_dispatchers):
            self.batch_dispatcher_actors[i].set_other_actors.remote(
                metrics_actor=self.metrics_actor,
                learners=self.learners,
            )
        print(f"Setup {num_batch_dispatchers} BatchDispatchers.")

        # Assign a Learner actor to each aggregator actor.
        #for aid, agg in enumerate(self.aggregator_actors):
        #    idx = aid % len(self.learners)
        #    learner = self.learners[idx]
        #    agg.add_learner.remote(learner)

        time.sleep(5.0)

        # Kick of sampling, aggregating, and training.
        for er in self.env_runners:
            er.start_infinite_sample.remote()

    def train(self) -> dict:
        t0 = time.time()

        # Ping metrics actor once per iteration.
        metrics = ray.get(self.metrics_actor.get.remote())

        # Wait until iteration is done.
        time.sleep(max(0, self.config.min_time_s_per_iteration - (time.time() - t0)))

        return metrics


@ray.remote
class WeightsServerActor:
    def __init__(self):
        self._weights_ref = None
        self._other_weights_server_actors = []

    def add_peers(self, other_weights_server_actors):
        self._other_weights_server_actors = other_weights_server_actors

    def put(self, weights_ref, broadcast=False):
        self._weights_ref = weights_ref
        # Send new weights to all peers.
        if broadcast:
            for peer in self._other_weights_server_actors:
                peer.put.remote(weights_ref, broadcast=False)

    def get(self):
        return self._weights_ref


@ray.remote
class MetricsActor:
    def __init__(self):
        self.metrics = MetricsLogger()

    def add(
        self,
        *,
        env_runner_metrics=None,
        aggregator_metrics=None,
        learner_metrics=None,
    ):
        if env_runner_metrics is not None:
            assert isinstance(env_runner_metrics, dict)
            self.metrics.merge_and_log_n_dicts(
                [env_runner_metrics],
                key="env_runners",
            )
        if aggregator_metrics is not None:
            assert isinstance(aggregator_metrics, dict)
            self.metrics.merge_and_log_n_dicts(
                [aggregator_metrics],
                key="aggregator_actors",
            )
        if learner_metrics is not None:
            assert isinstance(learner_metrics, dict)
            self.metrics.merge_and_log_n_dicts(
                [learner_metrics],
                key="learners",
            )

    def get(self):
        metrics = self.metrics.reduce()
        return metrics


class InfiniteAPPOMultiAgentEnvRunner(MultiAgentEnvRunner):
    def __init__(self, *, weights_server_actors, sync_freq, **kwargs):
        super().__init__(**kwargs)

        self.weights_server_actors = weights_server_actors
        self.sync_freq = sync_freq

        self._curr_agg_idx = 0
        self._aggregator_actor_refs = []

    def add_aggregator_actors(self, aggregator_actor_refs):
        random.shuffle(aggregator_actor_refs)
        self._aggregator_actor_refs = aggregator_actor_refs

    def start_infinite_sample(self):
        iteration = 0
        while True:
            # Pull new weights, every n times.
            if iteration % self.config.broadcast_interval == 0 and self.weights_server_actors:
                weights = ray.get(random.choice(self.weights_server_actors).get.remote())
                if weights is not None:
                    self.module.set_state(weights)

            episodes = self.sample()

            # Send data directly to an aggregator actor.
            # Pick an aggregator actor round-robin.
            if not self._aggregator_actor_refs:
                return

            agg_actor = self._aggregator_actor_refs[
                self._curr_agg_idx % len(self._aggregator_actor_refs)
            ]
            agg_actor.push_episodes.remote(
                episodes,
                env_runner_metrics=self.get_metrics(),
            )
            self._curr_agg_idx += 1

            # Sync with one aggregator actor.
            if iteration % self.sync_freq == 0 and self._aggregator_actor_refs:
                ray.get(random.choice(self._aggregator_actor_refs).sync.remote())

            iteration += 1


@ray.remote
class InfiniteAPPOAggregatorActor(AggregatorActor):
    def __init__(self, *, config, rl_module_spec, sync_freq, batch_dispatchers, metrics_actor):
        super().__init__(config=config, rl_module_spec=rl_module_spec)
        self.sync_freq = sync_freq
        self._batch_dispatchers = batch_dispatchers
        self._metrics_actor = metrics_actor
        self._num_batches_produced = 0
        self._ts = 0
        self._episodes = []

        self._env_runner_metrics = MetricsLogger()

    # Synchronization helper method.
    def sync(self):
        return None

    def push_episodes(self, episodes, env_runner_metrics):
        # Make sure we count how many timesteps we already have and only produce a
        # batch, once we have enough episode data.
        self._episodes.extend(episodes)
        self._env_runner_metrics.merge_and_log_n_dicts([env_runner_metrics])

        env_steps = sum(len(e) for e in episodes)
        self._ts += env_steps

        if self._ts >= self.config.train_batch_size_per_learner:
            # If we have enough episodes collected to create a single train batch, pass
            # them at once through the connector to receive a single train batch.
            batch = self._learner_connector(
                episodes=self._episodes,
                rl_module=self._module,
                metrics=self.metrics,
            )
            batch_env_steps = sum(len(e) for e in self._episodes)
            # Convert to a dict into a `MultiAgentBatch`.
            # TODO (sven): Try to get rid of dependency on MultiAgentBatch (once our mini-
            #  batch iterators support splitting over a dict).
            ma_batch = MultiAgentBatch(
                policy_batches={
                    pid: SampleBatch(pol_batch) for pid, pol_batch in batch.items()
                },
                env_steps=batch_env_steps,
            )

            self._ts = 0
            self._episodes = []

            self.metrics.log_value(
                "num_env_steps_aggregated_lifetime",
                batch_env_steps,
                reduce="sum",
                with_throughput=True,
            )

            # Forward results to a Learner actor.
            batch_dispatch_actor = random.choice(self._batch_dispatchers)
            batch_dispatch_actor.add_batch.remote(ma_batch)

            self._num_batches_produced += 1

            if self._num_batches_produced % 10 == 0:
                self._metrics_actor.add.remote(
                    env_runner_metrics=self._env_runner_metrics.reduce(),
                    aggregator_metrics=self.metrics.reduce(),
                )

            # Sync with one of the dispatcher actors.
            if self._num_batches_produced % self.sync_freq == 0:
                ray.get(batch_dispatch_actor.sync.remote())


@ray.remote
class BatchDispatcher:
    def __init__(self):
        self._learners = []
        self._batches = []

    def sync(self):
        return None

    def set_other_actors(self, *, metrics_actor, learners):
        self._metrics_actor = metrics_actor
        self._learners = learners

    def add_batch(self, batch):
        self._batches.append(batch)

        while len(self._batches) >= len(self._learners):
            for learner in self._learners:
                learner.update.remote(self._batches.pop(0))


class InfiniteAPPOLearner(APPOTorchLearner):
    def __init__(self, *, config, module_spec):
        super().__init__(config=config, module_spec=module_spec)
        self._num_updates = 0

    # Synchronization helper method.
    def sync(self):
        return None

    def set_other_actors(self, *, metrics_actor, weights_server_actors=None):
        self._metrics_actor = metrics_actor
        self._weights_server_actors = weights_server_actors

    def update(self, batch):#, timesteps):
        global _CURRENT_GLOBAL_TIMESTEPS
        if _CURRENT_GLOBAL_TIMESTEPS is None:
            _CURRENT_GLOBAL_TIMESTEPS = 0
        #_CURRENT_GLOBAL_TIMESTEPS += timesteps

        # Enqueue the batch, either directly into the learner thread's queue or to the
        # GPU loader threads.
        if self.config.num_gpus_per_learner > 0:
            self._gpu_loader_in_queue.put(batch)
            self.metrics.log_value(
                (ALL_MODULES, QUEUE_SIZE_GPU_LOADER_QUEUE),
                self._gpu_loader_in_queue.qsize(),
            )
        else:
            ts_dropped = self._learner_thread_in_queue.add(batch)
            self.metrics.log_value(
                (ALL_MODULES, LEARNER_THREAD_ENV_STEPS_DROPPED),
                ts_dropped,
                reduce="sum",
            )

        # Figure out, whether we need to send our weights to a weights server.
        if self._weights_server_actors:
            learner_state = self.get_state(
                # Only return the state of those RLModules that are trainable.
                components=[
                    COMPONENT_RL_MODULE + "/" + mid
                    for mid in self.module.keys()
                    if self.should_module_be_updated(mid)
                ],
                inference_only=True,
            )
            learner_state[COMPONENT_RL_MODULE] = ray.put(
                learner_state[COMPONENT_RL_MODULE]
            )
            random.choice(self._weights_server_actors).put.remote(
                learner_state, broadcast=True
            )

        # Send metrics to metrics actor.
        #print("SENDING learner metrics to metrics actor")
        self._metrics_actor.add.remote(
            learner_metrics=self.metrics.reduce(),
        )

        self._num_updates += 1


if __name__ == "__main__":
    NUM_ENV_RUNNERS = 1024
    NUM_ENVS_PER_ENV_RUNNER = 5
    NUM_AGG_ACTORS = 64
    NUM_LEARNERS = 16
    NUM_WEIGHTS_SERVER_ACTORS = 8
    NUM_BATCH_DISPATCHERS = 32
    NUM_GPUS_PER_LEARNER = 1

    def _make_env_to_module_connector(env):
        return FrameStackingEnvToModule(num_frames=4, multi_agent=True)


    def _make_learner_connector(input_observation_space, input_action_space):
        return FrameStackingLearner(num_frames=4, multi_agent=True)


    def _env_creator(cfg):
        return wrap_atari_for_new_api_stack(
            gym.make("ale_py:ALE/Pong-v5", **cfg, **{"render_mode": "rgb_array"}),
            dim=64,
            framestack=None,
        )


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
            #num_aggregator_actors_per_learner=NUM_AGG_ACTORS_PER_LEARNER,
        )
        .training(
            learner_class=InfiniteAPPOLearner,
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

    algo = Algo(
        config=config,
        observation_space=gym.spaces.Box(-1.0, 1.0, (64, 64, 4), np.float32),
        action_space=gym.spaces.Discrete(6),
        num_weights_server_actors=NUM_WEIGHTS_SERVER_ACTORS,
        num_batch_dispatchers=NUM_BATCH_DISPATCHERS,
        num_aggregator_actors=NUM_AGG_ACTORS,
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
