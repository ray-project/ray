import copy
import queue
import random
import time
import threading

import gymnasium as gym
import numpy as np

import ray
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.algorithms.appo import APPOConfig
from ray.rllib.algorithms.appo.utils import CircularBuffer
from ray.rllib.algorithms.impala.impala_learner import _GPULoaderThread
from ray.rllib.algorithms.utils import AggregatorActor
from ray.rllib.connectors.env_to_module.frame_stacking import FrameStackingEnvToModule
from ray.rllib.connectors.learner.frame_stacking import FrameStackingLearner
from ray.rllib.core.rl_module.default_model_config import DefaultModelConfig
from ray.rllib.core.rl_module.multi_rl_module import MultiRLModuleSpec
from ray.rllib.core.rl_module.rl_module import RLModuleSpec
from ray.rllib.env.multi_agent_env import make_multi_agent
from ray.rllib.env.multi_agent_env_runner import MultiAgentEnvRunner
from ray.rllib.env.wrappers.atari_wrappers import wrap_atari_for_new_api_stack
from ray.rllib.examples.rl_modules.classes.random_rlm import RandomRLModule
from ray.rllib.policy.sample_batch import MultiAgentBatch, SampleBatch
from ray.rllib.utils.framework import get_device
from ray.rllib.utils.metrics.metrics_logger import MetricsLogger
from ray.rllib.utils.test_utils import add_rllib_example_script_args


class Algo:
    def __init__(
        self,
        *,
        config,
        observation_space,
        action_space,
        num_weights_server_actors=1,
    ):
        self.observation_space = observation_space
        self.action_space = action_space
        self.config = config
        self.metrics = MetricsLogger()

        # Create 1 weights server actor.
        self.weights_server_actors = [
            WeightsServerActor.remote() for _ in range(num_weights_server_actors)
        ]
        for aid, actor in enumerate(self.weights_server_actors):
            actor.add_peers.remote(
                self.weights_server_actors[:aid] + self.weights_server_actors[aid + 1:])
        self.metrics_actor = MetricsActor.remote()

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
        rl_module_spec = self.config.get_multi_rl_module_spec(
            spaces=ray.get(self.env_runners[0].get_spaces.remote()),
            inference_only=False,
        )
        self.aggregator_actors = [
            InfiniteAPPOAggregatorActor.remote(
                config=self.config,
                rl_module_spec=rl_module_spec,
                sync_freq=10,
            ) for _ in range(self.config.num_aggregator_actors_per_learner * self.config.num_learners)
        ]
        print(f"Created {self.config.num_aggregator_actors_per_learner * self.config.num_learners} AggregatorActors.")

        # Add agg. actors to env runners.
        for aid, er in enumerate(self.env_runners):
            er.add_aggregator_actors.remote(self.aggregator_actors)

        # Create the Learner actors.
        self.learners = [
            ray.remote(num_cpus=1, num_gpus=self.config.num_gpus_per_learner)(Learner).remote(
                weights_server_actors=self.weights_server_actors,
                metrics_actor=self.metrics_actor,
            ) for _ in range(self.config.num_learners)
        ]
        # Let Learner w/ idx 0 know that it's responsible for pushing the weights.
        self.learners[0].set_push_weights.remote(True)
        print(f"Created {self.config.num_learners} Learners.")

        # Assign a Learner actor to each aggregator actor.
        for aid, agg in enumerate(self.aggregator_actors):
            idx = aid % len(self.learners)
            learner = self.learners[idx]
            agg.add_learner.remote(learner)

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

    def add(self, env_runner_metrics, aggregator_metrics, learner_metrics):
        assert isinstance(env_runner_metrics, list)
        self.metrics.merge_and_log_n_dicts(
            env_runner_metrics,
            key="env_runners",
        )
        self.metrics.merge_and_log_n_dicts(
            [aggregator_metrics],
            key="aggregator_actors",
        )
        self.metrics.merge_and_log_n_dicts(
            [learner_metrics],
            key="learners",
        )

    def get(self):
        return self.metrics.reduce()


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
                time.sleep(0.02)
                #self.module.set_state({COMPONENT_RL_MODULE: weights})

            episodes = self.sample()
            # Send data directly to an aggregator actor.
            # Pick an aggregator actor round-robin.
            if not self._aggregator_actor_refs:
                return

            agg_actor = self._aggregator_actor_refs[
                self._curr_agg_idx % len(self._aggregator_actor_refs)
            ]
            metrics = self.get_metrics()
            agg_actor.push_episodes.remote(
                episodes,
                env_runner_metrics=metrics,
            )
            self._curr_agg_idx += 1

            # Sync with one aggregator actor.
            if iteration % self.sync_freq == 0 and self._aggregator_actor_refs:
                ray.get(random.choice(self._aggregator_actor_refs).sync.remote())

            iteration += 1


@ray.remote
class InfiniteAPPOAggregatorActor(AggregatorActor):
    def __init__(self, *, config, rl_module_spec, sync_freq):
        super().__init__(config=config, rl_module_spec=rl_module_spec)
        self.sync_freq = sync_freq
        self._learner_ref = None
        self._num_batches_produced = 0
        self._ts = 0
        self._episodes = []
        self._env_runner_metrics = []

    # Synchronization helper method.
    def sync(self):
        return None

    def add_learner(self, learner_ref):
        self._learner_ref = learner_ref

    def push_episodes(self, episodes, env_runner_metrics):
        if not self._learner_ref:
            return

        # Make sure we count how many timesteps we already have and only produce a
        # batch, once we have enough episode data.
        self._episodes.extend(episodes)
        self._env_runner_metrics.append(env_runner_metrics)
        env_steps = sum(len(e) for e in episodes)
        self._ts += env_steps

        if self._ts >= self.config.train_batch_size_per_learner:
            # If we have enough episodes collected to create a single train batch, pass
            # them at once through the connector to recieve a single train batch.
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
            self._learner_ref.update.remote(
                ma_batch,
                aggregator_metrics=self.metrics.reduce(),
                env_runner_metrics=self._env_runner_metrics,
            )

            self._env_runner_metrics = []
            self._num_batches_produced += 1

            # Sync with the Learner actor.
            if self._num_batches_produced % self.sync_freq == 0:
                ray.get(self._learner_ref.sync.remote())


class Learner:
    def __init__(
        self,
        *,
        process_time_per_update=0.05,
        num_gpu_loader_threads=8,
        weights_server_actors,
        metrics_actor=None,
        num_gpus_per_learner=0,
    ):
        self._device = get_device(AlgorithmConfig(), num_gpus_per_learner)

        self.weights_server_actors = weights_server_actors
        self.metrics_actor = metrics_actor
        self.process_time_per_update = process_time_per_update

        self.metrics = MetricsLogger()
        self.metrics._threading_lock = threading.RLock()

        # Mimic a circular buffer.
        self._learner_thread_in_queue = (
            CircularBuffer(num_batches=4, iterations_per_batch=2)
        )

        # Mimic GPU loader threads.
        self._gpu_loader_in_queue = queue.Queue()
        self._gpu_loader_threads = [
            _GPULoaderThread(
                in_queue=self._gpu_loader_in_queue,
                out_queue=self._learner_thread_in_queue,
                device=self._device,
                metrics_logger=self.metrics,
            )
            for _ in range(num_gpu_loader_threads)
        ]
        for t in self._gpu_loader_threads:
            t.start()

        self._learner_thread = _LearnerThread(
            process_time_per_update=self.process_time_per_update,
            in_queue=self._learner_thread_in_queue,
            metrics=self.metrics,
        )
        self._learner_thread.start()

        self._push_weights = False

        self._num_updates = 0

    # Synchronization helper method.
    def sync(self):
        return None

    def set_push_weights(self, push_weights):
        self._push_weights = push_weights

    def update(self, batch, aggregator_metrics, env_runner_metrics):
        #self.metrics.merge_and_log_n_dicts([aggregator_metrics])

        self._gpu_loader_in_queue.put(batch)

        # Figure out, whether we need to send our weights to a weights server.
        if self._push_weights and self.weights_server_actors:
            # Time it takes to pull weights down from GPU to the CPU.
            time.sleep(0.01)
            weights = create_data(size_kb=10000, n_components=30)
            weights_ref = ray.put(weights)
            random.choice(self.weights_server_actors).put.remote(weights_ref, broadcast=True)

        # Send metrics to metrics actor.
        self._num_updates += 1
        if self._num_updates % 10 == 0:
            self.metrics_actor.add.remote(
                env_runner_metrics=env_runner_metrics,
                aggregator_metrics=aggregator_metrics,
                learner_metrics=self.metrics.reduce(),
            )


class _LearnerThread(threading.Thread):
    def __init__(
            self,
            *,
            in_queue,
            process_time_per_update,
            metrics,
    ):
        super().__init__()
        self.process_time_per_update = process_time_per_update
        self.metrics = metrics
        self._in_queue = in_queue

    def run(self) -> None:
        while True:
            self.step()

    def step(self):
        # Get a new batch from the GPU-data (learner queue OR circular buffer).
        ma_batch_on_gpu = self._in_queue.sample()
        time.sleep(self.process_time_per_update)
        self.metrics.log_value(
            ("learners", "num_env_steps_trained_lifetime"),
            ma_batch_on_gpu.env_steps(),
            reduce="sum",
            with_throughput=True,
        )


def create_data(size_kb, n_components=1, dtype=np.float32):
    bytes_per_element = np.dtype(dtype).itemsize
    total_bytes = size_kb * 1024

    # Divide bytes equally among components
    bytes_per_component = total_bytes / n_components

    if bytes_per_component < bytes_per_element:
        raise ValueError(
            "Target size too small for the given number of components and dtype.")

    elements_per_component = bytes_per_component / bytes_per_element

    data = {}
    for i in range(n_components):
        size = int(elements_per_component)
        data[f"component_{i}"] = np.zeros(size, dtype=dtype)

    return data


if __name__ == "__main__":
    NUM_LEARNERS = 1

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
    NUM_AGENTS = 2
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
            num_env_runners=2,
            rollout_fragment_length=50,
            num_envs_per_env_runner=1,
        )
        .learners(
            num_learners=NUM_LEARNERS,
            num_aggregator_actors_per_learner=2,
        )
        .training(
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
                    | {"random": RLModuleSpec(module_class=RandomRLModule)}
                ),
            ),
        )
        .multi_agent(
            policies={f"p{i}" for i in range(NUM_POLICIES)} | {"random"},
            policy_mapping_fn=lambda aid, eps, **kw: (
                random.choice([f"p{i}" for i in range(NUM_POLICIES)] + ["random"])
            ),
            policies_to_train=[f"p{i}" for i in range(NUM_POLICIES)],
        )
    )

    algo = Algo(
        config=config,
        observation_space=gym.spaces.Box(-1.0, 1.0, (64, 64, 4), np.float32),
        action_space=gym.spaces.Discrete(6),
        num_weights_server_actors=1,
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
            if "num_env_steps_trained_lifetime" in learner_results:
                env_steps_trained = learner_results["num_env_steps_trained_lifetime"]
                msg += (
                    f"trained={env_steps_trained.peek()} "
                    f"({env_steps_trained.peek(throughput=True):.0f}/sec) "
                )
        print(msg)
        if iteration == 50:
            control = ray.get(algo.aggregator_actors[
                                  0].control_num_env_steps.remote()) * algo.num_aggregator_actors
            print(f"CONTROL: num_env_steps_aggregated_lifetime={control}")
