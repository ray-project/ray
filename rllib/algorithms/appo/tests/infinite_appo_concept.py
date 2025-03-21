import queue
import random
import time
import threading

import gymnasium as gym
import msgpack
import numpy as np
import tree  # pip install dm_tree

import ray
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.algorithms.appo.utils import CircularBuffer
from ray.rllib.algorithms.impala.impala_learner import _GPULoaderThread
from ray.rllib.policy.sample_batch import MultiAgentBatch, SampleBatch
from ray.rllib.utils.framework import get_device
from ray.rllib.utils.metrics.metrics_logger import MetricsLogger, Stats


# from ray.util.anyscale.zmq_channel import RouterChannel


class Algo:
    def __init__(
            self,
            *,
            observation_space,
            num_env_runners=2,
            num_envs_per_env_runner=1,
            rollout_fragment_length=50,
            num_aggregator_actors=2,
            train_batch_size_per_learner=500,
            num_learners=1,
            zmq_asyncio=False,
            min_time_s_per_iteration=1,
            env_step_time=0.015,
            num_gpus_per_learner=0,
            num_weights_server_actors=1,
    ):
        self.observation_space = observation_space
        self.num_env_runners = num_env_runners
        self.num_aggregator_actors = num_aggregator_actors
        self.train_batch_size_per_learner = train_batch_size_per_learner
        self.num_learners = num_learners
        self.zmq_asyncio = zmq_asyncio
        self.min_time_s_per_iteration = min_time_s_per_iteration

        self.metrics = MetricsLogger()

        # Create 1 weights server actor.
        self.weights_server_actors = [
            WeightsServerActor.remote() for _ in range(num_weights_server_actors)
        ]
        for aid, actor in enumerate(self.weights_server_actors):
            actor.add_peers.remote(
                self.weights_server_actors[:aid] + self.weights_server_actors[aid + 1:])
        self.metrics_actor = MetricsActor.remote()

        # self.router_channel = RouterChannel(
        #    _asyncio=self.zmq_asyncio,
        #    max_num_actors=1_000,
        #    max_outbound_messages=100_000,
        #    max_inbound_messages=100_000,
        # )

        # Create the env runners.
        self.env_runners = [
            ray.remote(EnvRunner).remote(
                observation_space=self.observation_space,
                num_envs_per_env_runner=num_envs_per_env_runner,
                rollout_fragment_length=rollout_fragment_length,
                env_step_time=env_step_time,
                weights_server_actors=self.weights_server_actors,
            ) for _ in range(self.num_env_runners)
        ]
        print(f"Created {num_env_runners} EnvRunners.")

        # Create the agg. actors.
        self.aggregator_actors = [
            AggregatorActor.remote(
                observation_space=self.observation_space,
                batch_size=self.train_batch_size_per_learner,
            ) for _ in range(self.num_aggregator_actors)
        ]
        print(f"Created {num_aggregator_actors} AggregatorActors.")
        # Create 5 aggregator dealer channels.
        # TODO (sven): Figure out current limitation on router/dealer channels.
        # self._num_aggregator_actors_w_dealer = min(num_aggregator_actors_w_dealer, num_aggregator_actors)
        # _agg_dealer_channels = {
        #    f"AGG{aid}": self.router_channel.create_dealer(
        #        actor=actor,
        #        _asyncio=self.zmq_asyncio,
        #    )
        #    for aid, actor in enumerate(self.aggregator_actors[:num_aggregator_actors_w_dealer])
        # }
        # Add dealer channels to agg. actors.
        # for aid, agg in enumerate(self.aggregator_actors[:num_aggregator_actors_w_dealer]):
        #    agg.start_zmq.remote(dealer_channel=_agg_dealer_channels[f"AGG{aid}"])

        # Add dealer channels to env runners.
        # self._num_env_runners_w_dealer = min(num_env_runners_w_dealer, num_env_runners)
        # _er_dealer_channels = {
        #    f"ER{aid}": self.router_channel.create_dealer(
        #        actor=actor,
        #        _asyncio=self.zmq_asyncio,
        #    )
        #    for aid, actor in enumerate(self.env_runners[:num_env_runners_w_dealer])
        # }
        # Kick off env runners infinite sampling loops.
        for aid, er in enumerate(self.env_runners):
            # if aid < num_env_runners_w_dealer:
            #    er.start_zmq.remote(dealer_channel=_er_dealer_channels[f"ER{aid}"])
            er.add_aggregator_actors.remote(self.aggregator_actors)

        # Create the Learner actors.
        self.learners = [
            ray.remote(num_cpus=1, num_gpus=num_gpus_per_learner)(Learner).remote(
                weights_server_actors=self.weights_server_actors,
                metrics_actor=self.metrics_actor,
            ) for _ in range(self.num_learners)
        ]
        print(f"Created {num_learners} Learners.")
        # self._num_learners_w_dealer = min(num_learners_w_dealer, num_learners)
        # _learner_dealer_channels = {
        #    f"LEARN{aid}": self.router_channel.create_dealer(
        #        actor=actor,
        #        _asyncio=self.zmq_asyncio,
        #    )
        #    for aid, actor in enumerate(self.learners[:num_learners_w_dealer])
        # }
        # Add dealer channels to Learners.
        # for aid, learner in enumerate(self.learners[:num_learners_w_dealer]):
        #    learner.start_zmq.remote(
        #        dealer_channel=_learner_dealer_channels[f"LEARN{aid}"],
        #    )
        # Let Learner w/ idx 0 know that it's responsible for pushing the weights.
        self.learners[0].set_push_weights.remote(True)

        # Assign a Learner actor to each aggregator actor.
        for aid, agg in enumerate(self.aggregator_actors):
            idx = aid % len(self.learners)
            learner = self.learners[idx]
            agg.add_learner.remote(learner)

        time.sleep(5.0)
        # Kick of sampling, aggregating, and training.
        for er in self.env_runners:
            er.sample.remote()

    def train(self) -> dict:
        t0 = time.time()

        # Ping metrics actor once per iteration.
        metrics = ray.get(self.metrics_actor.get.remote())

        # Wait until iteration is done.
        time.sleep(max(0, self.min_time_s_per_iteration - (time.time() - t0)))

        return metrics

        # While the iteration is ongoing ...
        # while time.time() - t0 < self.min_time_s_per_iteration:
        # Ping all env runners w/ dealer channel and query their metrics.
        # env_runner_metrics = []
        # for er in self.env_runners[:self._num_env_runners_w_dealer]:
        #    self.router_channel.write(
        #        actor=er,
        #        message="get_metrics",
        #    )
        #    time.sleep(0.01)
        #    env_runner_metrics.append(self.router_channel.read()[0])
        # metrics = MetricsLogger()
        # metrics.merge_and_log_n_dicts(
        #    tree.map_structure(
        #        lambda s: Stats.from_state(msgpack.unpackb(s)),
        #        env_runner_metrics,
        #    ),
        # )
        ## Extrapolate metrics and override existing "env_runner" metrics.
        # tree.map_structure(
        #    lambda s: (
        #        _extrapolate_stats(
        #            s, self.num_env_runners, self._num_env_runners_w_dealer
        #        )
        #    ),
        #    metrics.stats,
        # )
        # self.metrics.stats["env_runners"] = metrics.stats
        #
        ## Ping one env runner and query its metrics.
        # agg_actor = self.aggregator_actors[self._aggregator_actor_idx % len(self.aggregator_actors)]
        # self.router_channel.write(
        #    actor=agg_actor,
        #    message="get_metrics",
        # )
        # time.sleep(0.01)
        # response = self.router_channel.read()
        # self.metrics.merge_and_log_n_dicts(
        #    [tree.map_structure(
        #        lambda s: Stats.from_state(msgpack.unpackb(s)),
        #        response[0],
        #    )],
        #    key="aggregator_actors",
        # )
        #
        ## Ping one Learner and query its metrics.
        # learner = self.learners[self._learner_idx % len(self.learners)]
        # self.router_channel.write(
        #    actor=learner,
        #    message="get_metrics",
        # )
        # time.sleep(0.01)
        # response = self.router_channel.read()
        # self.metrics.merge_and_log_n_dicts(
        #    [tree.map_structure(
        #        lambda s: Stats.from_state(msgpack.unpackb(s)),
        #        response[0],
        #    )],
        #    key="learners",
        # )

        # return self.metrics.reduce()


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
        for peer in self._other_weights_server_actors:
            peer.put.remote(weights_ref, broadcast=broadcast)

    def get(self):
        return self._weights_ref


@ray.remote
class MetricsActor:
    def __init__(self):
        self.metrics = MetricsLogger()

    def add(self, metrics):
        self.metrics.merge_and_log_n_dicts([metrics])

    def get(self):
        return self.metrics.reduce()


class EnvRunner:
    def __init__(
            self,
            *,
            observation_space,
            num_envs_per_env_runner=1,
            rollout_fragment_length=50,
            env_step_time=0.015,
            weights_server_actors,
            sync_freq=10,
    ):
        self.observation_space = observation_space
        self.num_envs_per_env_runner = num_envs_per_env_runner
        self.rollout_fragment_length = rollout_fragment_length
        self.env_step_time = env_step_time
        self.weights_server_actors = weights_server_actors
        self.sync_freq = sync_freq

        # Assuming synchronous/sequential env stepping.
        self._sample_time = (
                self.env_step_time
                * self.num_envs_per_env_runner
                * self.rollout_fragment_length
        )

        self.metrics = MetricsLogger()
        # self.metrics._threading_lock = threading.RLock()

        self._size_per_sample_kb = (
                np.prod(self.observation_space.shape)
                * self.observation_space.dtype.itemsize
                * self.num_envs_per_env_runner
                * self.rollout_fragment_length
        )
        # 0.1 for "all the reset" (actions, rewards, terminateds, etc..)
        self._size_per_sample_kb += 0.1 * self._size_per_sample_kb
        self._size_per_sample_kb = int(self._size_per_sample_kb / 1024)

        self._curr_agg_idx = 0
        self._aggregator_actor_refs = []

        self._control_num_env_steps = 0

    # def start_zmq(self, dealer_channel):
    #    self._dealer_channel = dealer_channel
    #    # Start a dealer channel thread.
    #    DealerChannelThread(self._dealer_channel, self.metrics).start()

    def add_aggregator_actors(self, aggregator_actor_refs):
        random.shuffle(aggregator_actor_refs)
        self._aggregator_actor_refs = aggregator_actor_refs

    def sample(self):
        iteration = 0
        while True:
            self._sample(iteration=iteration)
            iteration += 1
            # Sync with one aggregator actor.
            if iteration % self.sync_freq == 0 and self._aggregator_actor_refs:
                ray.get(random.choice(self._aggregator_actor_refs).sync.remote())

    def _sample(self, iteration):
        # Pull new weights, every n times.
        if iteration % 5 == 0 and self.weights_server_actors:
            weights = ray.get(random.choice(self.weights_server_actors).get.remote())

            # Time to apply weights to our model.
            time.sleep(0.01)

        time.sleep(self._sample_time)
        data = create_data(size_kb=self._size_per_sample_kb, n_components=5)

        env_steps = self.rollout_fragment_length * self.num_envs_per_env_runner
        self.metrics.log_value(
            ("env_runners", "num_env_steps_sampled_lifetime"),
            env_steps,
            reduce="sum",
            with_throughput=True,
        )

        # Send data directly to an aggregator actor.
        # Pick an aggregator actor round-robin.
        if not self._aggregator_actor_refs:
            return

        agg_actor = self._aggregator_actor_refs[
            self._curr_agg_idx % len(self._aggregator_actor_refs)]
        agg_actor.produce_batch.remote(
            data,
            env_steps=env_steps,
            env_runner_metrics=self.metrics.reduce(),
        )
        self._curr_agg_idx += 1

        return env_steps


@ray.remote
class AggregatorActor:
    def __init__(
            self,
            observation_space,
            batch_size=500,
            process_time_per_env_step=0.005,
            sync_freq=10,
    ):
        self.observation_space = observation_space
        self.batch_size = batch_size
        self.process_time_per_env_step = process_time_per_env_step
        self.sync_freq = sync_freq

        self.metrics = MetricsLogger()
        self.metrics._threading_lock = threading.RLock()

        self._size_per_batch_kb = (
                np.prod(observation_space.shape)
                * self.batch_size
        )
        # 0.5 for "all the reset" (actions, rewards, terminateds, etc..)
        self._size_per_batch_kb += 0.5 * self._size_per_batch_kb
        self._size_per_batch_kb = int(self._size_per_batch_kb / 1024)

        self._learner_ref = None

        self._control_num_env_steps = 0
        self._iteration = 0

    # Synchronization helper method.
    def sync(self):
        return None

    # def start_zmq(self, dealer_channel):
    #    self._dealer_channel = dealer_channel
    #    # Start the dealer channel thread.
    #    DealerChannelThread(self._dealer_channel, self.metrics).start()

    def add_learner(self, learner_ref):
        self._learner_ref = learner_ref

    def produce_batch(self, data, env_steps: int, env_runner_metrics):
        time.sleep(self.process_time_per_env_step * env_steps)

        self.metrics.merge_and_log_n_dicts([env_runner_metrics])

        if not self._learner_ref:
            return

        self.metrics.log_value(
            ("aggregator_actors", "num_env_steps_aggregated_lifetime"),
            env_steps,
            reduce="sum",
            with_throughput=True,
        )

        # Forward results to a Learner actor.
        self._learner_ref.update.remote(
            data,
            env_steps=env_steps,
            aggregator_metrics=self.metrics.reduce(),
        )

        self._control_num_env_steps += env_steps
        self._iteration += 1

        # Sync with the Learner actor.
        if self._iteration % self.sync_freq == 0 and self._learner_ref:
            ray.get(self._learner_ref.sync.remote())

        return env_steps

    def control_num_env_steps(self):
        return self._control_num_env_steps


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

    # Synchronization helper method.
    def sync(self):
        return None

    # def start_zmq(self, dealer_channel):
    #    self._dealer_channel = dealer_channel
    #    # Start the dealer channel thread.
    #    DealerChannelThread(self._dealer_channel, self.metrics).start()

    def set_push_weights(self, push_weights):
        self._push_weights = push_weights

    def update(self, batch, env_steps: int, aggregator_metrics):
        self.metrics.merge_and_log_n_dicts([aggregator_metrics])

        ma_batch = MultiAgentBatch({"default_policy": SampleBatch(batch)},
                                   env_steps=env_steps)
        self._gpu_loader_in_queue.put(ma_batch)

        # Figure out, whether we need to send our weights to a weights server.
        if self._push_weights and self.weights_server_actors:
            # Time it takes to pull weights down from GPU to the CPU.
            time.sleep(0.01)
            weights = create_data(size_kb=10000, n_components=30)
            weights_ref = ray.put(weights)
            random.choice(self.weights_server_actors).put.remote(weights_ref, broadcast=True)

        # Send metrics to metrics actor.
        self.metrics_actor.add.remote(self.metrics.reduce())


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
        # self.daemon = True
        # self.stopped = False
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


# class DealerChannelThread(threading.Thread):
#    def __init__(self, dealer_channel, metrics):
#        super().__init__()
#        self.dealer_channel = dealer_channel
#        self.metrics = metrics
#
#    def run(self):
#        while True:
#            # Receive the message from the RouterChannel.
#            message = self.dealer_channel.read()
#            if message == "get_metrics":
#                response = tree.map_structure(
#                    lambda s: msgpack.packb(s.get_state()),
#                    self.metrics.reduce(),
#                )
#            # TODO: just mirror message for now ...
#            else:
#                response = message
#
#            self.dealer_channel.write(response)


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
    algo = Algo(
        observation_space=gym.spaces.Box(-1.0, 1.0, (64, 64, 4), np.float32),
        num_env_runners=8192,
        num_envs_per_env_runner=1,
        rollout_fragment_length=50,
        num_aggregator_actors=512,
        train_batch_size_per_learner=500,
        num_learners=64,
        num_weights_server_actors=2,
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
