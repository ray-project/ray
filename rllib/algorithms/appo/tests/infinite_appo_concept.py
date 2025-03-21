import ray
import zmq
from typing import Any, Awaitable, Tuple, Optional, Callable
from ray.util.annotations import PublicAPI
import zmq.asyncio
import msgpack
import logging

logger = logging.getLogger(__name__)


def asyncio_check(expected_asyncio: bool):
    def decorator(func):
        def wrapper(self, *args, **kwargs):
            if self.asyncio is not expected_asyncio:
                async_or_sync = "async" if expected_asyncio is True else "sync"
                raise Exception(
                    f"Must be initialized with asyncio={expected_asyncio} "
                    f"to use {async_or_sync} functions"
                )
            result = func(self, *args, **kwargs)
            return result

        return wrapper

    return decorator


def socket_initialized_check(func):
    def wrapper(self, *args, **kwargs):
        if self._socket_initialized is False:
            raise Exception("Socket must be initialized before use")
        result = func(self, *args, **kwargs)
        return result

    return wrapper


@PublicAPI(stability="alpha")
class RouterChannel:
    """
    A class used to setup the Router / Dealer ZMQ Communication pattern for Ray actors.
    Example:

    @ray.remote
    class AsyncActor:
        async def run(self, dealer_channel):
            next_message = 0
            while True:
                await dealer_channel.async_write(next_message)
                next_message = await dealer_channel.async_read()

    actors = [AsyncActor.remote() for _ in range(5)]
    router_channel = RouterChannel(_asyncio=True)
    refs = []
    for actor in actors:
        dealer_channel = router_channel.create_dealer(actor, _asyncio=True)
        refs.append(actor.run.remote(dealer_channel))

    last_message = {actor: -1 for actor in actors}

    start_time = time.time()
    num_iterations = 0
    while num_iterations < 50:
        message, actor = await router_channel.async_read()
        assert message > last_message[actor]
        last_message[actor] = message
        await router_channel.async_write(actor, message + 1)
        num_iterations += 1
    """

    def __init__(
            self,
            _asyncio: bool = False,
            max_num_actors: int = 100,
            max_outbound_messages: int = 1000,
            max_inbound_messages: int = 1000,
    ):
        """
        Initializes a RouterChannel object.

        Args:
            _asyncio: Whether to use asyncio for the RouterChannel.
            max_num_actors: The maximum number of actors you want to connect
                Note: This is actually the max number of pending connections
                so you can likely have more actors than this number.
            max_outbound_messages: The max number of outbound messages
            max_inbound_messages: The max number of inbound messages
                Note: Router will drop excess messages if either limit is reached.
        """
        self.asyncio = _asyncio
        self._context = zmq.asyncio.Context() if _asyncio else zmq.Context()

        def setup_socket(socket):
            self._socket.setsockopt(zmq.BACKLOG, max_num_actors)
            self._socket.setsockopt(zmq.SNDHWM, max_outbound_messages)
            self._socket.setsockopt(zmq.RCVHWM, max_inbound_messages)

        self._socket = self._context.socket(zmq.ROUTER)
        setup_socket(self._socket)
        self._node_id = ray.get_runtime_context().get_node_id()
        ip_address = ray.util.get_node_ip_address()
        port = self._socket.bind_to_random_port(f"tcp://{ip_address}")
        self._address = f"tcp://{ip_address}:{port}"

        self._local_socket = self._context.socket(zmq.ROUTER)
        setup_socket(self._local_socket)
        # TODO: Account for multiple routers on the same node
        # and more ray sessions started after
        self._local_address = "ipc:///tmp/ray/session_latest/sockets/zmq_ipc"
        self._local_socket.bind(self._local_address)

        self._poller = zmq.asyncio.Poller() if _asyncio else zmq.Poller()
        self._poller.register(self._socket, zmq.POLLIN)
        self._poller.register(self._local_socket, zmq.POLLIN)

        self._remote_actors_to_client_ids = {}
        self._local_actors_to_client_ids = {}
        self._client_ids_to_actors = {}

    def create_dealer(
            self,
            actor: "ray.actor.ActorHandle",
            on_actor_failure: Optional[Callable[[None], None]] = None,
            _asyncio: bool = False,
            max_outbound_messages: int = 1000,
            max_inbound_messages: int = 1000,
    ) -> "DealerChannel":
        """
        Gives user a DealerChannel to communicate with the Router.
        The DealerChannel should be passed to the actor where it can be used.

        Args:
            asyncio: Whether to use asyncio for the DealerChannel.
            max_outbound_messages: The max number of outbound messages
            max_inbound_messages: The max number of inbound messages
                Note: Dealer will block until we get below max
                before sending/receiving more messages.
        Returns:
            A DealerChannel object that can be used to communicate with the Router.
        """
        client_id = actor._actor_id.binary()
        dealer_channel = DealerChannel(
            self._address,
            self._local_address,
            self._node_id,
            client_id,
            _asyncio,
            max_outbound_messages,
            max_inbound_messages,
        )

        def get_is_local(self, node_id):
            return ray.get_runtime_context().get_node_id() == node_id

        ref = actor.__ray_call__.remote(get_is_local, self._node_id)
        is_local = ray.get(ref)

        if is_local:
            self._local_actors_to_client_ids[actor] = client_id
        else:
            self._remote_actors_to_client_ids[actor] = client_id

        self._client_ids_to_actors[client_id] = actor

        return dealer_channel

    @asyncio_check(expected_asyncio=False)
    def read(self) -> Tuple[Any, "ray.actor.ActorHandle"]:
        """
        Blocking call that waits for / reads the next available message
        and returns the message and the actor that sent it.

        Returns:
            A tuple with the result of the read and the actor that sent the message.
        """
        poll_results = self._poller.poll()
        socket, _ = poll_results[0]

        message = socket.recv_multipart()
        client_id = message[0]
        data = msgpack.loads(message[1])

        return data, self._client_ids_to_actors[client_id]

    @asyncio_check(expected_asyncio=True)
    async def async_read(self) -> Awaitable[Tuple[Any, "ray.actor.ActorHandle"]]:
        """
        Asynchronous version of read. Waits for and reads the next available message
        and returns the message and the actor that sent it.

        Returns:
            A tuple with the result of the read and the actor that sent the message.
        """
        poll_results = await self._poller.poll()
        socket, _ = poll_results[0]

        message = await socket.recv_multipart()
        client_id = message[0]
        data = msgpack.loads(message[1])

        return data, self._client_ids_to_actors[client_id]

    @asyncio_check(expected_asyncio=False)
    def write(self, actor: "ray.actor.ActorHandle", message: Any):
        """
        Sends a message to the specified actor.

        Args:
            actor: The actor to send the message to.
            message: The message to send to the actor.
        """
        client_id = self._local_actors_to_client_ids.get(actor)
        if client_id is not None:
            self._local_socket.send_multipart([client_id, msgpack.dumps(message)])
            return

        client_id = self._remote_actors_to_client_ids.get(actor)
        if client_id is not None:
            self._socket.send_multipart([client_id, msgpack.dumps(message)])
            return

        raise Exception("Actor was never registered with `create_dealer`")

    @asyncio_check(expected_asyncio=True)
    async def async_write(self, actor: "ray.actor.ActorHandle", message: Any):
        """
        Asynchronous version of write. Sends a message to the specified actor.

        Args:
            actor: The actor to send the message to.
            message: The message to send to the actor.
        """
        client_id = self._local_actors_to_client_ids.get(actor)
        if client_id is not None:
            await self._local_socket.send_multipart([client_id, msgpack.dumps(message)])
            return

        client_id = self._remote_actors_to_client_ids.get(actor)
        if client_id is not None:
            await self._socket.send_multipart([client_id, msgpack.dumps(message)])
            return

        raise Exception("Actor was never registered with `create_dealer`")


class DealerChannel:
    def __init__(
            self,
            server_address: str,
            local_server_address: str,
            server_node_id: str,
            client_id: str,
            asyncio: bool,
            max_outbound_messages: int,
            max_inbound_messages: int,
    ):
        self._server_address = server_address
        self._local_server_address = local_server_address
        self._server_node_id = server_node_id
        self._client_id = client_id
        self._socket_initialized = False
        self.asyncio = asyncio
        self.max_outbound_messages = max_outbound_messages
        self.max_inbound_messages = max_inbound_messages

    def __getstate__(self):
        return {
            "_server_address": self._server_address,
            "_local_server_address": self._local_server_address,
            "_server_node_id": self._server_node_id,
            "_client_id": self._client_id,
            "asyncio": self.asyncio,
            "max_outbound_messages": self.max_outbound_messages,
            "max_inbound_messages": self.max_inbound_messages,
            "_socket_initialized": self._socket_initialized,
        }

    def __setstate__(self, state):
        self.__init__(
            state["_server_address"],
            state["_local_server_address"],
            state["_server_node_id"],
            state["_client_id"],
            state["asyncio"],
            state["max_outbound_messages"],
            state["max_inbound_messages"],
        )
        self._socket_initialized = state["_socket_initialized"]
        self._initialize_socket()

    def _initialize_socket(self):
        # Want to run this on the actual actor process wait until deserialization
        if self._socket_initialized is True:
            raise Exception(
                "Deserializing DealerChannel where socket was already initialized"
            )
        self._context = zmq.asyncio.Context() if self.asyncio else zmq.Context()
        self._socket = self._context.socket(zmq.DEALER)
        self._socket.setsockopt(zmq.IDENTITY, self._client_id)
        self._socket.setsockopt(zmq.SNDHWM, self.max_outbound_messages)
        self._socket.setsockopt(zmq.RCVHWM, self.max_inbound_messages)
        if ray.get_runtime_context().get_node_id() == self._server_node_id:
            self._socket.connect(self._local_server_address)
        else:
            self._socket.connect(self._server_address)
        self._socket_initialized = True

    @socket_initialized_check
    @asyncio_check(expected_asyncio=False)
    def write(self, message: Any):
        """
        Sends a message to the Router.

        Args:
            message: The message to send to the Router.
        """
        self._socket.send(msgpack.dumps(message))

    @socket_initialized_check
    @asyncio_check(expected_asyncio=True)
    async def async_write(self, message: Any):
        """
        Asynchronous version of write. Sends a message to the Router.

        Args:
            message: The message to send to the Router.
        """
        await self._socket.send(msgpack.dumps(message))

    @socket_initialized_check
    @asyncio_check(expected_asyncio=False)
    def read(self) -> Any:
        """
        Blocking call that waits for / reads the next available message.

        Returns:
            The result of the read.
        """
        return msgpack.loads(self._socket.recv())

    @socket_initialized_check
    @asyncio_check(expected_asyncio=True)
    async def async_read(self) -> Awaitable[Any]:
        """
        Asynchronous version of read. Waits for and reads the next available message.

        Returns:
            The result of the read.
        """
        return msgpack.loads(await self._socket.recv())


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

    def put(self, weights_ref):
        self._weights_ref = weights_ref
        # Send new weights to all peers.
        for peer in self._other_weights_server_actors:
            peer.put.remote(weights_ref)

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
            random.choice(self.weights_server_actors).put.remote(weights_ref)

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
