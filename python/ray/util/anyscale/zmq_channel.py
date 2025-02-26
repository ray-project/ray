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
