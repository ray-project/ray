import abc
import asyncio
import datetime
import functools
import importlib
import json
import logging
import os
import pkgutil
from abc import ABCMeta, abstractmethod
from base64 import b64decode
from collections import namedtuple
from collections.abc import Mapping, MutableMapping, Sequence
from typing import Optional

import aiosignal  # noqa: F401
from frozenlist import FrozenList  # noqa: F401

import ray
import ray._private.protobuf_compat
import ray._private.ray_constants as ray_constants
import ray._private.services as services
from ray._private.utils import (
    binary_to_hex,
    check_dashboard_dependencies_installed,
    split_address,
)
from ray._raylet import GcsClient

try:
    create_task = asyncio.create_task
except AttributeError:
    create_task = asyncio.ensure_future

logger = logging.getLogger(__name__)


class FrontendNotFoundError(OSError):
    pass


class DashboardAgentModule(abc.ABC):
    def __init__(self, dashboard_agent):
        """
        Initialize current module when DashboardAgent loading modules.
        :param dashboard_agent: The DashboardAgent instance.
        """
        self._dashboard_agent = dashboard_agent
        self.session_name = dashboard_agent.session_name

    @abc.abstractmethod
    async def run(self, server):
        """
        Run the module in an asyncio loop. An agent module can provide
        servicers to the server.
        :param server: Asyncio GRPC server, or None if ray is minimal.
        """

    @staticmethod
    @abc.abstractclassmethod
    def is_minimal_module():
        """
        Return True if the module is minimal, meaning it
        should work with `pip install ray` that doesn't requires additional
        dependencies.
        """

    def get_gcs_address(self):
        return self._dashboard_agent.gcs_address


class DashboardHeadModule(abc.ABC):
    def __init__(self, dashboard_head):
        """
        Initialize current module when DashboardHead loading modules.
        :param dashboard_head: The DashboardHead instance.
        """
        self._dashboard_head = dashboard_head
        self.session_name = dashboard_head.session_name

    @abc.abstractmethod
    async def run(self, server):
        """
        Run the module in an asyncio loop. A head module can provide
        servicers to the server.
        :param server: Asyncio GRPC server, or None if ray is minimal.
        """

    @staticmethod
    @abc.abstractclassmethod
    def is_minimal_module():
        """
        Return True if the module is minimal, meaning it
        should work with `pip install ray` that doesn't requires additional
        dependencies.
        """

    def get_gcs_address(self):
        return self._dashboard_head.gcs_address


def dashboard_module(enable):
    """A decorator for dashboard module."""

    def _cls_wrapper(cls):
        cls.__ray_dashboard_module_enable__ = enable
        return cls

    return _cls_wrapper


def get_all_modules(module_type):
    """
    Get all importable modules that are subclass of a given module type.
    """
    logger.info(f"Get all modules by type: {module_type.__name__}")
    import ray.dashboard.modules

    should_only_load_minimal_modules = not check_dashboard_dependencies_installed()

    for module_loader, name, ispkg in pkgutil.walk_packages(
        ray.dashboard.modules.__path__, ray.dashboard.modules.__name__ + "."
    ):
        try:
            importlib.import_module(name)
        except ModuleNotFoundError as e:
            logger.info(
                f"Module {name} cannot be loaded because "
                "we cannot import all dependencies. Install this module using "
                "`pip install 'ray[default]'` for the full "
                f"dashboard functionality. Error: {e}"
            )
            if not should_only_load_minimal_modules:
                logger.info(
                    "Although `pip install 'ray[default]'` is downloaded, "
                    "module couldn't be imported`"
                )
                raise e

    imported_modules = []
    # module_type.__subclasses__() should contain modules that
    # we could successfully import.
    for m in module_type.__subclasses__():
        if not getattr(m, "__ray_dashboard_module_enable__", True):
            continue
        if should_only_load_minimal_modules and not m.is_minimal_module():
            continue
        imported_modules.append(m)
    logger.info(f"Available modules: {imported_modules}")
    return imported_modules


def to_posix_time(dt):
    return (dt - datetime.datetime(1970, 1, 1)).total_seconds()


def address_tuple(address):
    if isinstance(address, tuple):
        return address
    ip, port = address.split(":")
    return ip, int(port)


class CustomEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, bytes):
            return binary_to_hex(obj)
        if isinstance(obj, Immutable):
            return obj.mutable()
        # Let the base class default method raise the TypeError
        return json.JSONEncoder.default(self, obj)


def to_camel_case(snake_str):
    """Convert a snake str to camel case."""
    components = snake_str.split("_")
    # We capitalize the first letter of each component except the first one
    # with the 'title' method and join them together.
    return components[0] + "".join(x.title() for x in components[1:])


def to_google_style(d):
    """Recursive convert all keys in dict to google style."""
    new_dict = {}

    for k, v in d.items():
        if isinstance(v, dict):
            new_dict[to_camel_case(k)] = to_google_style(v)
        elif isinstance(v, list):
            new_list = []
            for i in v:
                if isinstance(i, dict):
                    new_list.append(to_google_style(i))
                else:
                    new_list.append(i)
            new_dict[to_camel_case(k)] = new_list
        else:
            new_dict[to_camel_case(k)] = v
    return new_dict


def message_to_dict(message, decode_keys=None, **kwargs):
    """Convert protobuf message to Python dict."""

    def _decode_keys(d):
        for k, v in d.items():
            if isinstance(v, dict):
                d[k] = _decode_keys(v)
            if isinstance(v, list):
                new_list = []
                for i in v:
                    if isinstance(i, dict):
                        new_list.append(_decode_keys(i))
                    else:
                        new_list.append(i)
                d[k] = new_list
            else:
                if k in decode_keys:
                    d[k] = binary_to_hex(b64decode(v))
                else:
                    d[k] = v
        return d

    d = ray._private.protobuf_compat.message_to_dict(
        message, use_integers_for_enums=False, **kwargs
    )
    if decode_keys:
        return _decode_keys(d)
    else:
        return d


class SignalManager:
    _signals = FrozenList()

    @classmethod
    def register(cls, sig):
        cls._signals.append(sig)

    @classmethod
    def freeze(cls):
        cls._signals.freeze()
        for sig in cls._signals:
            sig.freeze()


class Signal(aiosignal.Signal):
    __slots__ = ()

    def __init__(self, owner):
        super().__init__(owner)
        SignalManager.register(self)


class Bunch(dict):
    """A dict with attribute-access."""

    def __getattr__(self, key):
        try:
            return self.__getitem__(key)
        except KeyError:
            raise AttributeError(key)

    def __setattr__(self, key, value):
        self.__setitem__(key, value)


class Change:
    """Notify change object."""

    def __init__(self, owner=None, old=None, new=None):
        self.owner = owner
        self.old = old
        self.new = new

    def __str__(self):
        return (
            f"Change(owner: {type(self.owner)}), " f"old: {self.old}, new: {self.new}"
        )


class NotifyQueue:
    """Asyncio notify queue for Dict signal."""

    _queue = asyncio.Queue()

    @classmethod
    def put(cls, co):
        cls._queue.put_nowait(co)

    @classmethod
    async def get(cls):
        return await cls._queue.get()


"""
https://docs.python.org/3/library/json.html?highlight=json#json.JSONEncoder
    +-------------------+---------------+
    | Python            | JSON          |
    +===================+===============+
    | dict              | object        |
    +-------------------+---------------+
    | list, tuple       | array         |
    +-------------------+---------------+
    | str               | string        |
    +-------------------+---------------+
    | int, float        | number        |
    +-------------------+---------------+
    | True              | true          |
    +-------------------+---------------+
    | False             | false         |
    +-------------------+---------------+
    | None              | null          |
    +-------------------+---------------+
"""
_json_compatible_types = {dict, list, tuple, str, int, float, bool, type(None), bytes}


def is_immutable(self):
    raise TypeError("%r objects are immutable" % self.__class__.__name__)


def make_immutable(value, strict=True):
    value_type = type(value)
    if value_type is dict:
        return ImmutableDict(value)
    if value_type is list:
        return ImmutableList(value)
    if strict:
        if value_type not in _json_compatible_types:
            raise TypeError("Type {} can't be immutable.".format(value_type))
    return value


class Immutable(metaclass=ABCMeta):
    @abstractmethod
    def mutable(self):
        pass


class ImmutableList(Immutable, Sequence):
    """Makes a :class:`list` immutable."""

    __slots__ = ("_list", "_proxy")

    def __init__(self, list_value):
        if type(list_value) not in (list, ImmutableList):
            raise TypeError(f"{type(list_value)} object is not a list.")
        if isinstance(list_value, ImmutableList):
            list_value = list_value.mutable()
        self._list = list_value
        self._proxy = [None] * len(list_value)

    def __reduce_ex__(self, protocol):
        return type(self), (self._list,)

    def mutable(self):
        return self._list

    def __eq__(self, other):
        if isinstance(other, ImmutableList):
            other = other.mutable()
        return list.__eq__(self._list, other)

    def __ne__(self, other):
        if isinstance(other, ImmutableList):
            other = other.mutable()
        return list.__ne__(self._list, other)

    def __contains__(self, item):
        if isinstance(item, Immutable):
            item = item.mutable()
        return list.__contains__(self._list, item)

    def __getitem__(self, item):
        proxy = self._proxy[item]
        if proxy is None:
            proxy = self._proxy[item] = make_immutable(self._list[item])
        return proxy

    def __len__(self):
        return len(self._list)

    def __repr__(self):
        return "%s(%s)" % (self.__class__.__name__, list.__repr__(self._list))


class ImmutableDict(Immutable, Mapping):
    """Makes a :class:`dict` immutable."""

    __slots__ = ("_dict", "_proxy")

    def __init__(self, dict_value):
        if type(dict_value) not in (dict, ImmutableDict):
            raise TypeError(f"{type(dict_value)} object is not a dict.")
        if isinstance(dict_value, ImmutableDict):
            dict_value = dict_value.mutable()
        self._dict = dict_value
        self._proxy = {}

    def __reduce_ex__(self, protocol):
        return type(self), (self._dict,)

    def mutable(self):
        return self._dict

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return make_immutable(default)

    def __eq__(self, other):
        if isinstance(other, ImmutableDict):
            other = other.mutable()
        return dict.__eq__(self._dict, other)

    def __ne__(self, other):
        if isinstance(other, ImmutableDict):
            other = other.mutable()
        return dict.__ne__(self._dict, other)

    def __contains__(self, item):
        if isinstance(item, Immutable):
            item = item.mutable()
        return dict.__contains__(self._dict, item)

    def __getitem__(self, item):
        proxy = self._proxy.get(item, None)
        if proxy is None:
            proxy = self._proxy[item] = make_immutable(self._dict[item])
        return proxy

    def __len__(self) -> int:
        return len(self._dict)

    def __iter__(self):
        if len(self._proxy) != len(self._dict):
            for key in self._dict.keys() - self._proxy.keys():
                self._proxy[key] = make_immutable(self._dict[key])
        return iter(self._proxy)

    def __repr__(self):
        return "%s(%s)" % (self.__class__.__name__, dict.__repr__(self._dict))


class MutableNotificationDict(dict, MutableMapping):
    """A simple descriptor for dict type to notify data changes.
    :note: Only the first level data report change.
    """

    ChangeItem = namedtuple("DictChangeItem", ["key", "value"])

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._signal = Signal(self)

    def mutable(self):
        return self

    @property
    def signal(self):
        return self._signal

    def __setitem__(self, key, value):
        old = self.pop(key, None)
        super().__setitem__(key, value)
        if len(self._signal) and old != value:
            if old is None:
                co = self._signal.send(
                    Change(owner=self, new=Dict.ChangeItem(key, value))
                )
            else:
                co = self._signal.send(
                    Change(
                        owner=self,
                        old=Dict.ChangeItem(key, old),
                        new=Dict.ChangeItem(key, value),
                    )
                )
            NotifyQueue.put(co)

    def __delitem__(self, key):
        old = self.pop(key, None)
        if len(self._signal) and old is not None:
            co = self._signal.send(Change(owner=self, old=Dict.ChangeItem(key, old)))
            NotifyQueue.put(co)

    def reset(self, d):
        assert isinstance(d, Mapping)
        for key in self.keys() - d.keys():
            del self[key]
        for key, value in d.items():
            self[key] = value


class Dict(ImmutableDict, MutableMapping):
    """A simple descriptor for dict type to notify data changes.
    :note: Only the first level data report change.
    """

    ChangeItem = namedtuple("DictChangeItem", ["key", "value"])

    def __init__(self, *args, **kwargs):
        super().__init__(dict(*args, **kwargs))
        self.signal = Signal(self)

    def __setitem__(self, key, value):
        old = self._dict.pop(key, None)
        self._proxy.pop(key, None)
        self._dict[key] = value
        if len(self.signal) and old != value:
            if old is None:
                co = self.signal.send(
                    Change(owner=self, new=Dict.ChangeItem(key, value))
                )
            else:
                co = self.signal.send(
                    Change(
                        owner=self,
                        old=Dict.ChangeItem(key, old),
                        new=Dict.ChangeItem(key, value),
                    )
                )
            NotifyQueue.put(co)

    def __delitem__(self, key):
        old = self._dict.pop(key, None)
        self._proxy.pop(key, None)
        if len(self.signal) and old is not None:
            co = self.signal.send(Change(owner=self, old=Dict.ChangeItem(key, old)))
            NotifyQueue.put(co)

    def reset(self, d):
        assert isinstance(d, Mapping)
        for key in self._dict.keys() - d.keys():
            del self[key]
        for key, value in d.items():
            self[key] = value


# Register immutable types.
for immutable_type in Immutable.__subclasses__():
    _json_compatible_types.add(immutable_type)


def async_loop_forever(interval_seconds, cancellable=False):
    def _wrapper(coro):
        @functools.wraps(coro)
        async def _looper(*args, **kwargs):
            while True:
                try:
                    await coro(*args, **kwargs)
                except asyncio.CancelledError as ex:
                    if cancellable:
                        logger.info(
                            f"An async loop forever coroutine " f"is cancelled {coro}."
                        )
                        raise ex
                    else:
                        logger.exception(
                            f"Can not cancel the async loop "
                            f"forever coroutine {coro}."
                        )
                except Exception:
                    logger.exception(f"Error looping coroutine {coro}.")
                await asyncio.sleep(interval_seconds)

        return _looper

    return _wrapper


def ray_client_address_to_api_server_url(address: str):
    """Convert a Ray Client address of a running Ray cluster to its API server URL.

    Args:
        address: The Ray Client address, e.g. "ray://my-cluster".

    Returns:
        str: The API server URL of the cluster, e.g. "http://<head-node-ip>:8265".
    """
    with ray.init(address=address) as client_context:
        dashboard_url = client_context.dashboard_url

    return f"http://{dashboard_url}"


def ray_address_to_api_server_url(address: Optional[str]) -> str:
    """Parse a Ray cluster address into API server URL.

    When an address is provided, it will be used to query GCS for
    API server address from GCS, so a Ray cluster must be running.

    When an address is not provided, it will first try to auto-detect
    a running Ray instance, or look for local GCS process.

    Args:
        address: Ray cluster bootstrap address or Ray Client address.
            Could also be `auto`.

    Returns:
        API server HTTP URL.
    """

    address = services.canonicalize_bootstrap_address_or_die(address)
    gcs_client = GcsClient(address=address, nums_reconnect_retry=0)

    ray.experimental.internal_kv._initialize_internal_kv(gcs_client)
    api_server_url = ray._private.utils.internal_kv_get_with_retry(
        gcs_client,
        ray_constants.DASHBOARD_ADDRESS,
        namespace=ray_constants.KV_NAMESPACE_DASHBOARD,
        num_retries=20,
    )

    if api_server_url is None:
        raise ValueError(
            (
                "Couldn't obtain the API server address from GCS. It is likely that "
                "the GCS server is down. Check gcs_server.[out | err] to see if it is "
                "still alive."
            )
        )
    api_server_url = f"http://{api_server_url.decode()}"
    return api_server_url


def get_address_for_submission_client(address: Optional[str]) -> str:
    """Get Ray API server address from Ray bootstrap or Client address.

    If None, it will try to auto-detect a running Ray instance, or look
    for local GCS process.

    `address` is always overridden by the RAY_ADDRESS environment
    variable, just like the `address` argument in `ray.init()`.

    Args:
        address: Ray cluster bootstrap address or Ray Client address.
            Could also be "auto".

    Returns:
        API server HTTP URL, e.g. "http://<head-node-ip>:8265".
    """
    if os.environ.get("RAY_ADDRESS"):
        logger.debug(f"Using RAY_ADDRESS={os.environ['RAY_ADDRESS']}")
        address = os.environ["RAY_ADDRESS"]

    if address and "://" in address:
        module_string, _ = split_address(address)
        if module_string == "ray":
            logger.debug(
                f"Retrieving API server address from Ray Client address {address}..."
            )
            address = ray_client_address_to_api_server_url(address)
    else:
        # User specified a non-Ray-Client Ray cluster address.
        address = ray_address_to_api_server_url(address)
    logger.debug(f"Using API server address {address}.")
    return address


def compose_state_message(
    death_reason: Optional[str], death_reason_message: Optional[str]
) -> Optional[str]:
    """Compose node state message based on death information.

    Args:
        death_reason: The reason of node death.
            This is a string representation of `gcs_pb2.NodeDeathInfo.Reason`.
        death_reason_message: The message of node death.
            This corresponds to `gcs_pb2.NodeDeathInfo.ReasonMessage`.
    """
    if death_reason == "EXPECTED_TERMINATION":
        state_message = "Expected termination"
    elif death_reason == "UNEXPECTED_TERMINATION":
        state_message = "Unexpected termination"
    elif death_reason == "AUTOSCALER_DRAIN_PREEMPTED":
        state_message = "Terminated due to preemption"
    elif death_reason == "AUTOSCALER_DRAIN_IDLE":
        state_message = "Terminated due to idle (no Ray activity)"
    else:
        state_message = None

    if death_reason_message:
        if state_message:
            state_message += f": {death_reason_message}"
        else:
            state_message = death_reason_message
    return state_message
