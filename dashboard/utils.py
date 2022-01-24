import abc
import asyncio
import datetime
import functools
import importlib
import json
import logging
import pkgutil
import socket
from abc import ABCMeta, abstractmethod
from base64 import b64decode
from collections import namedtuple
from collections.abc import MutableMapping, Mapping, Sequence

import aioredis  # noqa: F401
import aiosignal  # noqa: F401

from google.protobuf.json_format import MessageToDict
from frozenlist import FrozenList  # noqa: F401

from ray._private.utils import binary_to_hex

try:
    create_task = asyncio.create_task
except AttributeError:
    create_task = asyncio.ensure_future

logger = logging.getLogger(__name__)


class DashboardAgentModule(abc.ABC):
    def __init__(self, dashboard_agent):
        """
        Initialize current module when DashboardAgent loading modules.
        :param dashboard_agent: The DashboardAgent instance.
        """
        self._dashboard_agent = dashboard_agent

    @abc.abstractmethod
    async def run(self, server):
        """
        Run the module in an asyncio loop. An agent module can provide
        servicers to the server.
        :param server: Asyncio GRPC server.
        """


class DashboardHeadModule(abc.ABC):
    def __init__(self, dashboard_head):
        """
        Initialize current module when DashboardHead loading modules.
        :param dashboard_head: The DashboardHead instance.
        """
        self._dashboard_head = dashboard_head

    @abc.abstractmethod
    async def run(self, server):
        """
        Run the module in an asyncio loop. A head module can provide
        servicers to the server.
        :param server: Asyncio GRPC server.
        """


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

    for module_loader, name, ispkg in pkgutil.walk_packages(
            ray.dashboard.modules.__path__,
            ray.dashboard.modules.__name__ + "."):
        importlib.import_module(name)
    return [
        m for m in module_type.__subclasses__()
        if getattr(m, "__ray_dashboard_module_enable__", True)
    ]


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

    if decode_keys:
        return _decode_keys(
            MessageToDict(message, use_integers_for_enums=False, **kwargs))
    else:
        return MessageToDict(message, use_integers_for_enums=False, **kwargs)


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
        return f"Change(owner: {type(self.owner)}), " \
               f"old: {self.old}, new: {self.new}"


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
_json_compatible_types = {
    dict, list, tuple, str, int, float, bool,
    type(None), bytes
}


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
    """Makes a :class:`list` immutable.
    """

    __slots__ = ("_list", "_proxy")

    def __init__(self, list_value):
        if type(list_value) not in (list, ImmutableList):
            raise TypeError(f"{type(list_value)} object is not a list.")
        if isinstance(list_value, ImmutableList):
            list_value = list_value.mutable()
        self._list = list_value
        self._proxy = [None] * len(list_value)

    def __reduce_ex__(self, protocol):
        return type(self), (self._list, )

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
    """Makes a :class:`dict` immutable.
    """

    __slots__ = ("_dict", "_proxy")

    def __init__(self, dict_value):
        if type(dict_value) not in (dict, ImmutableDict):
            raise TypeError(f"{type(dict_value)} object is not a dict.")
        if isinstance(dict_value, ImmutableDict):
            dict_value = dict_value.mutable()
        self._dict = dict_value
        self._proxy = {}

    def __reduce_ex__(self, protocol):
        return type(self), (self._dict, )

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
                    Change(owner=self, new=Dict.ChangeItem(key, value)))
            else:
                co = self.signal.send(
                    Change(
                        owner=self,
                        old=Dict.ChangeItem(key, old),
                        new=Dict.ChangeItem(key, value)))
            NotifyQueue.put(co)

    def __delitem__(self, key):
        old = self._dict.pop(key, None)
        self._proxy.pop(key, None)
        if len(self.signal) and old is not None:
            co = self.signal.send(
                Change(owner=self, old=Dict.ChangeItem(key, old)))
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
                        logger.info(f"An async loop forever coroutine "
                                    f"is cancelled {coro}.")
                        raise ex
                    else:
                        logger.exception(f"Can not cancel the async loop "
                                         f"forever coroutine {coro}.")
                except Exception:
                    logger.exception(f"Error looping coroutine {coro}.")
                await asyncio.sleep(interval_seconds)

        return _looper

    return _wrapper


async def get_aioredis_client(redis_address, redis_password,
                              retry_interval_seconds, retry_times):
    for x in range(retry_times):
        try:
            return await aioredis.create_redis_pool(
                address=redis_address, password=redis_password)
        except (socket.gaierror, ConnectionError) as ex:
            logger.error("Connect to Redis failed: %s, retry...", ex)
            await asyncio.sleep(retry_interval_seconds)
    # Raise exception from create_redis_pool
    return await aioredis.create_redis_pool(
        address=redis_address, password=redis_password)
