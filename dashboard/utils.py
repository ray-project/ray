import abc
import asyncio
import collections
import datetime
import functools
import importlib
import inspect
import json
import logging
import os
import pkgutil
import socket
import traceback
from abc import ABCMeta, abstractmethod
from base64 import b64decode
from collections import namedtuple
from collections.abc import MutableMapping, Mapping, Sequence
from typing import Any

import aiohttp.signals
import aiohttp.web
import aioredis
import time
from aiohttp import hdrs
from aiohttp.frozenlist import FrozenList
from aiohttp.typedefs import PathLike
from aiohttp.web import RouteDef
from google.protobuf.json_format import MessageToDict

import ray.new_dashboard.consts as dashboard_consts
from ray.ray_constants import env_bool
from ray.utils import binary_to_hex

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


class ClassMethodRouteTable:
    """A helper class to bind http route to class method."""

    _bind_map = collections.defaultdict(dict)
    _routes = aiohttp.web.RouteTableDef()

    class _BindInfo:
        def __init__(self, filename, lineno, instance):
            self.filename = filename
            self.lineno = lineno
            self.instance = instance

    @classmethod
    def routes(cls):
        return cls._routes

    @classmethod
    def bound_routes(cls):
        bound_items = []
        for r in cls._routes._items:
            if isinstance(r, RouteDef):
                route_method = getattr(r.handler, "__route_method__")
                route_path = getattr(r.handler, "__route_path__")
                instance = cls._bind_map[route_method][route_path].instance
                if instance is not None:
                    bound_items.append(r)
            else:
                bound_items.append(r)
        routes = aiohttp.web.RouteTableDef()
        routes._items = bound_items
        return routes

    @classmethod
    def _register_route(cls, method, path, **kwargs):
        def _wrapper(handler):
            if path in cls._bind_map[method]:
                bind_info = cls._bind_map[method][path]
                raise Exception(f"Duplicated route path: {path}, "
                                f"previous one registered at "
                                f"{bind_info.filename}:{bind_info.lineno}")

            bind_info = cls._BindInfo(handler.__code__.co_filename,
                                      handler.__code__.co_firstlineno, None)

            @functools.wraps(handler)
            async def _handler_route(*args) -> aiohttp.web.Response:
                try:
                    # Make the route handler as a bound method.
                    # The args may be:
                    #   * (Request, )
                    #   * (self, Request)
                    req = args[-1]
                    return await handler(bind_info.instance, req)
                except Exception:
                    logger.exception("Handle %s %s failed.", method, path)
                    return rest_response(
                        success=False, message=traceback.format_exc())

            cls._bind_map[method][path] = bind_info
            _handler_route.__route_method__ = method
            _handler_route.__route_path__ = path
            return cls._routes.route(method, path, **kwargs)(_handler_route)

        return _wrapper

    @classmethod
    def head(cls, path, **kwargs):
        return cls._register_route(hdrs.METH_HEAD, path, **kwargs)

    @classmethod
    def get(cls, path, **kwargs):
        return cls._register_route(hdrs.METH_GET, path, **kwargs)

    @classmethod
    def post(cls, path, **kwargs):
        return cls._register_route(hdrs.METH_POST, path, **kwargs)

    @classmethod
    def put(cls, path, **kwargs):
        return cls._register_route(hdrs.METH_PUT, path, **kwargs)

    @classmethod
    def patch(cls, path, **kwargs):
        return cls._register_route(hdrs.METH_PATCH, path, **kwargs)

    @classmethod
    def delete(cls, path, **kwargs):
        return cls._register_route(hdrs.METH_DELETE, path, **kwargs)

    @classmethod
    def view(cls, path, **kwargs):
        return cls._register_route(hdrs.METH_ANY, path, **kwargs)

    @classmethod
    def static(cls, prefix: str, path: PathLike, **kwargs: Any) -> None:
        cls._routes.static(prefix, path, **kwargs)

    @classmethod
    def bind(cls, instance):
        def predicate(o):
            if inspect.ismethod(o):
                return hasattr(o, "__route_method__") and hasattr(
                    o, "__route_path__")
            return False

        handler_routes = inspect.getmembers(instance, predicate)
        for _, h in handler_routes:
            cls._bind_map[h.__func__.__route_method__][
                h.__func__.__route_path__].instance = instance


def dashboard_module(enable):
    """A decorator for dashboard module."""

    def _cls_wrapper(cls):
        cls.__ray_dashboard_module_enable__ = enable
        return cls

    return _cls_wrapper


def get_all_modules(module_type):
    logger.info(f"Get all modules by type: {module_type.__name__}")
    import ray.new_dashboard.modules

    for module_loader, name, ispkg in pkgutil.walk_packages(
            ray.new_dashboard.modules.__path__,
            ray.new_dashboard.modules.__name__ + "."):
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


def rest_response(success, message, **kwargs) -> aiohttp.web.Response:
    # In the dev context we allow a dev server running on a
    # different port to consume the API, meaning we need to allow
    # cross-origin access
    if os.environ.get("RAY_DASHBOARD_DEV") == "1":
        headers = {"Access-Control-Allow-Origin": "*"}
    else:
        headers = {}
    return aiohttp.web.json_response(
        {
            "result": success,
            "msg": message,
            "data": to_google_style(kwargs)
        },
        dumps=functools.partial(json.dumps, cls=CustomEncoder),
        headers=headers)


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


# The cache value type used by aiohttp_cache.
_AiohttpCacheValue = namedtuple("AiohttpCacheValue",
                                ["data", "expiration", "task"])
# The methods with no request body used by aiohttp_cache.
_AIOHTTP_CACHE_NOBODY_METHODS = {hdrs.METH_GET, hdrs.METH_DELETE}


def aiohttp_cache(
        ttl_seconds=dashboard_consts.AIOHTTP_CACHE_TTL_SECONDS,
        maxsize=dashboard_consts.AIOHTTP_CACHE_MAX_SIZE,
        enable=not env_bool(
            dashboard_consts.AIOHTTP_CACHE_DISABLE_ENVIRONMENT_KEY, False)):
    assert maxsize > 0
    cache = collections.OrderedDict()

    def _wrapper(handler):
        if enable:

            @functools.wraps(handler)
            async def _cache_handler(*args) -> aiohttp.web.Response:
                # Make the route handler as a bound method.
                # The args may be:
                #   * (Request, )
                #   * (self, Request)
                req = args[-1]
                # Make key.
                if req.method in _AIOHTTP_CACHE_NOBODY_METHODS:
                    key = req.path_qs
                else:
                    key = (req.path_qs, await req.read())
                # Query cache.
                value = cache.get(key)
                if value is not None:
                    cache.move_to_end(key)
                    if (not value.task.done()
                            or value.expiration >= time.time()):
                        # Update task not done or the data is not expired.
                        return aiohttp.web.Response(**value.data)

                def _update_cache(task):
                    try:
                        response = task.result()
                    except Exception:
                        response = rest_response(
                            success=False, message=traceback.format_exc())
                    data = {
                        "status": response.status,
                        "headers": dict(response.headers),
                        "body": response.body,
                    }
                    cache[key] = _AiohttpCacheValue(data,
                                                    time.time() + ttl_seconds,
                                                    task)
                    cache.move_to_end(key)
                    if len(cache) > maxsize:
                        cache.popitem(last=False)
                    return response

                task = create_task(handler(*args))
                task.add_done_callback(_update_cache)
                if value is None:
                    return await task
                else:
                    return aiohttp.web.Response(**value.data)

            suffix = f"[cache ttl={ttl_seconds}, max_size={maxsize}]"
            _cache_handler.__name__ += suffix
            _cache_handler.__qualname__ += suffix
            return _cache_handler
        else:
            return handler

    if inspect.iscoroutinefunction(ttl_seconds):
        target_func = ttl_seconds
        ttl_seconds = dashboard_consts.AIOHTTP_CACHE_TTL_SECONDS
        return _wrapper(target_func)
    else:
        return _wrapper


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


class Signal(aiohttp.signals.Signal):
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


def async_loop_forever(interval_seconds):
    def _wrapper(coro):
        @functools.wraps(coro)
        async def _looper(*args, **kwargs):
            while True:
                try:
                    await coro(*args, **kwargs)
                except Exception:
                    logger.exception(f"Error looping coroutine {coro}.")
                await asyncio.sleep(interval_seconds)

        return _looper

    return _wrapper
