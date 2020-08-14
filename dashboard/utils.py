import abc
import asyncio
import collections
import copy
import json
import datetime
import functools
import importlib
import inspect
import logging
import pkgutil
import traceback
from base64 import b64decode
from collections.abc import MutableMapping, Mapping

import aiohttp.web
from aiohttp import hdrs
from aiohttp.frozenlist import FrozenList
import aiohttp.signals
from google.protobuf.json_format import MessageToDict
from ray.utils import binary_to_hex

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
    async def run(self):
        """
        Run the module in an asyncio loop.
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
    def _register_route(cls, method, path, **kwargs):
        def _wrapper(handler):
            if path in cls._bind_map[method]:
                bind_info = cls._bind_map[method][path]
                raise Exception("Duplicated route path: {}, "
                                "previous one registered at {}:{}".format(
                                    path, bind_info.filename,
                                    bind_info.lineno))

            bind_info = cls._BindInfo(handler.__code__.co_filename,
                                      handler.__code__.co_firstlineno, None)

            @functools.wraps(handler)
            async def _handler_route(*args, **kwargs):
                if len(args) and args[0] == bind_info.instance:
                    args = args[1:]
                try:
                    return await handler(bind_info.instance, *args, **kwargs)
                except Exception:
                    return await rest_response(
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


def get_all_modules(module_type):
    logger.info("Get all modules by type: {}".format(module_type.__name__))
    import ray.new_dashboard.modules

    for module_loader, name, ispkg in pkgutil.walk_packages(
            ray.new_dashboard.modules.__path__,
            ray.new_dashboard.modules.__name__ + "."):
        importlib.import_module(name)
    return module_type.__subclasses__()


def to_posix_time(dt):
    return (dt - datetime.datetime(1970, 1, 1)).total_seconds()


class CustomEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, bytes):
            return binary_to_hex(obj)
        # Let the base class default method raise the TypeError
        return json.JSONEncoder.default(self, obj)


async def rest_response(success, message, **kwargs) -> aiohttp.web.Response:
    return aiohttp.web.json_response(
        {
            "result": success,
            "msg": message,
            "data": to_google_style(kwargs)
        },
        dumps=functools.partial(json.dumps, cls=CustomEncoder))


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
        return "Change(owner: {}, old: {}, new: {}".format(
            self.owner, self.old, self.new)


class NotifyQueue:
    """Asyncio notify queue for Dict signal."""

    _queue = asyncio.Queue()

    @classmethod
    def put(cls, co):
        cls._queue.put_nowait(co)

    @classmethod
    async def get(cls):
        return await cls._queue.get()


class Dict(MutableMapping):
    """A simple descriptor for dict type to notify data changes.

    :note: Only the first level data report change.
    """

    def __init__(self, *args, **kwargs):
        self._data = dict(*args, **kwargs)
        self.signal = Signal(self)

    def __setitem__(self, key, value):
        old = self._data.pop(key, None)
        self._data[key] = value
        if len(self.signal) and old != value:
            if old is None:
                co = self.signal.send(Change(owner=self, new={key: value}))
            else:
                co = self.signal.send(
                    Change(owner=self, old={key: old}, new={key: value}))
            NotifyQueue.put(co)

    def __getitem__(self, item):
        return copy.deepcopy(self._data[item])

    def __delitem__(self, key):
        old = self._data.pop(key, None)
        if len(self.signal) and old is not None:
            co = self.signal.send(Change(owner=self, old={key: old}))
            NotifyQueue.put(co)

    def __len__(self):
        return len(self._data)

    def __iter__(self):
        return iter(copy.deepcopy(self._data))

    def reset(self, d):
        assert isinstance(d, Mapping)
        for key in self._data.keys() - d.keys():
            self.pop(key)
        self.update(d)
