import os
import sys
import asyncio
import collections
import copy
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
import ray.new_dashboard.consts as dashboard_consts
from aiohttp import hdrs
from aiohttp.signals import Signal
from google.protobuf.json_format import MessageToDict
from ray.utils import binary_to_hex

logger = logging.getLogger(__name__)


def agent(enable_or_cls):
    """A decorator to mark a class as an agent module.

    @agent(x is not None)
    class AgentModule:
        def __init__(self, dashboard_agent):
            pass

        async def run(self, server):
            pass
    """

    def _wrapper(cls):
        if enable_or_cls:
            assert inspect.iscoroutinefunction(cls.run)
            sig = inspect.signature(cls.run)
            assert len(sig.parameters) == 2, \
                "Expect signature: async def run(self, server), " \
                "but got `async def run{}` instead.".format(sig)
            cls.__ray_module_type__ = dashboard_consts.TYPE_AGENT
        return cls

    if inspect.isclass(enable_or_cls):
        return _wrapper(enable_or_cls)
    return _wrapper


def master(enable_or_cls):
    """A decorator to mark a class as a master module.

    @master(x is not None)
    class MasterModule:
        def __init__(self, dashboard_master):
            pass

        async def run(self):
            pass
    """

    def _wrapper(cls):
        if enable_or_cls:
            assert inspect.iscoroutinefunction(cls.run)
            sig = inspect.signature(cls.run)
            assert len(sig.parameters) == 1, \
                "Expect signature: async def run(self), " \
                "but got `async def run{}` instead.".format(sig)
            cls.__ray_module_type__ = dashboard_consts.TYPE_MASTER
        return cls

    if inspect.isclass(enable_or_cls):
        return _wrapper(enable_or_cls)
    return _wrapper


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
    def bind(cls, master_instance):
        def predicate(o):
            if inspect.ismethod(o):
                return hasattr(o, "__route_method__") and hasattr(
                    o, "__route_path__")
            return False

        handler_routes = inspect.getmembers(master_instance, predicate)
        for _, h in handler_routes:
            cls._bind_map[h.__func__.__route_method__][
                h.__func__.__route_path__].instance = master_instance


def get_all_modules(module_type):
    """Collect all modules by type."""
    logger.info("Get all modules by type: {}".format(module_type))
    assert module_type in [
        dashboard_consts.TYPE_AGENT, dashboard_consts.TYPE_MASTER
    ]
    import ray.new_dashboard.modules

    result = []
    for module_loader, name, ispkg in pkgutil.walk_packages(
            ray.new_dashboard.modules.__path__,
            ray.new_dashboard.modules.__name__ + "."):
        m = importlib.import_module(name)
        for k, v in m.__dict__.items():
            if not k.startswith("_") and inspect.isclass(v):
                mtype = getattr(v, "__ray_module_type__", None)
                if mtype == module_type:
                    result.append(v)
    return result


def to_posix_time(dt):
    return (dt - datetime.datetime(1970, 1, 1)).total_seconds()


async def rest_response(success, message, **kwargs) -> aiohttp.web.Response:
    return aiohttp.web.json_response({
        "result": success,
        "msg": message,
        "data": to_google_style(kwargs)
    })


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


def redirect_stream(stdout_filename, stderr_filename):
    """Redirect stdout or stderr to file."""
    if stdout_filename:
        fout = open(stdout_filename, "a")
        os.dup2(fout.fileno(), sys.stdout.fileno())
    if stderr_filename:
        ferr = open(stderr_filename, "a")
        os.dup2(ferr.fileno(), sys.stderr.fileno())


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


class NotifyQueue:
    """Asyncio notify queue for Dict signal."""

    _queue = asyncio.Queue()
    _signals = []

    @classmethod
    def register_signal(cls, sig):
        cls._signals.append(sig)

    @classmethod
    def freeze_signal(cls):
        for sig in cls._signals:
            sig.freeze()

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
        NotifyQueue.register_signal(self.signal)

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


class MetricExporter:
    """The base class for metric exporter."""

    def __init__(self, *args, **kwargs):
        pass

    async def commit(self,
                     timeout=dashboard_consts.REPORT_METRICS_TIMEOUT_SECONDS):
        logger.debug("%s commit", type(self).__name__)

    def put(self, metric, value, tags=None, timestamp=None):
        logger.debug("%s metric: %s, value: %s, tags: %s",
                     type(self).__name__, metric, value, tags)
        return self

    @classmethod
    def create(cls, name, *args, **kwargs):
        """Create MetricExporter instance. If the subtype does not exists,
        fallback to cls.
        """
        for subtype in MetricExporter.__subclasses__():
            if name == subtype.__name__:
                logger.info(
                    "Construct metric exporter %s with args %s, kwargs %s",
                    name, args, kwargs)
                return subtype(*args, **kwargs)
        logger.warning(
            "Construct metric exporter %s failed, "
            "available metric exporters: %s", name,
            MetricExporter.__subclasses__())
        return cls(*args, **kwargs)
