from pathlib import Path
import json
import logging

import ray

logger = logging.getLogger(__name__)

_sess_path = Path(ray.worker._global_node.get_session_dir_path())
_mutable_status_path = _sess_path / "mutable_status.json"
_event_log_path = _sess_path / "event_log.json"

print('AAAAAAAAA')

class MutableStatusBaseProxy:
    def _set(self, k, v, flush=True):
        if type(v) in [int, float, bool, str, None]:
            self._data[k] = v
            if flush:
                self._root._flush()
            return

        if isinstance(v, list):
            if k not in self._data:
                self._data[k] = MutableStatusListProxy(self._root, len(v))

            for i in range(len(v)):
                self._data[k]._set(i, v[i], flush=False)
        elif isinstance(v, dict):
            if k not in self._data:
                self._data[k] = MutableStatusDictProxy(self._root)

            for sub_k in v:
                self._data[k]._set(sub_k, v[sub_k], flush=False)
        else:
            raise ValueError(f"cannot store value of type {type(v)} "
                             f"as '{k}' in the mutable status")

        if flush:
            self._root._flush()

    def __str__(self):
        return f"{self._proxy_name}{str(self._data)}"
        return str(self._data)

    def __repr__(self):
        return f"{self._proxy_name}{repr(self._data)}"
        return repr(self._data)

class MutableStatusListProxy(MutableStatusBaseProxy):
    def __init__(self, root, length=0):
        self._root = root
        self._data = [None] * length

        self._proxy_name = "ListProxy"

    def __getitem__(self, k):
        return self._data[k]

    def __setitem(self, k, v):
        self._set(k, v)

    def __getattr__(self, k):
        return getattr(self._data, k)

class MutableStatusDictProxy(MutableStatusBaseProxy):
    def __init__(self, root):
        object.__setattr__(self, "_intercept_setattr", False)

        self._root = root
        self._data = {}

        self._proxy_name = "DictProxy"
        self._intercept_setattr = True

    def __getattr__(self, k):
        if k not in self._data:
            raise AttributeError(f"the mutable status has no key '{k}'")
        return self._data[k]

    def __setattr__(self, k, v):
        if not self._intercept_setattr:
            return object.__setattr__(self, k, v)

        self._set(k, v)

class MutableStatusEncoder(json.JSONEncoder):
    def default(self, x):
        if type(x) in [MutableStatusListProxy, MutableStatusDictProxy]:
            return x._data
        return json.JSONEncoder.default(self, x)

class MutableStatusRoot:
    def __init__(self):
        object.__setattr__(self, "_intercept_setattr", False)

        data = {}

        self.f = _mutable_status_path.open("w")
        self.proxy = MutableStatusDictProxy(self)

        for sub_k in data:
            self.proxy._set(sub_k, data[sub_k], flush=False)

        self._intercept_setattr = True

    def load(self):
        close(self.f)

        if _mutable_status_path.exists():
            with _mutable_status_path.open("r") as f:
                try:
                    data = json.load(f)
                except json.JSONDecodeError:
                    logger.exception("Error loading last saved mutable status")
                    pass

        self.f = _mutable_status_path.open("w")

    def _flush(self):
        self.f.seek(0)
        json.dump(self.proxy, self.f,
            cls=MutableStatusEncoder,
            separators=(",", ":"))
        self.f.truncate()
        self.f.flush()

    def __getattr__(self, k):
        return getattr(self.proxy, k)

    def __setattr__(self, k, v):
        if not self._intercept_setattr:
            return object.__setattr__(self, k, v)

        return setattr(self.proxy, k, v)

    def __str__(self):
        return str(self.proxy)

    def __repr__(self):
        return repr(self.proxy)

mutable_status = MutableStatusRoot()
