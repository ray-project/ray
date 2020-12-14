from pathlib import Path
import json
import logging

"""
Note on debugging:
1) `ray up` a cluster
2) attach to the cluster
3) Kill the monitor process (find the pid with `ps -f | grep monitor.py`)
4) Launch a new monitor process to see the stack trace in case it crashes:
python anaconda3/lib/python3.7/site-packages/ray/monitor.py \
--redis-address=localhost:6379 --autoscaling-config=ray_bootstrap_config.yaml \
--redis-password 5241590000000000 --logs-dir /tmp/ray/session_latest/logs/
5) `cat /tmp/ray/session_latest/logs/mutable_status.json` will output
   the current status
"""

import ray

logger = logging.getLogger(__name__)

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

    def __iter__(self):
        return iter(self._data)

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

    def __getitem__(self, k):
        return getattr(self, k)

    def __setitem(self, k, v):
        return setattr(self, k, v)

    def __len__(self):
        return len(self._data)

    def __iter__(self):
        return iter(self._data)

class MutableStatusEncoder(json.JSONEncoder):
    def default(self, x):
        if type(x) in [MutableStatusListProxy, MutableStatusDictProxy]:
            return x._data
        return json.JSONEncoder.default(self, x)

class MutableStatusRoot:
    # todo(maximsmol): use atomic writes
    def __init__(self):
        object.__setattr__(self, "_intercept_setattr", False)

        self._f = None
        self._proxy = None
        self._readonly = False

        self._intercept_setattr = True

    def _setup(self, path):
        object.__setattr__(self, "_intercept_setattr", False)

        if self._f is not None:
            self._f.close()

        data = {}
        if path.exists():
            with path.open("r") as f:
                try:
                    data = json.load(f)
                except json.JSONDecodeError:
                    logger.exception("Error loading last saved mutable status")
                    pass

        self._f = path.open("w")
        self._proxy = MutableStatusDictProxy(self)

        for sub_k in data:
            self._proxy._set(sub_k, data[sub_k], flush=False)

        self._intercept_setattr = True

    def _setup_readonly(self, data_str):
        object.__setattr__(self, "_intercept_setattr", False)

        if self._f is not None:
            self._f.close()
        self._f = None
        self._readonly = True

        data = {}
        try:
            data = json.loads(data_str)
        except json.JSONDecodeError:
            logger.exception("Error loading mutable status")
            pass

        self._proxy = MutableStatusDictProxy(self)

        for sub_k in data:
            self._proxy._set(sub_k, data[sub_k], flush=False)

        self._intercept_setattr = True

    def _reload(self):
        self._setup(Path(self._f.name))

    def _flush(self):
        if self._readonly:
            return

        self._f.seek(0)
        json.dump(self._proxy, self._f,
            cls=MutableStatusEncoder,
            separators=(",", ":"))
        self._f.truncate()
        self._f.flush()

    def __getattr__(self, k):
        if self._proxy is None:
            raise ValueError("Status API not setup")

        return getattr(self._proxy, k)

    def __setattr__(self, k, v):
        if not self._intercept_setattr:
            return object.__setattr__(self, k, v)

        if self._proxy is None:
            raise ValueError("Status API not setup")

        return setattr(self._proxy, k, v)

    def __getitem__(self, k):
        return getattr(self, k)

    def __setitem(self, k, v):
        return setattr(self, k, v)

    def __str__(self):
        return str(self._proxy)

    def __repr__(self):
        return repr(self._proxy)

    def __len__(self):
        return len(self._proxy)

    def __iter__(self):
        return iter(self._proxy)
mutable_status = MutableStatusRoot()

def setup(session_path):
    _sess_path = Path(session_path)
    _mutable_status_path = _sess_path / "mutable_status.json"
    _event_log_path = _sess_path / "event_log.json"

    mutable_status._setup(_mutable_status_path)
