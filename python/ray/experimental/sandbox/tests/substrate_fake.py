"""A fake ate-env-api client for test_substrate_backend.py.

Not a test module. The tests and the processes they start load it by path,
through ``RAY_SUBSTRATE_CLIENT_FACTORY=<this file>:fake_client_factory``.

``FakeClient`` keeps each environment as a directory under
``SUBSTRATE_FAKE_ROOT``, so threads and processes share one view the way they
share one ate-env-api, and logs every call to ``ops.log`` there. Commands run
with the host's /bin/sh, with ``/tmp/`` rewritten to the environment's own
directory, so copies don't see each other's writes. A fork holds an operation
lock on its source, so a concurrent fork of it fails with ABORTED, as on
Substrate (the backend retries). ``SUBSTRATE_FAKE_LATENCY_S`` adds a sleep to every fork.
"""

import asyncio
import json
import os
import shutil
import threading
from types import SimpleNamespace

RUNNING, SUSPENDED = 2, 4


class NotFoundError(Exception):
    """Named like ate_env_rl.NotFoundError, which the backend matches by name."""


class RpcError(Exception):
    def __init__(self, message, code):
        super().__init__(message)
        self.code = SimpleNamespace(name=code)


def _write_json(path, data):
    tmp = f"{path}.tmp.{os.getpid()}.{threading.get_ident()}"
    with open(tmp, "w") as f:
        json.dump(data, f)
    os.replace(tmp, path)


class FakeEnv:
    def __init__(self, client, env_id, atespace):
        self._client = client
        self.id = env_id
        self.atespace = atespace

    def _fs(self):
        meta = self._client._meta(self.atespace, self.id)
        if meta["status"] != RUNNING:  # any guest call wakes it, as on Substrate
            self._client._set_status(self.atespace, self.id, RUNNING)
        return os.path.join(self._client._dir(self.atespace, self.id), "fs")

    async def shell(self, command_line, *, cwd="", env=None, timeout=None):
        self._client._log("shell", id=self.id, line=command_line)
        fs = self._fs()
        proc = await asyncio.create_subprocess_exec(
            "/bin/sh",
            "-c",
            command_line.replace("/tmp/", f"{fs}/tmp/"),
            cwd=os.path.join(fs, cwd.lstrip("/")) if cwd else fs,
            env={**os.environ, **dict(env or {})},
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            out, err = await asyncio.wait_for(proc.communicate(), timeout)
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()
            return SimpleNamespace(stdout="", stderr="", exit_code=137, timed_out=True)
        return SimpleNamespace(
            stdout=out.decode(),
            stderr=err.decode(),
            exit_code=proc.returncode,
            timed_out=False,
        )

    async def read_file_bytes(self, path):
        try:
            with open(os.path.join(self._fs(), path.lstrip("/")), "rb") as f:
                return f.read()
        except FileNotFoundError:
            raise NotFoundError(f"open {path}: no such file or directory") from None

    async def write_file(self, path, data, *, mode=0o644, seek_offset=0):
        host = os.path.join(self._fs(), path.lstrip("/"))
        os.makedirs(os.path.dirname(host), exist_ok=True)
        with open(host, "r+b" if seek_offset and os.path.exists(host) else "wb") as f:
            f.seek(seek_offset)
            f.write(data)
        return len(data)


class FakeClient:
    def __init__(self, root, latency_s=0.0):
        self.root = str(root)
        self.latency_s = latency_s
        self.closed = False
        os.makedirs(self.root, exist_ok=True)

    def _dir(self, atespace, env_id):
        return os.path.join(self.root, atespace, env_id)

    def _log(self, op, **kw):
        line = json.dumps({"op": op, "pid": os.getpid(), **kw}) + "\n"
        with open(os.path.join(self.root, "ops.log"), "a") as f:
            f.write(line)

    def ops(self, op=None):
        try:
            with open(os.path.join(self.root, "ops.log")) as f:
                rows = [json.loads(line) for line in f]
        except FileNotFoundError:
            return []
        return [r for r in rows if op is None or r["op"] == op]

    def live(self):
        d = os.path.join(self.root, "ate-env")
        if not os.path.isdir(d):
            return set()
        return {n for n in os.listdir(d) if "." not in n}

    def status(self, env_id, atespace="ate-env"):
        return self._meta(atespace, env_id)["status"]

    def _meta(self, atespace, env_id):
        try:
            with open(os.path.join(self._dir(atespace, env_id), "meta.json")) as f:
                return json.load(f)
        except FileNotFoundError:
            raise NotFoundError(f'environment "{env_id}" not found') from None

    def _set_status(self, atespace, env_id, status):
        meta = self._meta(atespace, env_id)
        meta["status"] = status
        _write_json(os.path.join(self._dir(atespace, env_id), "meta.json"), meta)

    def env(self, env_id, *, atespace="ate-env"):
        return FakeEnv(self, env_id, atespace)

    async def close(self):
        self.closed = True

    async def server_info(self):
        self._log("server_info")
        no_fork = os.environ.get("SUBSTRATE_FAKE_NO_FORK_FEATURE")
        features = () if no_fork else ("fork_environment",)
        return SimpleNamespace(server="fake", version="0", features=features)

    async def create(
        self, env_id, *, atespace, template_name, rootfs_image, worker_selector
    ):
        self._log(
            "create",
            id=env_id,
            template=template_name,
            image=rootfs_image,
            selector=dict(worker_selector or {}),
        )
        if "@sha256:" not in rootfs_image:
            raise RpcError("image is not pinned by digest", "INVALID_ARGUMENT")
        d = self._dir(atespace, env_id)
        os.makedirs(os.path.join(d, "fs", "tmp"))
        _write_json(os.path.join(d, "meta.json"), {"status": RUNNING})
        return self.env(env_id, atespace=atespace)

    async def get(self, env_id, *, atespace):
        return SimpleNamespace(status=self._meta(atespace, env_id)["status"])

    async def suspend(self, env_id, *, atespace, mode):
        self._log("suspend", id=env_id, mode=mode)
        self._set_status(atespace, env_id, SUSPENDED)

    async def delete(self, env_id, *, atespace):
        d = self._dir(atespace, env_id)
        if os.path.exists(d + ".op"):  # busy with a fork, as on Substrate
            self._log("delete_aborted", id=env_id)
            raise RpcError(
                f'ate: deleting "{env_id}": another operation is in progress',
                "ABORTED",
            )
        self._log("delete", id=env_id)
        gone = f"{d}.deleting.{os.getpid()}.{threading.get_ident()}"
        try:
            os.rename(d, gone)
        except FileNotFoundError:
            raise NotFoundError(f'environment "{env_id}" not found') from None
        shutil.rmtree(gone, ignore_errors=True)

    async def fork(self, env_id, count, *, atespace, ids, worker_selector=None):
        self._meta(atespace, env_id)
        op = self._dir(atespace, env_id) + ".op"
        try:
            os.close(os.open(op, os.O_CREAT | os.O_EXCL | os.O_WRONLY))
        except FileExistsError:
            self._log("fork_aborted", id=env_id)
            raise RpcError(
                f'ate: suspending "{env_id}": another operation is in progress',
                "ABORTED",
            ) from None
        try:
            self._log(
                "fork",
                id=env_id,
                count=count,
                ids=list(ids),
                selector=dict(worker_selector or {}),
            )
            if self.latency_s:
                await asyncio.sleep(self.latency_s)
            src = os.path.join(self._dir(atespace, env_id), "fs")
            if not os.path.isdir(src):  # deleted during the fork
                raise NotFoundError(f'environment "{env_id}" not found')
            for i in ids:
                shutil.copytree(src, os.path.join(self._dir(atespace, i), "fs"))
                _write_json(
                    os.path.join(self._dir(atespace, i), "meta.json"),
                    {"status": SUSPENDED},
                )
            return [self.env(i, atespace=atespace) for i in ids]
        finally:
            os.unlink(op)


class ForklessClient(FakeClient):
    """A client of an SDK without ForkEnvironment."""

    def __getattribute__(self, name):
        if name == "fork":
            raise AttributeError(name)
        return super().__getattribute__(name)


def fake_client_factory(settings):
    """``RAY_SUBSTRATE_CLIENT_FACTORY`` entry point."""
    cls = ForklessClient if os.environ.get("SUBSTRATE_FAKE_FORKLESS") else FakeClient
    return cls(
        os.environ["SUBSTRATE_FAKE_ROOT"],
        float(os.environ.get("SUBSTRATE_FAKE_LATENCY_S", "0")),
    )
