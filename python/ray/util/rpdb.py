# Some code in this file is from
# https://github.com/ionelmc/python-remote-pdb/blob/07d563331c4ab9eb45731bb272b158816d98236e/src/remote_pdb.py
# (BSD 2-Clause "Simplified" License)

import errno
import inspect
import json
import logging
import os
import re
import select
import socket
import sys
import uuid
from pdb import Pdb
import setproctitle
import traceback

import ray
from ray.experimental.internal_kv import _internal_kv_del, _internal_kv_put

PY3 = sys.version_info[0] == 3
log = logging.getLogger(__name__)


def cry(message, stderr=sys.__stderr__):
    print(message, file=stderr)
    stderr.flush()


class LF2CRLF_FileWrapper(object):
    def __init__(self, connection):
        self.connection = connection
        self.stream = fh = connection.makefile("rw")
        self.read = fh.read
        self.readline = fh.readline
        self.readlines = fh.readlines
        self.close = fh.close
        self.flush = fh.flush
        self.fileno = fh.fileno
        if hasattr(fh, "encoding"):
            self._send = lambda data: connection.sendall(
                data.encode(fh.encoding))
        else:
            self._send = connection.sendall

    @property
    def encoding(self):
        return self.stream.encoding

    def __iter__(self):
        return self.stream.__iter__()

    def write(self, data, nl_rex=re.compile("\r?\n")):
        data = nl_rex.sub("\r\n", data)
        self._send(data)

    def writelines(self, lines, nl_rex=re.compile("\r?\n")):
        for line in lines:
            self.write(line, nl_rex)


class RemotePdb(Pdb):
    """
    This will run pdb as a ephemeral telnet service. Once you connect no one
    else can connect. On construction this object will block execution till a
    client has connected.
    Based on https://github.com/tamentis/rpdb I think ...
    To use this::
        RemotePdb(host="0.0.0.0", port=4444).set_trace()
    Then run: telnet 127.0.0.1 4444
    """
    active_instance = None

    def __init__(self,
                 breakpoint_uuid,
                 host,
                 port,
                 patch_stdstreams=False,
                 quiet=False):
        self._breakpoint_uuid = breakpoint_uuid
        self._quiet = quiet
        self._patch_stdstreams = patch_stdstreams
        self._listen_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._listen_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR,
                                       True)
        self._listen_socket.bind((host, port))

    def listen(self):
        if not self._quiet:
            cry("RemotePdb session open at %s:%s, "
                "use 'ray debug' to connect..." %
                self._listen_socket.getsockname())
        self._listen_socket.listen(1)
        connection, address = self._listen_socket.accept()
        if not self._quiet:
            cry("RemotePdb accepted connection from %s." % repr(address))
        self.handle = LF2CRLF_FileWrapper(connection)
        Pdb.__init__(
            self, completekey="tab", stdin=self.handle, stdout=self.handle)
        self.backup = []
        if self._patch_stdstreams:
            for name in (
                    "stderr",
                    "stdout",
                    "__stderr__",
                    "__stdout__",
                    "stdin",
                    "__stdin__",
            ):
                self.backup.append((name, getattr(sys, name)))
                setattr(sys, name, self.handle)
        RemotePdb.active_instance = self

    def __restore(self):
        if self.backup and not self._quiet:
            cry("Restoring streams: %s ..." % self.backup)
        for name, fh in self.backup:
            setattr(sys, name, fh)
        self.handle.close()
        RemotePdb.active_instance = None

    def do_quit(self, arg):
        self.__restore()
        return Pdb.do_quit(self, arg)

    do_q = do_exit = do_quit

    def do_continue(self, arg):
        self.__restore()
        self.handle.connection.close()
        return Pdb.do_continue(self, arg)

    do_c = do_continue

    def set_trace(self, frame=None):
        if frame is None:
            frame = sys._getframe().f_back
        try:
            Pdb.set_trace(self, frame)
        except IOError as exc:
            if exc.errno != errno.ECONNRESET:
                raise

    def post_mortem(self, traceback=None):
        # See https://github.com/python/cpython/blob/
        # 022bc7572f061e1d1132a4db9d085b29707701e7/Lib/pdb.py#L1617
        try:
            t = sys.exc_info()[2]
            self.reset()
            Pdb.interaction(self, None, t)
        except IOError as exc:
            if exc.errno != errno.ECONNRESET:
                raise

    def do_remote(self, arg):
        """remote
        Skip into the next remote call.
        """
        # Tell the next task to drop into the debugger.
        ray.worker.global_worker.debugger_breakpoint = self._breakpoint_uuid
        # Tell the debug loop to connect to the next task.
        _internal_kv_put("RAY_PDB_CONTINUE_{}".format(self._breakpoint_uuid),
                         "")
        self.__restore()
        self.handle.connection.close()
        return Pdb.do_continue(self, arg)

    def do_get(self, arg):
        """get
        Skip to where the current task returns to.
        """
        ray.worker.global_worker.debugger_get_breakpoint = (
            self._breakpoint_uuid)
        self.__restore()
        self.handle.connection.close()
        return Pdb.do_continue(self, arg)


def connect_ray_pdb(host=None,
                    port=None,
                    patch_stdstreams=False,
                    quiet=None,
                    breakpoint_uuid=None):
    """
    Opens a remote PDB on first available port.
    """
    if host is None:
        host = os.environ.get("REMOTE_PDB_HOST", "127.0.0.1")
    if port is None:
        port = int(os.environ.get("REMOTE_PDB_PORT", "0"))
    if quiet is None:
        quiet = bool(os.environ.get("REMOTE_PDB_QUIET", ""))
    if not breakpoint_uuid:
        breakpoint_uuid = uuid.uuid4().hex
    rdb = RemotePdb(
        breakpoint_uuid=breakpoint_uuid,
        host=host,
        port=port,
        patch_stdstreams=patch_stdstreams,
        quiet=quiet)
    sockname = rdb._listen_socket.getsockname()
    pdb_address = "{}:{}".format(sockname[0], sockname[1])
    parentframeinfo = inspect.getouterframes(inspect.currentframe())[2]
    data = {
        "proctitle": setproctitle.getproctitle(),
        "pdb_address": pdb_address,
        "filename": parentframeinfo.filename,
        "lineno": parentframeinfo.lineno,
        "traceback": "\n".join(traceback.format_exception(*sys.exc_info()))
    }
    _internal_kv_put(
        "RAY_PDB_{}".format(breakpoint_uuid), json.dumps(data), overwrite=True)
    rdb.listen()
    _internal_kv_del("RAY_PDB_{}".format(breakpoint_uuid))

    return rdb


def set_trace(breakpoint_uuid=None):
    """Interrupt the flow of the program and drop into the Ray debugger.

    Can be used within a Ray task or actor.
    """
    # If there is an active debugger already, we do not want to
    # start another one, so "set_trace" is just a no-op in that case.
    if ray.worker.global_worker.debugger_breakpoint == b"":
        frame = sys._getframe().f_back
        rdb = connect_ray_pdb(
            None, None, False, None,
            breakpoint_uuid.decode() if breakpoint_uuid else None)
        rdb.set_trace(frame=frame)


def post_mortem():
    rdb = connect_ray_pdb(None, None, False, None)
    rdb.post_mortem()


def connect_pdb_client(host, port):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((host, port))

    while True:
        # Get the list of sockets which are readable.
        read_sockets, write_sockets, error_sockets = select.select(
            [sys.stdin, s], [], [])

        for sock in read_sockets:
            if sock == s:
                # Incoming message from remote debugger.
                data = sock.recv(4096)
                if not data:
                    return
                else:
                    sys.stdout.write(data.decode())
                    sys.stdout.flush()
            else:
                # User entered a message.
                msg = sys.stdin.readline()
                s.send(msg.encode())
