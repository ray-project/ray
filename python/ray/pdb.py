from __future__ import print_function

import errno
import logging
import os
import re
import socket
import sys
from pdb import Pdb


PY3 = sys.version_info[0] == 3
log = logging.getLogger(__name__)


def cry(message, stderr=sys.__stderr__):
    log.critical(message)
    print(message, file=stderr)
    stderr.flush()


class LF2CRLF_FileWrapper(object):
    def __init__(self, connection):
        self.connection = connection
        self.stream = fh = connection.makefile('rw')
        self.read = fh.read
        self.readline = fh.readline
        self.readlines = fh.readlines
        self.close = fh.close
        self.flush = fh.flush
        self.fileno = fh.fileno
        if hasattr(fh, 'encoding'):
            self._send = lambda data: connection.sendall(data.encode(fh.encoding))
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
        RemotePdb(host='0.0.0.0', port=4444).set_trace()
    Then run: telnet 127.0.0.1 4444
    """
    active_instance = None

    def __init__(self, host, port, patch_stdstreams=False, quiet=False):
        self._quiet = quiet
        listen_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listen_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, True)
        listen_socket.bind((host, port))
        if not self._quiet:
            cry("RemotePdb session open at %s:%s, waiting for connection ..." % listen_socket.getsockname())
        listen_socket.listen(1)
        connection, address = listen_socket.accept()
        if not self._quiet:
            cry("RemotePdb accepted connection from %s." % repr(address))
        self.handle = LF2CRLF_FileWrapper(connection)
        Pdb.__init__(self, completekey='tab', stdin=self.handle, stdout=self.handle)
        self.backup = []
        if patch_stdstreams:
            for name in (
                    'stderr',
                    'stdout',
                    '__stderr__',
                    '__stdout__',
                    'stdin',
                    '__stdin__',
            ):
                self.backup.append((name, getattr(sys, name)))
                setattr(sys, name, self.handle)
        RemotePdb.active_instance = self

    def __restore(self):
        if self.backup and not self._quiet:
            cry('Restoring streams: %s ...' % self.backup)
        for name, fh in self.backup:
            setattr(sys, name, fh)
        self.handle.close()
        RemotePdb.active_instance = None

    def do_quit(self, arg):
        self.__restore()
        return Pdb.do_quit(self, arg)

    do_q = do_exit = do_quit

    def set_trace(self, frame=None):
        if frame is None:
            frame = sys._getframe().f_back
        try:
            Pdb.set_trace(self, frame)
        except IOError as exc:
            if exc.errno != errno.ECONNRESET:
                raise


def set_trace(host=None, port=None, patch_stdstreams=False, quiet=None):
    """
    Opens a remote PDB on first available port.
    """
    if host is None:
        host = os.environ.get('REMOTE_PDB_HOST', '127.0.0.1')
    if port is None:
        port = int(os.environ.get('REMOTE_PDB_PORT', '0'))
    if quiet is None:
        quiet = bool(os.environ.get('REMOTE_PDB_QUIET', ''))
    rdb = RemotePdb(host=host, port=port, patch_stdstreams=patch_stdstreams, quiet=quiet)
    rdb.set_trace(frame=sys._getframe().f_back)