# Copyright 2017 The TensorFlow Authors. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ==============================================================================
"""Utilities for TensorBoard command line program.

This is a lightweight module for bringing up a TensorBoard HTTP server
or emulating the `tensorboard` shell command.

Those wishing to create custom builds of TensorBoard can use this module
by swapping out `tensorboard.main` with the custom definition that
modifies the set of plugins and static assets.

This module does not depend on first-party plugins or the default web
server assets. Those are defined in `tensorboard.default`.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from abc import ABCMeta
from abc import abstractmethod
import argparse
import atexit
from collections import defaultdict
import datetime
import errno
import os
import signal
import socket
import sys
import threading
import inspect

import six
from six.moves import urllib
from six.moves import xrange  # pylint: disable=redefined-builtin
from werkzeug import serving

from tensorboard import manager
from tensorboard import version
from tensorboard.backend import application
from tensorboard.backend.event_processing import event_file_inspector as efi
from tensorboard.plugins import base_plugin
from tensorboard.plugins.core import core_plugin
from tensorboard.util import tb_logging
from tensorboard.util import util

try:
  from absl import flags as absl_flags
  from absl.flags import argparse_flags
except ImportError:
  # Fall back to argparse with no absl flags integration.
  absl_flags = None
  argparse_flags = argparse

logger = tb_logging.get_logger()


def setup_environment():
  """Makes recommended modifications to the environment.

  This functions changes global state in the Python process. Calling
  this function is a good idea, but it can't appropriately be called
  from library routines.
  """
  util.setup_logging()

  # The default is HTTP/1.0 for some strange reason. If we don't use
  # HTTP/1.1 then a new TCP socket and Python thread is created for
  # each HTTP request. The tradeoff is we must always specify the
  # Content-Length header, or do chunked encoding for streaming.
  serving.WSGIRequestHandler.protocol_version = 'HTTP/1.1'

def get_default_assets_zip_provider():
  """Opens stock TensorBoard web assets collection.

  Returns:
    Returns function that returns a newly opened file handle to zip file
    containing static assets for stock TensorBoard, or None if webfiles.zip
    could not be found. The value the callback returns must be closed. The
    paths inside the zip file are considered absolute paths on the web server.
  """
  path = os.path.join(os.path.dirname(inspect.getfile(sys._getframe(1))),
                      'webfiles.zip')
  if not os.path.exists(path):
    logger.warning('webfiles.zip static assets not found: %s', path)
    return None
  return lambda: open(path, 'rb')

class TensorBoard(object):
  """Class for running TensorBoard.

  Fields:
    plugin_loaders: Set from plugins passed to constructor.
    assets_zip_provider: Set by constructor.
    server_class: Set by constructor.
    flags: An argparse.Namespace set by the configure() method.
    cache_key: As `manager.cache_key`; set by the configure() method.
  """

  def __init__(self,
               plugins=None,
               assets_zip_provider=None,
               server_class=None):
    """Creates new instance.

    Args:
      plugins: A list of TensorBoard plugins to load, as TBLoader instances or
        TBPlugin classes. If not specified, defaults to first-party plugins.
      assets_zip_provider: Delegates to TBContext or uses default if None.
      server_class: An optional subclass of TensorBoardServer to use for serving
        the TensorBoard WSGI app.

    :type plugins: list[Union[base_plugin.TBLoader, Type[base_plugin.TBPlugin]]]
    :type assets_zip_provider: () -> file
    :type server_class: class
    """
    if plugins is None:
      from tensorboard import default
      plugins = default.get_plugins()
    if assets_zip_provider is None:
      assets_zip_provider = get_default_assets_zip_provider()
    if server_class is None:
      server_class = WerkzeugServer
    def make_loader(plugin):
      if isinstance(plugin, base_plugin.TBLoader):
        return plugin
      if issubclass(plugin, base_plugin.TBPlugin):
        return base_plugin.BasicLoader(plugin)
      raise ValueError("Not a TBLoader or TBPlugin subclass: %s" % plugin)
    self.plugin_loaders = [make_loader(p) for p in plugins]
    self.assets_zip_provider = assets_zip_provider
    self.server_class = server_class
    self.flags = None

  def configure(self, argv=('',), **kwargs):
    """Configures TensorBoard behavior via flags.

    This method will populate the "flags" property with an argparse.Namespace
    representing flag values parsed from the provided argv list, overriden by
    explicit flags from remaining keyword arguments.

    Args:
      argv: Can be set to CLI args equivalent to sys.argv; the first arg is
        taken to be the name of the path being executed.
      kwargs: Additional arguments will override what was parsed from
        argv. They must be passed as Python data structures, e.g.
        `foo=1` rather than `foo="1"`.

    Returns:
      Either argv[:1] if argv was non-empty, or [''] otherwise, as a mechanism
      for absl.app.run() compatibility.

    Raises:
      ValueError: If flag values are invalid.
    """
    parser = argparse_flags.ArgumentParser(
        prog='tensorboard',
        description=('TensorBoard is a suite of web applications for '
                     'inspecting and understanding your TensorFlow runs '
                     'and graphs. https://github.com/tensorflow/tensorboard '))
    for loader in self.plugin_loaders:
      loader.define_flags(parser)
    arg0 = argv[0] if argv else ''
    flags = parser.parse_args(argv[1:])  # Strip binary name from argv.
    self.cache_key = manager.cache_key(
        working_directory=os.getcwd(),
        arguments=argv[1:],
        configure_kwargs=kwargs,
    )
    if absl_flags and arg0:
      # Only expose main module Abseil flags as TensorBoard native flags.
      # This is the same logic Abseil's ArgumentParser uses for determining
      # which Abseil flags to include in the short helpstring.
      for flag in set(absl_flags.FLAGS.get_key_flags_for_module(arg0)):
        if hasattr(flags, flag.name):
          raise ValueError('Conflicting Abseil flag: %s' % flag.name)
        setattr(flags, flag.name, flag.value)
    for k, v in kwargs.items():
      if not hasattr(flags, k):
        raise ValueError('Unknown TensorBoard flag: %s' % k)
      setattr(flags, k, v)
    for loader in self.plugin_loaders:
      loader.fix_flags(flags)
    self.flags = flags
    return [arg0]

  def main(self, ignored_argv=('',)):
    """Blocking main function for TensorBoard.

    This method is called by `tensorboard.main.run_main`, which is the
    standard entrypoint for the tensorboard command line program. The
    configure() method must be called first.

    Args:
      ignored_argv: Do not pass. Required for Abseil compatibility.

    Returns:
      Process exit code, i.e. 0 if successful or non-zero on failure. In
      practice, an exception will most likely be raised instead of
      returning non-zero.

    :rtype: int
    """
    self._install_signal_handler(signal.SIGTERM, "SIGTERM")
    if self.flags.inspect:
      logger.info('Not bringing up TensorBoard, but inspecting event files.')
      event_file = os.path.expanduser(self.flags.event_file)
      efi.inspect(self.flags.logdir, event_file, self.flags.tag)
      return 0
    try:
      server = self._make_server()
      sys.stderr.write('TensorBoard %s at %s (Press CTRL+C to quit)\n' %
                       (version.VERSION, server.get_url()))
      sys.stderr.flush()
      self._register_info(server)
      server.serve_forever()
      return 0
    except TensorBoardServerException as e:
      logger.error(e.msg)
      sys.stderr.write('ERROR: %s\n' % e.msg)
      sys.stderr.flush()
      return -1

  def launch(self):
    """Python API for launching TensorBoard.

    This method is the same as main() except it launches TensorBoard in
    a separate permanent thread. The configure() method must be called
    first.

    Returns:
      The URL of the TensorBoard web server.

    :rtype: str
    """
    # Make it easy to run TensorBoard inside other programs, e.g. Colab.
    server = self._make_server()
    thread = threading.Thread(target=server.serve_forever, name='TensorBoard')
    thread.daemon = True
    thread.start()
    return server.get_url()

  def _register_info(self, server):
    """Write a TensorBoardInfo file and arrange for its cleanup.

    Args:
      server: The result of `self._make_server()`.
    """
    server_url = urllib.parse.urlparse(server.get_url())
    info = manager.TensorBoardInfo(
        version=version.VERSION,
        start_time=datetime.datetime.now(),
        port=server_url.port,
        pid=os.getpid(),
        path_prefix=self.flags.path_prefix,
        logdir=self.flags.logdir,
        db=self.flags.db,
        cache_key=self.cache_key,
    )
    atexit.register(manager.remove_info_file)
    manager.write_info_file(info)

  def _install_signal_handler(self, signal_number, signal_name):
    """Set a signal handler to gracefully exit on the given signal.

    When this process receives the given signal, it will run `atexit`
    handlers and then exit with `0`.

    Args:
      signal_number: The numeric code for the signal to handle, like
        `signal.SIGTERM`.
      signal_name: The human-readable signal name.
    """
    old_signal_handler = None  # set below
    def handler(handled_signal_number, frame):
      # In case we catch this signal again while running atexit
      # handlers, take the hint and actually die.
      signal.signal(signal_number, signal.SIG_DFL)
      sys.stderr.write("TensorBoard caught %s; exiting...\n" % signal_name)
      # The main thread is the only non-daemon thread, so it suffices to
      # exit hence.
      if old_signal_handler not in (signal.SIG_IGN, signal.SIG_DFL):
        old_signal_handler(handled_signal_number, frame)
      sys.exit(0)
    old_signal_handler = signal.signal(signal_number, handler)


  def _make_server(self):
    """Constructs the TensorBoard WSGI app and instantiates the server."""
    app = application.standard_tensorboard_wsgi(self.flags,
                                                self.plugin_loaders,
                                                self.assets_zip_provider)
    return self.server_class(app, self.flags)


class TensorBoardServer(object):
  """Class for customizing TensorBoard WSGI app serving."""
  __metaclass__ = ABCMeta

  @abstractmethod
  def __init__(self, wsgi_app, flags):
    """Create a flag-configured HTTP server for TensorBoard's WSGI app.

    Args:
      wsgi_app: The TensorBoard WSGI application to create a server for.
      flags: argparse.Namespace instance of TensorBoard flags.
    """
    raise NotImplementedError()

  @abstractmethod
  def serve_forever(self):
    """Blocking call to start serving the TensorBoard server."""
    raise NotImplementedError()

  @abstractmethod
  def get_url(self):
    """Returns a URL at which this server should be reachable."""
    raise NotImplementedError()


class TensorBoardServerException(Exception):
  """Exception raised by TensorBoardServer for user-friendly errors.

  Subclasses of TensorBoardServer can raise this exception in order to
  generate a clean error message for the user rather than a stacktrace.
  """
  def __init__(self, msg):
    self.msg = msg


class WerkzeugServer(serving.ThreadedWSGIServer, TensorBoardServer):
  """Implementation of TensorBoardServer using the Werkzeug dev server."""
  # ThreadedWSGIServer handles this in werkzeug 0.12+ but we allow 0.11.x.
  daemon_threads = True

  def __init__(self, wsgi_app, flags):
    self._flags = flags
    host = flags.host

    # base_port: what's the first port to which we should try to bind?
    # should_scan: if that fails, shall we try additional ports?
    # max_attempts: how many ports shall we try?
    should_scan = flags.port is None
    base_port = core_plugin.DEFAULT_PORT if flags.port is None else flags.port
    max_attempts = 10 if should_scan else 1

    if base_port > 0xFFFF:
      raise TensorBoardServerException(
          'TensorBoard cannot bind to port %d > %d' % (base_port, 0xFFFF)
      )
    max_attempts = 10 if should_scan else 1
    base_port = min(base_port + max_attempts, 0x10000) - max_attempts

    # Without an explicit host, we default to serving on all interfaces,
    # and will attempt to serve both IPv4 and IPv6 traffic through one
    # socket.
    self._auto_wildcard = not host

    for (attempt_index, port) in (
        enumerate(xrange(base_port, base_port + max_attempts))):
      if self._auto_wildcard:
        host = self._get_wildcard_address(port)
      try:
        # Yes, this invokes the super initializer potentially many
        # times. This seems to work fine, and looking at the superclass
        # chain (type(self).__mro__) it doesn't seem that anything
        # _should_ go wrong (nor does any superclass provide a facility
        # to do this natively).
        #
        # TODO(@wchargin): Refactor so that this is no longer necessary,
        # probably by switching composing around a Werkzeug server
        # rather than inheriting from it.
        super(WerkzeugServer, self).__init__(host, port, wsgi_app)
        break
      except socket.error as e:
        if hasattr(errno, 'EACCES') and e.errno == errno.EACCES:
          raise TensorBoardServerException(
              'TensorBoard must be run as superuser to bind to port %d' %
              port)
        elif hasattr(errno, 'EADDRINUSE') and e.errno == errno.EADDRINUSE:
          if attempt_index < max_attempts - 1:
            continue
          if port == 0:
            raise TensorBoardServerException(
                'TensorBoard unable to find any open port')
          elif should_scan:
            raise TensorBoardServerException(
                'TensorBoard could not bind to any port around %s '
                '(tried %d times)'
                % (base_port, max_attempts))
          else:
            raise TensorBoardServerException(
                'TensorBoard could not bind to port %d, it was already in use' %
                port)
        elif hasattr(errno, 'EADDRNOTAVAIL') and e.errno == errno.EADDRNOTAVAIL:
          raise TensorBoardServerException(
              'TensorBoard could not bind to unavailable address %s' % host)
        elif hasattr(errno, 'EAFNOSUPPORT') and e.errno == errno.EAFNOSUPPORT:
          raise TensorBoardServerException(
              'Tensorboard could not bind to unsupported address family %s' %
              host)
        # Raise the raw exception if it wasn't identifiable as a user error.
        raise

  def _get_wildcard_address(self, port):
    """Returns a wildcard address for the port in question.

    This will attempt to follow the best practice of calling getaddrinfo() with
    a null host and AI_PASSIVE to request a server-side socket wildcard address.
    If that succeeds, this returns the first IPv6 address found, or if none,
    then returns the first IPv4 address. If that fails, then this returns the
    hardcoded address "::" if socket.has_ipv6 is True, else "0.0.0.0".
    """
    fallback_address = '::' if socket.has_ipv6 else '0.0.0.0'
    if hasattr(socket, 'AI_PASSIVE'):
      try:
        addrinfos = socket.getaddrinfo(None, port, socket.AF_UNSPEC,
                                       socket.SOCK_STREAM, socket.IPPROTO_TCP,
                                       socket.AI_PASSIVE)
      except socket.gaierror as e:
        logger.warn('Failed to auto-detect wildcard address, assuming %s: %s',
                    fallback_address, str(e))
        return fallback_address
      addrs_by_family = defaultdict(list)
      for family, _, _, _, sockaddr in addrinfos:
        # Format of the "sockaddr" socket address varies by address family,
        # but [0] is always the IP address portion.
        addrs_by_family[family].append(sockaddr[0])
      if hasattr(socket, 'AF_INET6') and addrs_by_family[socket.AF_INET6]:
        return addrs_by_family[socket.AF_INET6][0]
      if hasattr(socket, 'AF_INET') and addrs_by_family[socket.AF_INET]:
        return addrs_by_family[socket.AF_INET][0]
    logger.warn('Failed to auto-detect wildcard address, assuming %s',
                fallback_address)
    return fallback_address

  def server_bind(self):
    """Override to enable IPV4 mapping for IPV6 sockets when desired.

    The main use case for this is so that when no host is specified, TensorBoard
    can listen on all interfaces for both IPv4 and IPv6 connections, rather than
    having to choose v4 or v6 and hope the browser didn't choose the other one.
    """
    socket_is_v6 = (
        hasattr(socket, 'AF_INET6') and self.socket.family == socket.AF_INET6)
    has_v6only_option = (
        hasattr(socket, 'IPPROTO_IPV6') and hasattr(socket, 'IPV6_V6ONLY'))
    if self._auto_wildcard and socket_is_v6 and has_v6only_option:
      try:
        self.socket.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 0)
      except socket.error as e:
        # Log a warning on failure to dual-bind, except for EAFNOSUPPORT
        # since that's expected if IPv4 isn't supported at all (IPv6-only).
        if hasattr(errno, 'EAFNOSUPPORT') and e.errno != errno.EAFNOSUPPORT:
          logger.warn('Failed to dual-bind to IPv4 wildcard: %s', str(e))
    super(WerkzeugServer, self).server_bind()

  def handle_error(self, request, client_address):
    """Override to get rid of noisy EPIPE errors."""
    del request  # unused
    # Kludge to override a SocketServer.py method so we can get rid of noisy
    # EPIPE errors. They're kind of a red herring as far as errors go. For
    # example, `curl -N http://localhost:6006/ | head` will cause an EPIPE.
    exc_info = sys.exc_info()
    e = exc_info[1]
    if isinstance(e, IOError) and e.errno == errno.EPIPE:
      logger.warn('EPIPE caused by %s in HTTP serving' % str(client_address))
    else:
      logger.error('HTTP serving error', exc_info=exc_info)

  def get_url(self):
    if self._auto_wildcard:
      display_host = socket.gethostname()
    else:
      host = self._flags.host
      display_host = (
          '[%s]' % host if ':' in host and not host.startswith('[') else host)
    return 'http://%s:%d%s' % (display_host, self.server_port,
                               self._flags.path_prefix)
