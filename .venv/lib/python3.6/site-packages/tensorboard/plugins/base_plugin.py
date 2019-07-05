# Copyright 2016 The TensorFlow Authors. All Rights Reserved.
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
"""TensorBoard Plugin abstract base class.

Every plugin in TensorBoard must extend and implement the abstract methods of
this base class.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from abc import ABCMeta
from abc import abstractmethod


class TBPlugin(object):
  """TensorBoard plugin interface.

  Every plugin must extend from this class.

  Subclasses must have a trivial constructor that takes a TBContext
  argument. Any operation that might throw an exception should either be
  done lazily or made safe with a TBLoader subclass, so the plugin won't
  negatively impact the rest of TensorBoard.

  Fields:
    plugin_name: The plugin_name will also be a prefix in the http
      handlers, e.g. `data/plugins/$PLUGIN_NAME/$HANDLER` The plugin
      name must be unique for each registered plugin, or a ValueError
      will be thrown when the application is constructed. The plugin
      name must only contain characters among [A-Za-z0-9_.-], and must
      be nonempty, or a ValueError will similarly be thrown.
  """
  __metaclass__ = ABCMeta

  plugin_name = None

  @abstractmethod
  def get_plugin_apps(self):
    """Returns a set of WSGI applications that the plugin implements.

    Each application gets registered with the tensorboard app and is served
    under a prefix path that includes the name of the plugin.

    Returns:
      A dict mapping route paths to WSGI applications. Each route path
      should include a leading slash.
    """
    raise NotImplementedError()

  @abstractmethod
  def is_active(self):
    """Determines whether this plugin is active.

    A plugin may not be active for instance if it lacks relevant data. If a
    plugin is inactive, the frontend may avoid issuing requests to its routes.

    Returns:
      A boolean value. Whether this plugin is active.
    """
    raise NotImplementedError()


class TBContext(object):
  """Magic container of information passed from TensorBoard core to plugins.

  A TBContext instance is passed to the constructor of a TBPlugin class. Plugins
  are strongly encouraged to assume that any of these fields can be None. In
  cases when a field is considered mandatory by a plugin, it can either crash
  with ValueError, or silently choose to disable itself by returning False from
  its is_active method.

  All fields in this object are thread safe.
  """

  def __init__(
      self,
      assets_zip_provider=None,
      db_connection_provider=None,
      db_module=None,
      db_uri=None,
      flags=None,
      logdir=None,
      multiplexer=None,
      plugin_name_to_instance=None,
      window_title=None):
    """Instantiates magic container.

    The argument list is sorted and may be extended in the future; therefore,
    callers must pass only named arguments to this constructor.

    Args:
      assets_zip_provider: A function that returns a newly opened file handle
          for a zip file containing all static assets. The file names inside the
          zip file are considered absolute paths on the web server. The file
          handle this function returns must be closed. It is assumed that you
          will pass this file handle to zipfile.ZipFile. This zip file should
          also have been created by the tensorboard_zip_file build rule.
      db_connection_provider: Function taking no arguments that returns a
          PEP-249 database Connection object, or None if multiplexer should be
          used instead. The returned value must be closed, and is safe to use in
          a `with` statement. It is also safe to assume that calling this
          function is cheap. The returned connection must only be used by a
          single thread. Things like connection pooling are considered
          implementation details of the provider.
      db_module: A PEP-249 DB Module, e.g. sqlite3. This is useful for accessing
          things like date time constructors. This value will be None if we are
          not in SQL mode and multiplexer should be used instead.
      db_uri: The string db URI TensorBoard was started with. If this is set,
          the logdir should be None.
      flags: An object of the runtime flags provided to TensorBoard to their
          values.
      logdir: The string logging directory TensorBoard was started with. If this
          is set, the db_uri should be None.
      multiplexer: An EventMultiplexer with underlying TB data. Plugins should
          copy this data over to the database when the db fields are set.
      plugin_name_to_instance: A mapping between plugin name to instance.
          Plugins may use this property to access other plugins. The context
          object is passed to plugins during their construction, so a given
          plugin may be absent from this mapping until it is registered. Plugin
          logic should handle cases in which a plugin is absent from this
          mapping, lest a KeyError is raised.
      window_title: A string specifying the window title.
    """
    self.assets_zip_provider = assets_zip_provider
    self.db_connection_provider = db_connection_provider
    self.db_module = db_module
    self.db_uri = db_uri
    self.flags = flags
    self.logdir = logdir
    self.multiplexer = multiplexer
    self.plugin_name_to_instance = plugin_name_to_instance
    self.window_title = window_title


class TBLoader(object):
  """TBPlugin factory base class.

  Plugins can override this class to customize how a plugin is loaded at
  startup. This might entail adding command-line arguments, checking if
  optional dependencies are installed, and potentially also specializing
  the plugin class at runtime.

  When plugins use optional dependencies, the loader needs to be
  specified in its own module. That way it's guaranteed to be
  importable, even if the `TBPlugin` itself can't be imported.

  Subclasses must have trivial constructors.
  """

  def define_flags(self, parser):
    """Adds plugin-specific CLI flags to parser.

    The default behavior is to do nothing.

    When overriding this method, it's recommended that plugins call the
    `parser.add_argument_group(plugin_name)` method for readability. No
    flags should be specified that would cause `parse_args([])` to fail.

    Args:
      parser: The argument parsing object, which may be mutated.
    """
    pass

  def fix_flags(self, flags):
    """Allows flag values to be corrected or validated after parsing.

    Args:
      flags: The parsed argparse.Namespace object.
    """
    pass


class BasicLoader(TBLoader):
  """Simple TBLoader that's sufficient for most plugins."""

  def __init__(self, plugin_class):
    """Creates simple plugin instance maker.

    :param plugin_class: :class:`TBPlugin`
    """
    self._plugin_class = plugin_class

  def load(self, context):
    return self._plugin_class(context)
