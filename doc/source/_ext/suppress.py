#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Following the discussion in:
# https://github.com/sphinx-doc/sphinx/issues/11325
#
# author: picnixz
# license: MIT

r"""
A filter suppressing logging records issued by a Sphinx logger.

The records are filtered according to their context (logger name and record
level) and their formatted message.

Typical usage::

    suppress_loggers = {
        'sphinx.ext.autodoc': True                  # suppress logger
        'sphinx.ext.intersphinx': ['INFO', 'ERROR'] # specific levels
    }

    suppress_records = [
        'error: .+',
        ['sphinx.ext.intersphinx', '.*Name or service not known$']
    ]

Note that Sphinx automatically adds ``sphinx.`` to the logger names when
instantiating a logger adapter through :func:`sphinx.util.logging.getLogger`.

In particular, *all* Sphinx-related modules and third-party extensions are assumed
to do the same. Use ``EXTENSION`` and not ``sphinx.EXTENSION`` to suppress the
logger associated with the named extension (e.g., ``sphinx.ext.intersphinx`` to
suppress the logger declared in the :mod:`sphinx.ext.intersphinx` module).

.. confval:: suppress_loggers = {}

    A dictionary describing which logger to suppress, possibly partially.

    .. code-block::

        # suppress messages from 'sphinx.ext.autodoc'
        suppress_loggers = {'sphinx.ext.autodoc': True}

        # suppress INFO and ERROR messages from 'sphinx.ext.autodoc'
        suppress_loggers = {'sphinx.ext.autodoc': ['INFO', 'ERROR']}

.. confval:: suppress_protect = []

    A list of module names that are known to contain a Sphinx logger but
    that will never be suppressed automatically. This is typically useful
    when an extension contains submodules declaring loggers which, when
    imported, result in undesirable side-effects.

.. confval:: suppress_records = []

    A list of message patterns to suppress, possibly filtered by logger.

    .. code-block::

        # suppress messages matching 'error: .*' and 'warning: .*'
        suppress_records = ['error: .*', 'warning: .*']

        # suppress messages issued by 'sphinx.ext.intersphinx'
        suppress_records = [('sphinx.ext.intersphinx', '.*')]
"""

__all__ = ()

import abc
import importlib
import inspect
import itertools
import logging
import pkgutil
import re
import warnings
from typing import TYPE_CHECKING

from sphinx.util.logging import NAMESPACE, SphinxLoggerAdapter

if TYPE_CHECKING:
    from sphinx.application import Sphinx
    from sphinx.config import Config
    from sphinx.extension import Extension


def partition(predicate, iterable):
    """
    Return a pair `(no, yes)`, where *yes* and *no* are subsets of *iterable*
    over which *predicate* evaluates to |False| and |True| respectively.
    """

    no, yes = itertools.tee(iterable)
    no, yes = itertools.filterfalse(predicate, no), filter(predicate, yes)
    return no, yes


def notnone(value):
    return value is not None


ALL = object()


def _normalize_level(level):
    if isinstance(level, int):
        return level

    # pylint: disable-next=W0212
    result = logging._nameToLevel.get(level)
    if result is not None:
        return result

    return None  # unknown level


def _parse_levels(levels):
    if not isinstance(levels, (list, tuple)):
        if isinstance(levels, (int, str)):
            levels = [levels]
    return list(filter(notnone, map(_normalize_level, levels)))


class SphinxSuppressFilter(logging.Filter, metaclass=abc.ABCMeta):
    def filter(self, record):
        # type: (logging.LogRecord) -> bool
        return not self.suppressed(record)

    @abc.abstractmethod
    def suppressed(self, record):
        # type: (logging.LogRecord) -> bool
        pass


class SphinxSuppressLogger(SphinxSuppressFilter):
    r"""A filter suppressing logging records issued by a Sphinx logger."""

    def __init__(self, name, levels=()):
        """
        Construct a :class:`SphinxSuppressLogger`.

        :param name: The (real) logger name to suppress.
        :type name: str
        :param levels: Optional logging levels to suppress.
        :type levels: bool | list[int]
        """

        super().__init__(name)
        if isinstance(levels, bool):
            self.levels = ALL if levels else []
        else:
            self.levels = _parse_levels(levels)

    def suppressed(self, record):
        return (
            logging.Filter.filter(self, record)
            and (self.levels is ALL or record.levelno in self.levels)
        )


class SphinxSuppressPatterns(SphinxSuppressFilter):
    r"""A filter suppressing matching messages."""

    def __init__(self, patterns=()):
        """
        Construct a :class:`SphinxSuppressPatterns`.

        :param patterns: Optional logging messages (regex) to suppress.
        :type patterns: list[str | re.Pattern]
        """

        super().__init__('')  # all loggers
        self.patterns = set(map(re.compile, patterns))

    def suppressed(self, record):
        m = record.getMessage()
        return self.patterns and any(p.search(m) for p in self.patterns)


class SphinxSuppressRecord(SphinxSuppressLogger, SphinxSuppressPatterns):
    r"""A filter suppressing matching messages by logger's name pattern."""

    def __init__(self, name, levels=(), patterns=()):
        """
        Construct a :class:`SphinxSuppressRecord` filter.

        :param name: A logger's name to suppress.
        :type name: str
        :param levels: Optional logging levels to suppress.
        :type levels: bool | list[int]
        :param patterns: Optional logging messages (regex) to suppress.
        :type patterns: list[str | re.Pattern]
        """

        SphinxSuppressLogger.__init__(self, name, levels)
        SphinxSuppressPatterns.__init__(self, patterns)

    def suppressed(self, record):
        return (
            SphinxSuppressLogger.suppressed(self, record)
            and SphinxSuppressPatterns.suppressed(self, record)
        )


### event: config-inited

def _get_filters(config):
    format_name = lambda name: f'{NAMESPACE}.{name}'

    filters_by_prefix = {}
    for name, levels in config.suppress_loggers.items():
        prefix = format_name(name)
        suppressor = SphinxSuppressLogger(prefix, levels)
        filters_by_prefix.setdefault(prefix, []).append(suppressor)

    is_pattern = lambda s: isinstance(s, (str, re.Pattern))
    groups, patterns = partition(is_pattern, config.suppress_records)
    for group in groups:  # type: tuple[str, ...]
        prefix = format_name(group[0])
        suppressor = SphinxSuppressRecord(prefix, True, group[1:])
        filters_by_prefix.setdefault(prefix, []).append(suppressor)
    # default filter
    default_filter = SphinxSuppressPatterns(patterns)
    return default_filter, filters_by_prefix


def _is_sphinx_logger_adapter(obj):
    return isinstance(obj, SphinxLoggerAdapter)


def _update_logger_in(module, default_filter, filters_by_prefix, _cache):
    if module.__name__ in _cache:
        return

    _cache.add(module.__name__)
    members = inspect.getmembers(module, _is_sphinx_logger_adapter)
    for _, adapter in members:
        for prefix, filters in filters_by_prefix.items():
            if adapter.logger.name.startswith(prefix):
                for f in filters:
                    # a logger might be imported from a module
                    # that was not yet marked, so we only add
                    # the filter once
                    if f not in adapter.logger.filters:
                        adapter.logger.addFilter(f)
        if default_filter not in adapter.logger.filters:
            adapter.logger.addFilter(default_filter)


def install_supress_handlers(app, config):
    # type: (Sphinx, Config) -> None

    default_filter, filters_by_prefix = _get_filters(config)
    seen = set()

    for extension in app.extensions.values():  # type: Extension
        if extension.name in config.suppress_protect:
            # skip the extension
            continue

        mod = extension.module
        _update_logger_in(mod, default_filter, filters_by_prefix, seen)
        if not hasattr(mod, '__path__'):
            continue

        # find the loggers declared in a submodule
        mod_path, mod_prefix = mod.__path__, mod.__name__ + '.'
        with warnings.catch_warnings():
            warnings.simplefilter('ignore', DeprecationWarning)
            warnings.simplefilter('ignore', PendingDeprecationWarning)
            for mod_info in pkgutil.iter_modules(mod_path, mod_prefix):
                if mod_info.name in config.suppress_protect:
                    # skip the module
                    continue

                try:
                    mod = importlib.import_module(mod_info.name)
                except ImportError:
                    continue
                _update_logger_in(mod, default_filter, filters_by_prefix, seen)


def setup(app):
    # type: (Sphinx) -> dict
    app.add_config_value('suppress_loggers', {}, True)
    app.add_config_value('suppress_protect', [], True)
    app.add_config_value('suppress_records', [], True)
    # @contract: no extension is loaded after config-inited is fired
    app.connect('config-inited', install_supress_handlers, priority=1000)
    return {'parallel_read_safe': True, 'parallel_write_safe': True}