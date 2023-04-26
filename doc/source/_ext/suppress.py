#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# author: picnixz
# license: MIT

r"""A filter suppressing logging records issued by a Sphinx logger.

The records are filtered according to their context (logger name and record
level) and their formatted message. You can filter entire loggers, or individual
records.

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
import inspect
import itertools
import logging
import re
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


def not_none(value):
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
    return list(filter(not_none, map(_normalize_level, levels)))


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
            and self.levels is ALL
            or record.levelno in self.levels
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

        :param name: A logger's name pattern to suppress.
        :type name: str | re.Pattern
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


# event: config-inited
def install_supress_handlers(app, config):
    # type: (Sphinx, Config) -> None

    prefix = f'{NAMESPACE}.'
    filters = []
    for name, levels in config.suppress_loggers.items():
        # the real logger name is always prefixed by NAMESPACE
        logger_name = f'{prefix}{name}'
        suppressor = SphinxSuppressLogger(logger_name, levels)
        filters.append(suppressor)

    is_pattern = lambda value: isinstance(value, (str, re.Pattern))
    groups, patterns = partition(is_pattern, config.suppress_records)
    for group in groups:  # type: tuple[str, ...]
        name = prefix + group[0]
        suppressor = SphinxSuppressRecord(name, True, group[1:])
        filters.append(suppressor)
    filters.append(SphinxSuppressPatterns(patterns))

    is_sphinx_logger_adapter = lambda value: isinstance(value, SphinxLoggerAdapter)
    for extension in app.extensions.values():  # type: Extension
        module = extension.module
        for _, adapter in inspect.getmembers(module, is_sphinx_logger_adapter):
            add_filter = adapter.logger.addFilter
            for f in filters:
                add_filter(f)


def setup(app):
    # type: (Sphinx) -> dict
    app.add_config_value('suppress_loggers', {}, True)
    app.add_config_value('suppress_records', [], True)
    # @contract: no extension is loaded after config-inited is fired
    app.connect('config-inited', install_supress_handlers, priority=1000)
    return {'parallel_read_safe': True, 'parallel_write_safe': True}
