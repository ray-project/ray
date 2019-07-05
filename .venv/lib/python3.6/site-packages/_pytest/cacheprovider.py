"""
merged implementation of the cache provider

the name cache was not chosen to ensure pluggy automatically
ignores the external pytest-cache
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import json
import os
from collections import OrderedDict

import attr
import py
import six

import pytest
from .compat import _PY2 as PY2
from .pathlib import Path
from .pathlib import resolve_from_str
from .pathlib import rmtree

README_CONTENT = u"""\
# pytest cache directory #

This directory contains data from the pytest's cache plugin,
which provides the `--lf` and `--ff` options, as well as the `cache` fixture.

**Do not** commit this to version control.

See [the docs](https://docs.pytest.org/en/latest/cache.html) for more information.
"""

CACHEDIR_TAG_CONTENT = b"""\
Signature: 8a477f597d28d172789f06886806bc55
# This file is a cache directory tag created by pytest.
# For information about cache directory tags, see:
#	http://www.bford.info/cachedir/spec.html
"""


@attr.s
class Cache(object):
    _cachedir = attr.ib(repr=False)
    _config = attr.ib(repr=False)

    @classmethod
    def for_config(cls, config):
        cachedir = cls.cache_dir_from_config(config)
        if config.getoption("cacheclear") and cachedir.exists():
            rmtree(cachedir, force=True)
            cachedir.mkdir()
        return cls(cachedir, config)

    @staticmethod
    def cache_dir_from_config(config):
        return resolve_from_str(config.getini("cache_dir"), config.rootdir)

    def warn(self, fmt, **args):
        from _pytest.warnings import _issue_warning_captured
        from _pytest.warning_types import PytestCacheWarning

        _issue_warning_captured(
            PytestCacheWarning(fmt.format(**args) if args else fmt),
            self._config.hook,
            stacklevel=3,
        )

    def makedir(self, name):
        """ return a directory path object with the given name.  If the
        directory does not yet exist, it will be created.  You can use it
        to manage files likes e. g. store/retrieve database
        dumps across test sessions.

        :param name: must be a string not containing a ``/`` separator.
             Make sure the name contains your plugin or application
             identifiers to prevent clashes with other cache users.
        """
        name = Path(name)
        if len(name.parts) > 1:
            raise ValueError("name is not allowed to contain path separators")
        res = self._cachedir.joinpath("d", name)
        res.mkdir(exist_ok=True, parents=True)
        return py.path.local(res)

    def _getvaluepath(self, key):
        return self._cachedir.joinpath("v", Path(key))

    def get(self, key, default):
        """ return cached value for the given key.  If no value
        was yet cached or the value cannot be read, the specified
        default is returned.

        :param key: must be a ``/`` separated value. Usually the first
             name is the name of your plugin or your application.
        :param default: must be provided in case of a cache-miss or
             invalid cache values.

        """
        path = self._getvaluepath(key)
        try:
            with path.open("r") as f:
                return json.load(f)
        except (ValueError, IOError, OSError):
            return default

    def set(self, key, value):
        """ save value for the given key.

        :param key: must be a ``/`` separated value. Usually the first
             name is the name of your plugin or your application.
        :param value: must be of any combination of basic
               python types, including nested types
               like e. g. lists of dictionaries.
        """
        path = self._getvaluepath(key)
        try:
            if path.parent.is_dir():
                cache_dir_exists_already = True
            else:
                cache_dir_exists_already = self._cachedir.exists()
                path.parent.mkdir(exist_ok=True, parents=True)
        except (IOError, OSError):
            self.warn("could not create cache path {path}", path=path)
            return
        if not cache_dir_exists_already:
            self._ensure_supporting_files()
        try:
            f = path.open("wb" if PY2 else "w")
        except (IOError, OSError):
            self.warn("cache could not write path {path}", path=path)
        else:
            with f:
                json.dump(value, f, indent=2, sort_keys=True)

    def _ensure_supporting_files(self):
        """Create supporting files in the cache dir that are not really part of the cache."""
        readme_path = self._cachedir / "README.md"
        readme_path.write_text(README_CONTENT)

        gitignore_path = self._cachedir.joinpath(".gitignore")
        msg = u"# Created by pytest automatically.\n*"
        gitignore_path.write_text(msg, encoding="UTF-8")

        cachedir_tag_path = self._cachedir.joinpath("CACHEDIR.TAG")
        cachedir_tag_path.write_bytes(CACHEDIR_TAG_CONTENT)


class LFPlugin(object):
    """ Plugin which implements the --lf (run last-failing) option """

    def __init__(self, config):
        self.config = config
        active_keys = "lf", "failedfirst"
        self.active = any(config.getoption(key) for key in active_keys)
        self.lastfailed = config.cache.get("cache/lastfailed", {})
        self._previously_failed_count = None
        self._report_status = None
        self._skipped_files = 0  # count skipped files during collection due to --lf

    def last_failed_paths(self):
        """Returns a set with all Paths()s of the previously failed nodeids (cached).
        """
        result = getattr(self, "_last_failed_paths", None)
        if result is None:
            rootpath = Path(self.config.rootdir)
            result = {rootpath / nodeid.split("::")[0] for nodeid in self.lastfailed}
            self._last_failed_paths = result
        return result

    def pytest_ignore_collect(self, path):
        """
        Ignore this file path if we are in --lf mode and it is not in the list of
        previously failed files.
        """
        if (
            self.active
            and self.config.getoption("lf")
            and path.isfile()
            and self.lastfailed
        ):
            skip_it = Path(path) not in self.last_failed_paths()
            if skip_it:
                self._skipped_files += 1
            return skip_it

    def pytest_report_collectionfinish(self):
        if self.active and self.config.getoption("verbose") >= 0:
            return "run-last-failure: %s" % self._report_status

    def pytest_runtest_logreport(self, report):
        if (report.when == "call" and report.passed) or report.skipped:
            self.lastfailed.pop(report.nodeid, None)
        elif report.failed:
            self.lastfailed[report.nodeid] = True

    def pytest_collectreport(self, report):
        passed = report.outcome in ("passed", "skipped")
        if passed:
            if report.nodeid in self.lastfailed:
                self.lastfailed.pop(report.nodeid)
                self.lastfailed.update((item.nodeid, True) for item in report.result)
        else:
            self.lastfailed[report.nodeid] = True

    def pytest_collection_modifyitems(self, session, config, items):
        if not self.active:
            return

        if self.lastfailed:
            previously_failed = []
            previously_passed = []
            for item in items:
                if item.nodeid in self.lastfailed:
                    previously_failed.append(item)
                else:
                    previously_passed.append(item)
            self._previously_failed_count = len(previously_failed)

            if not previously_failed:
                # Running a subset of all tests with recorded failures
                # only outside of it.
                self._report_status = "%d known failures not in selected tests" % (
                    len(self.lastfailed),
                )
            else:
                if self.config.getoption("lf"):
                    items[:] = previously_failed
                    config.hook.pytest_deselected(items=previously_passed)
                else:  # --failedfirst
                    items[:] = previously_failed + previously_passed

                noun = "failure" if self._previously_failed_count == 1 else "failures"
                if self._skipped_files > 0:
                    files_noun = "file" if self._skipped_files == 1 else "files"
                    skipped_files_msg = " (skipped {files} {files_noun})".format(
                        files=self._skipped_files, files_noun=files_noun
                    )
                else:
                    skipped_files_msg = ""
                suffix = " first" if self.config.getoption("failedfirst") else ""
                self._report_status = "rerun previous {count} {noun}{suffix}{skipped_files}".format(
                    count=self._previously_failed_count,
                    suffix=suffix,
                    noun=noun,
                    skipped_files=skipped_files_msg,
                )
        else:
            self._report_status = "no previously failed tests, "
            if self.config.getoption("last_failed_no_failures") == "none":
                self._report_status += "deselecting all items."
                config.hook.pytest_deselected(items=items)
                items[:] = []
            else:
                self._report_status += "not deselecting items."

    def pytest_sessionfinish(self, session):
        config = self.config
        if config.getoption("cacheshow") or hasattr(config, "slaveinput"):
            return

        saved_lastfailed = config.cache.get("cache/lastfailed", {})
        if saved_lastfailed != self.lastfailed:
            config.cache.set("cache/lastfailed", self.lastfailed)


class NFPlugin(object):
    """ Plugin which implements the --nf (run new-first) option """

    def __init__(self, config):
        self.config = config
        self.active = config.option.newfirst
        self.cached_nodeids = config.cache.get("cache/nodeids", [])

    def pytest_collection_modifyitems(self, session, config, items):
        if self.active:
            new_items = OrderedDict()
            other_items = OrderedDict()
            for item in items:
                if item.nodeid not in self.cached_nodeids:
                    new_items[item.nodeid] = item
                else:
                    other_items[item.nodeid] = item

            items[:] = self._get_increasing_order(
                six.itervalues(new_items)
            ) + self._get_increasing_order(six.itervalues(other_items))
        self.cached_nodeids = [x.nodeid for x in items if isinstance(x, pytest.Item)]

    def _get_increasing_order(self, items):
        return sorted(items, key=lambda item: item.fspath.mtime(), reverse=True)

    def pytest_sessionfinish(self, session):
        config = self.config
        if config.getoption("cacheshow") or hasattr(config, "slaveinput"):
            return

        config.cache.set("cache/nodeids", self.cached_nodeids)


def pytest_addoption(parser):
    group = parser.getgroup("general")
    group.addoption(
        "--lf",
        "--last-failed",
        action="store_true",
        dest="lf",
        help="rerun only the tests that failed "
        "at the last run (or all if none failed)",
    )
    group.addoption(
        "--ff",
        "--failed-first",
        action="store_true",
        dest="failedfirst",
        help="run all tests but run the last failures first.  "
        "This may re-order tests and thus lead to "
        "repeated fixture setup/teardown",
    )
    group.addoption(
        "--nf",
        "--new-first",
        action="store_true",
        dest="newfirst",
        help="run tests from new files first, then the rest of the tests "
        "sorted by file mtime",
    )
    group.addoption(
        "--cache-show",
        action="append",
        nargs="?",
        dest="cacheshow",
        help=(
            "show cache contents, don't perform collection or tests. "
            "Optional argument: glob (default: '*')."
        ),
    )
    group.addoption(
        "--cache-clear",
        action="store_true",
        dest="cacheclear",
        help="remove all cache contents at start of test run.",
    )
    cache_dir_default = ".pytest_cache"
    if "TOX_ENV_DIR" in os.environ:
        cache_dir_default = os.path.join(os.environ["TOX_ENV_DIR"], cache_dir_default)
    parser.addini("cache_dir", default=cache_dir_default, help="cache directory path.")
    group.addoption(
        "--lfnf",
        "--last-failed-no-failures",
        action="store",
        dest="last_failed_no_failures",
        choices=("all", "none"),
        default="all",
        help="which tests to run with no previously (known) failures.",
    )


def pytest_cmdline_main(config):
    if config.option.cacheshow:
        from _pytest.main import wrap_session

        return wrap_session(config, cacheshow)


@pytest.hookimpl(tryfirst=True)
def pytest_configure(config):
    config.cache = Cache.for_config(config)
    config.pluginmanager.register(LFPlugin(config), "lfplugin")
    config.pluginmanager.register(NFPlugin(config), "nfplugin")


@pytest.fixture
def cache(request):
    """
    Return a cache object that can persist state between testing sessions.

    cache.get(key, default)
    cache.set(key, value)

    Keys must be a ``/`` separated value, where the first part is usually the
    name of your plugin or application to avoid clashes with other cache users.

    Values can be any object handled by the json stdlib module.
    """
    return request.config.cache


def pytest_report_header(config):
    """Display cachedir with --cache-show and if non-default."""
    if config.option.verbose > 0 or config.getini("cache_dir") != ".pytest_cache":
        cachedir = config.cache._cachedir
        # TODO: evaluate generating upward relative paths
        # starting with .., ../.. if sensible

        try:
            displaypath = cachedir.relative_to(config.rootdir)
        except ValueError:
            displaypath = cachedir
        return "cachedir: {}".format(displaypath)


def cacheshow(config, session):
    from pprint import pformat

    tw = py.io.TerminalWriter()
    tw.line("cachedir: " + str(config.cache._cachedir))
    if not config.cache._cachedir.is_dir():
        tw.line("cache is empty")
        return 0

    glob = config.option.cacheshow[0]
    if glob is None:
        glob = "*"

    dummy = object()
    basedir = config.cache._cachedir
    vdir = basedir / "v"
    tw.sep("-", "cache values for %r" % glob)
    for valpath in sorted(x for x in vdir.rglob(glob) if x.is_file()):
        key = valpath.relative_to(vdir)
        val = config.cache.get(key, dummy)
        if val is dummy:
            tw.line("%s contains unreadable content, will be ignored" % key)
        else:
            tw.line("%s contains:" % key)
            for line in pformat(val).splitlines():
                tw.line("  " + line)

    ddir = basedir / "d"
    if ddir.is_dir():
        contents = sorted(ddir.rglob(glob))
        tw.sep("-", "cache directories for %r" % glob)
        for p in contents:
            # if p.check(dir=1):
            #    print("%s/" % p.relto(basedir))
            if p.is_file():
                key = p.relative_to(basedir)
                tw.line("{} is a file of length {:d}".format(key, p.stat().st_size))
    return 0
