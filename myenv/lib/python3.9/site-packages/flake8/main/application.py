"""Module containing the application logic for Flake8."""
from __future__ import print_function

import argparse
import logging
import sys
import time
from typing import Dict, List, Optional, Set, Tuple

import flake8
from flake8 import checker
from flake8 import defaults
from flake8 import exceptions
from flake8 import style_guide
from flake8 import utils
from flake8.main import options
from flake8.options import aggregator, config
from flake8.options import manager
from flake8.plugins import manager as plugin_manager

if False:  # `typing.TYPE_CHECKING` was introduced in 3.5.2
    from typing import Type  # `typing.Type` was introduced in 3.5.2
    from flake8.formatting.base import BaseFormatter


LOG = logging.getLogger(__name__)


class Application(object):
    """Abstract our application into a class."""

    def __init__(self, program="flake8", version=flake8.__version__):
        """Initialize our application.

        :param str program:
            The name of the program/application that we're executing.
        :param str version:
            The version of the program/application we're executing.
        """
        #: The timestamp when the Application instance was instantiated.
        self.start_time = time.time()
        #: The timestamp when the Application finished reported errors.
        self.end_time = None  # type: float
        #: The name of the program being run
        self.program = program
        #: The version of the program being run
        self.version = version
        #: The prelimary argument parser for handling options required for
        #: obtaining and parsing the configuration file.
        self.prelim_arg_parser = argparse.ArgumentParser(add_help=False)
        options.register_preliminary_options(self.prelim_arg_parser)
        #: The instance of :class:`flake8.options.manager.OptionManager` used
        #: to parse and handle the options and arguments passed by the user
        self.option_manager = manager.OptionManager(
            prog="flake8",
            version=flake8.__version__,
            parents=[self.prelim_arg_parser],
        )
        options.register_default_options(self.option_manager)

        #: The instance of :class:`flake8.plugins.manager.Checkers`
        self.check_plugins = None  # type: plugin_manager.Checkers
        # fmt: off
        #: The instance of :class:`flake8.plugins.manager.ReportFormatters`
        self.formatting_plugins = None  # type: plugin_manager.ReportFormatters
        # fmt: on
        #: The user-selected formatter from :attr:`formatting_plugins`
        self.formatter = None  # type: BaseFormatter
        #: The :class:`flake8.style_guide.StyleGuideManager` built from the
        #: user's options
        self.guide = None  # type: style_guide.StyleGuideManager
        #: The :class:`flake8.checker.Manager` that will handle running all of
        #: the checks selected by the user.
        self.file_checker_manager = None  # type: checker.Manager

        #: The user-supplied options parsed into an instance of
        #: :class:`argparse.Namespace`
        self.options = None  # type: argparse.Namespace
        #: The left over arguments that were not parsed by
        #: :attr:`option_manager`
        self.args = None  # type: List[str]
        #: The number of errors, warnings, and other messages after running
        #: flake8 and taking into account ignored errors and lines.
        self.result_count = 0
        #: The total number of errors before accounting for ignored errors and
        #: lines.
        self.total_result_count = 0
        #: Whether or not something catastrophic happened and we should exit
        #: with a non-zero status code
        self.catastrophic_failure = False

        #: Whether the program is processing a diff or not
        self.running_against_diff = False
        #: The parsed diff information
        self.parsed_diff = {}  # type: Dict[str, Set[int]]

    def parse_preliminary_options(self, argv):
        # type: (List[str]) -> Tuple[argparse.Namespace, List[str]]
        """Get preliminary options from the CLI, pre-plugin-loading.

        We need to know the values of a few standard options so that we can
        locate configuration files and configure logging.

        Since plugins aren't loaded yet, there may be some as-yet-unknown
        options; we ignore those for now, they'll be parsed later when we do
        real option parsing.

        :param list argv:
            Command-line arguments passed in directly.
        :returns:
            Populated namespace and list of remaining argument strings.
        :rtype:
            (argparse.Namespace, list)
        """
        args, rest = self.prelim_arg_parser.parse_known_args(argv)
        # XXX (ericvw): Special case "forwarding" the output file option so
        # that it can be reparsed again for the BaseFormatter.filename.
        if args.output_file:
            rest.extend(("--output-file", args.output_file))
        return args, rest

    def exit(self):
        # type: () -> None
        """Handle finalization and exiting the program.

        This should be the last thing called on the application instance. It
        will check certain options and exit appropriately.
        """
        if self.options.count:
            print(self.result_count)

        if self.options.exit_zero:
            raise SystemExit(self.catastrophic_failure)
        else:
            raise SystemExit(
                (self.result_count > 0) or self.catastrophic_failure
            )

    def find_plugins(self, config_finder):
        # type: (config.ConfigFileFinder) -> None
        """Find and load the plugins for this application.

        Set the :attr:`check_plugins` and :attr:`formatting_plugins` attributes
        based on the discovered plugins found.

        :param config.ConfigFileFinder config_finder:
            The finder for finding and reading configuration files.
        """
        local_plugins = config.get_local_plugins(config_finder)

        sys.path.extend(local_plugins.paths)

        self.check_plugins = plugin_manager.Checkers(local_plugins.extension)

        self.formatting_plugins = plugin_manager.ReportFormatters(
            local_plugins.report
        )

        self.check_plugins.load_plugins()
        self.formatting_plugins.load_plugins()

    def register_plugin_options(self):
        # type: () -> None
        """Register options provided by plugins to our option manager."""
        self.check_plugins.register_options(self.option_manager)
        self.check_plugins.register_plugin_versions(self.option_manager)
        self.formatting_plugins.register_options(self.option_manager)

    def parse_configuration_and_cli(
        self,
        config_finder,  # type: config.ConfigFileFinder
        argv,  # type: List[str]
    ):
        # type: (...) -> None
        """Parse configuration files and the CLI options.

        :param config.ConfigFileFinder config_finder:
            The finder for finding and reading configuration files.
        :param list argv:
            Command-line arguments passed in directly.
        """
        self.options, self.args = aggregator.aggregate_options(
            self.option_manager,
            config_finder,
            argv,
        )

        self.running_against_diff = self.options.diff
        if self.running_against_diff:
            self.parsed_diff = utils.parse_unified_diff()
            if not self.parsed_diff:
                self.exit()

        self.options._running_from_vcs = False

        self.check_plugins.provide_options(
            self.option_manager, self.options, self.args
        )
        self.formatting_plugins.provide_options(
            self.option_manager, self.options, self.args
        )

    def formatter_for(self, formatter_plugin_name):
        """Retrieve the formatter class by plugin name."""
        default_formatter = self.formatting_plugins["default"]
        formatter_plugin = self.formatting_plugins.get(formatter_plugin_name)
        if formatter_plugin is None:
            LOG.warning(
                '"%s" is an unknown formatter. Falling back to default.',
                formatter_plugin_name,
            )
            formatter_plugin = default_formatter

        return formatter_plugin.execute

    def make_formatter(self, formatter_class=None):
        # type: (Optional[Type[BaseFormatter]]) -> None
        """Initialize a formatter based on the parsed options."""
        format_plugin = self.options.format
        if 1 <= self.options.quiet < 2:
            format_plugin = "quiet-filename"
        elif 2 <= self.options.quiet:
            format_plugin = "quiet-nothing"

        if formatter_class is None:
            formatter_class = self.formatter_for(format_plugin)

        self.formatter = formatter_class(self.options)

    def make_guide(self):
        # type: () -> None
        """Initialize our StyleGuide."""
        self.guide = style_guide.StyleGuideManager(
            self.options, self.formatter
        )

        if self.running_against_diff:
            self.guide.add_diff_ranges(self.parsed_diff)

    def make_file_checker_manager(self):
        # type: () -> None
        """Initialize our FileChecker Manager."""
        self.file_checker_manager = checker.Manager(
            style_guide=self.guide,
            arguments=self.args,
            checker_plugins=self.check_plugins,
        )

    def run_checks(self, files=None):
        # type: (Optional[List[str]]) -> None
        """Run the actual checks with the FileChecker Manager.

        This method encapsulates the logic to make a
        :class:`~flake8.checker.Manger` instance run the checks it is
        managing.

        :param list files:
            List of filenames to process
        """
        if self.running_against_diff:
            files = sorted(self.parsed_diff)
        self.file_checker_manager.start(files)
        try:
            self.file_checker_manager.run()
        except exceptions.PluginExecutionFailed as plugin_failed:
            print(str(plugin_failed))
            print("Run flake8 with greater verbosity to see more details")
            self.catastrophic_failure = True
        LOG.info("Finished running")
        self.file_checker_manager.stop()
        self.end_time = time.time()

    def report_benchmarks(self):
        """Aggregate, calculate, and report benchmarks for this run."""
        if not self.options.benchmark:
            return

        time_elapsed = self.end_time - self.start_time
        statistics = [("seconds elapsed", time_elapsed)]
        add_statistic = statistics.append
        for statistic in defaults.STATISTIC_NAMES + ("files",):
            value = self.file_checker_manager.statistics[statistic]
            total_description = "total " + statistic + " processed"
            add_statistic((total_description, value))
            per_second_description = statistic + " processed per second"
            add_statistic((per_second_description, int(value / time_elapsed)))

        self.formatter.show_benchmarks(statistics)

    def report_errors(self):
        # type: () -> None
        """Report all the errors found by flake8 3.0.

        This also updates the :attr:`result_count` attribute with the total
        number of errors, warnings, and other messages found.
        """
        LOG.info("Reporting errors")
        results = self.file_checker_manager.report()
        self.total_result_count, self.result_count = results
        LOG.info(
            "Found a total of %d violations and reported %d",
            self.total_result_count,
            self.result_count,
        )

    def report_statistics(self):
        """Aggregate and report statistics from this run."""
        if not self.options.statistics:
            return

        self.formatter.show_statistics(self.guide.stats)

    def initialize(self, argv):
        # type: (List[str]) -> None
        """Initialize the application to be run.

        This finds the plugins, registers their options, and parses the
        command-line arguments.
        """
        # NOTE(sigmavirus24): When updating this, make sure you also update
        # our legacy API calls to these same methods.
        prelim_opts, remaining_args = self.parse_preliminary_options(argv)
        flake8.configure_logging(prelim_opts.verbose, prelim_opts.output_file)
        config_finder = config.ConfigFileFinder(
            self.program,
            prelim_opts.append_config,
            config_file=prelim_opts.config,
            ignore_config_files=prelim_opts.isolated,
        )
        self.find_plugins(config_finder)
        self.register_plugin_options()
        self.parse_configuration_and_cli(
            config_finder,
            remaining_args,
        )
        self.make_formatter()
        self.make_guide()
        self.make_file_checker_manager()

    def report(self):
        """Report errors, statistics, and benchmarks."""
        self.formatter.start()
        self.report_errors()
        self.report_statistics()
        self.report_benchmarks()
        self.formatter.stop()

    def _run(self, argv):
        # type: (List[str]) -> None
        self.initialize(argv)
        self.run_checks()
        self.report()

    def run(self, argv):
        # type: (List[str]) -> None
        """Run our application.

        This method will also handle KeyboardInterrupt exceptions for the
        entirety of the flake8 application. If it sees a KeyboardInterrupt it
        will forcibly clean up the :class:`~flake8.checker.Manager`.
        """
        try:
            self._run(argv)
        except KeyboardInterrupt as exc:
            print("... stopped")
            LOG.critical("Caught keyboard interrupt from user")
            LOG.exception(exc)
            self.catastrophic_failure = True
        except exceptions.ExecutionError as exc:
            print("There was a critical error during execution of Flake8:")
            print(exc)
            LOG.exception(exc)
            self.catastrophic_failure = True
        except exceptions.EarlyQuit:
            self.catastrophic_failure = True
            print("... stopped while processing files")
