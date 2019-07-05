import argparse
import warnings

import py
import six

from _pytest.config.exceptions import UsageError

FILE_OR_DIR = "file_or_dir"


class Parser(object):
    """ Parser for command line arguments and ini-file values.

    :ivar extra_info: dict of generic param -> value to display in case
        there's an error processing the command line arguments.
    """

    prog = None

    def __init__(self, usage=None, processopt=None):
        self._anonymous = OptionGroup("custom options", parser=self)
        self._groups = []
        self._processopt = processopt
        self._usage = usage
        self._inidict = {}
        self._ininames = []
        self.extra_info = {}

    def processoption(self, option):
        if self._processopt:
            if option.dest:
                self._processopt(option)

    def getgroup(self, name, description="", after=None):
        """ get (or create) a named option Group.

        :name: name of the option group.
        :description: long description for --help output.
        :after: name of other group, used for ordering --help output.

        The returned group object has an ``addoption`` method with the same
        signature as :py:func:`parser.addoption
        <_pytest.config.Parser.addoption>` but will be shown in the
        respective group in the output of ``pytest. --help``.
        """
        for group in self._groups:
            if group.name == name:
                return group
        group = OptionGroup(name, description, parser=self)
        i = 0
        for i, grp in enumerate(self._groups):
            if grp.name == after:
                break
        self._groups.insert(i + 1, group)
        return group

    def addoption(self, *opts, **attrs):
        """ register a command line option.

        :opts: option names, can be short or long options.
        :attrs: same attributes which the ``add_option()`` function of the
           `argparse library
           <http://docs.python.org/2/library/argparse.html>`_
           accepts.

        After command line parsing options are available on the pytest config
        object via ``config.option.NAME`` where ``NAME`` is usually set
        by passing a ``dest`` attribute, for example
        ``addoption("--long", dest="NAME", ...)``.
        """
        self._anonymous.addoption(*opts, **attrs)

    def parse(self, args, namespace=None):
        from _pytest._argcomplete import try_argcomplete

        self.optparser = self._getparser()
        try_argcomplete(self.optparser)
        args = [str(x) if isinstance(x, py.path.local) else x for x in args]
        return self.optparser.parse_args(args, namespace=namespace)

    def _getparser(self):
        from _pytest._argcomplete import filescompleter

        optparser = MyOptionParser(self, self.extra_info, prog=self.prog)
        groups = self._groups + [self._anonymous]
        for group in groups:
            if group.options:
                desc = group.description or group.name
                arggroup = optparser.add_argument_group(desc)
                for option in group.options:
                    n = option.names()
                    a = option.attrs()
                    arggroup.add_argument(*n, **a)
        # bash like autocompletion for dirs (appending '/')
        optparser.add_argument(FILE_OR_DIR, nargs="*").completer = filescompleter
        return optparser

    def parse_setoption(self, args, option, namespace=None):
        parsedoption = self.parse(args, namespace=namespace)
        for name, value in parsedoption.__dict__.items():
            setattr(option, name, value)
        return getattr(parsedoption, FILE_OR_DIR)

    def parse_known_args(self, args, namespace=None):
        """parses and returns a namespace object with known arguments at this
        point.
        """
        return self.parse_known_and_unknown_args(args, namespace=namespace)[0]

    def parse_known_and_unknown_args(self, args, namespace=None):
        """parses and returns a namespace object with known arguments, and
        the remaining arguments unknown at this point.
        """
        optparser = self._getparser()
        args = [str(x) if isinstance(x, py.path.local) else x for x in args]
        return optparser.parse_known_args(args, namespace=namespace)

    def addini(self, name, help, type=None, default=None):
        """ register an ini-file option.

        :name: name of the ini-variable
        :type: type of the variable, can be ``pathlist``, ``args``, ``linelist``
               or ``bool``.
        :default: default value if no ini-file option exists but is queried.

        The value of ini-variables can be retrieved via a call to
        :py:func:`config.getini(name) <_pytest.config.Config.getini>`.
        """
        assert type in (None, "pathlist", "args", "linelist", "bool")
        self._inidict[name] = (help, type, default)
        self._ininames.append(name)


class ArgumentError(Exception):
    """
    Raised if an Argument instance is created with invalid or
    inconsistent arguments.
    """

    def __init__(self, msg, option):
        self.msg = msg
        self.option_id = str(option)

    def __str__(self):
        if self.option_id:
            return "option %s: %s" % (self.option_id, self.msg)
        else:
            return self.msg


class Argument(object):
    """class that mimics the necessary behaviour of optparse.Option

    it's currently a least effort implementation
    and ignoring choices and integer prefixes
    https://docs.python.org/3/library/optparse.html#optparse-standard-option-types
    """

    _typ_map = {"int": int, "string": str, "float": float, "complex": complex}

    def __init__(self, *names, **attrs):
        """store parms in private vars for use in add_argument"""
        self._attrs = attrs
        self._short_opts = []
        self._long_opts = []
        self.dest = attrs.get("dest")
        if "%default" in (attrs.get("help") or ""):
            warnings.warn(
                'pytest now uses argparse. "%default" should be'
                ' changed to "%(default)s" ',
                DeprecationWarning,
                stacklevel=3,
            )
        try:
            typ = attrs["type"]
        except KeyError:
            pass
        else:
            # this might raise a keyerror as well, don't want to catch that
            if isinstance(typ, six.string_types):
                if typ == "choice":
                    warnings.warn(
                        "`type` argument to addoption() is the string %r."
                        " For choices this is optional and can be omitted, "
                        " but when supplied should be a type (for example `str` or `int`)."
                        " (options: %s)" % (typ, names),
                        DeprecationWarning,
                        stacklevel=4,
                    )
                    # argparse expects a type here take it from
                    # the type of the first element
                    attrs["type"] = type(attrs["choices"][0])
                else:
                    warnings.warn(
                        "`type` argument to addoption() is the string %r, "
                        " but when supplied should be a type (for example `str` or `int`)."
                        " (options: %s)" % (typ, names),
                        DeprecationWarning,
                        stacklevel=4,
                    )
                    attrs["type"] = Argument._typ_map[typ]
                # used in test_parseopt -> test_parse_defaultgetter
                self.type = attrs["type"]
            else:
                self.type = typ
        try:
            # attribute existence is tested in Config._processopt
            self.default = attrs["default"]
        except KeyError:
            pass
        self._set_opt_strings(names)
        if not self.dest:
            if self._long_opts:
                self.dest = self._long_opts[0][2:].replace("-", "_")
            else:
                try:
                    self.dest = self._short_opts[0][1:]
                except IndexError:
                    raise ArgumentError("need a long or short option", self)

    def names(self):
        return self._short_opts + self._long_opts

    def attrs(self):
        # update any attributes set by processopt
        attrs = "default dest help".split()
        if self.dest:
            attrs.append(self.dest)
        for attr in attrs:
            try:
                self._attrs[attr] = getattr(self, attr)
            except AttributeError:
                pass
        if self._attrs.get("help"):
            a = self._attrs["help"]
            a = a.replace("%default", "%(default)s")
            # a = a.replace('%prog', '%(prog)s')
            self._attrs["help"] = a
        return self._attrs

    def _set_opt_strings(self, opts):
        """directly from optparse

        might not be necessary as this is passed to argparse later on"""
        for opt in opts:
            if len(opt) < 2:
                raise ArgumentError(
                    "invalid option string %r: "
                    "must be at least two characters long" % opt,
                    self,
                )
            elif len(opt) == 2:
                if not (opt[0] == "-" and opt[1] != "-"):
                    raise ArgumentError(
                        "invalid short option string %r: "
                        "must be of the form -x, (x any non-dash char)" % opt,
                        self,
                    )
                self._short_opts.append(opt)
            else:
                if not (opt[0:2] == "--" and opt[2] != "-"):
                    raise ArgumentError(
                        "invalid long option string %r: "
                        "must start with --, followed by non-dash" % opt,
                        self,
                    )
                self._long_opts.append(opt)

    def __repr__(self):
        args = []
        if self._short_opts:
            args += ["_short_opts: " + repr(self._short_opts)]
        if self._long_opts:
            args += ["_long_opts: " + repr(self._long_opts)]
        args += ["dest: " + repr(self.dest)]
        if hasattr(self, "type"):
            args += ["type: " + repr(self.type)]
        if hasattr(self, "default"):
            args += ["default: " + repr(self.default)]
        return "Argument({})".format(", ".join(args))


class OptionGroup(object):
    def __init__(self, name, description="", parser=None):
        self.name = name
        self.description = description
        self.options = []
        self.parser = parser

    def addoption(self, *optnames, **attrs):
        """ add an option to this group.

        if a shortened version of a long option is specified it will
        be suppressed in the help. addoption('--twowords', '--two-words')
        results in help showing '--two-words' only, but --twowords gets
        accepted **and** the automatic destination is in args.twowords
        """
        conflict = set(optnames).intersection(
            name for opt in self.options for name in opt.names()
        )
        if conflict:
            raise ValueError("option names %s already added" % conflict)
        option = Argument(*optnames, **attrs)
        self._addoption_instance(option, shortupper=False)

    def _addoption(self, *optnames, **attrs):
        option = Argument(*optnames, **attrs)
        self._addoption_instance(option, shortupper=True)

    def _addoption_instance(self, option, shortupper=False):
        if not shortupper:
            for opt in option._short_opts:
                if opt[0] == "-" and opt[1].islower():
                    raise ValueError("lowercase shortoptions reserved")
        if self.parser:
            self.parser.processoption(option)
        self.options.append(option)


class MyOptionParser(argparse.ArgumentParser):
    def __init__(self, parser, extra_info=None, prog=None):
        if not extra_info:
            extra_info = {}
        self._parser = parser
        argparse.ArgumentParser.__init__(
            self,
            prog=prog,
            usage=parser._usage,
            add_help=False,
            formatter_class=DropShorterLongHelpFormatter,
        )
        # extra_info is a dict of (param -> value) to display if there's
        # an usage error to provide more contextual information to the user
        self.extra_info = extra_info

    def error(self, message):
        """Transform argparse error message into UsageError."""
        msg = "%s: error: %s" % (self.prog, message)

        if hasattr(self._parser, "_config_source_hint"):
            msg = "%s (%s)" % (msg, self._parser._config_source_hint)

        raise UsageError(self.format_usage() + msg)

    def parse_args(self, args=None, namespace=None):
        """allow splitting of positional arguments"""
        args, argv = self.parse_known_args(args, namespace)
        if argv:
            for arg in argv:
                if arg and arg[0] == "-":
                    lines = ["unrecognized arguments: %s" % (" ".join(argv))]
                    for k, v in sorted(self.extra_info.items()):
                        lines.append("  %s: %s" % (k, v))
                    self.error("\n".join(lines))
            getattr(args, FILE_OR_DIR).extend(argv)
        return args


class DropShorterLongHelpFormatter(argparse.HelpFormatter):
    """shorten help for long options that differ only in extra hyphens

    - collapse **long** options that are the same except for extra hyphens
    - special action attribute map_long_option allows surpressing additional
      long options
    - shortcut if there are only two options and one of them is a short one
    - cache result on action object as this is called at least 2 times
    """

    def _format_action_invocation(self, action):
        orgstr = argparse.HelpFormatter._format_action_invocation(self, action)
        if orgstr and orgstr[0] != "-":  # only optional arguments
            return orgstr
        res = getattr(action, "_formatted_action_invocation", None)
        if res:
            return res
        options = orgstr.split(", ")
        if len(options) == 2 and (len(options[0]) == 2 or len(options[1]) == 2):
            # a shortcut for '-h, --help' or '--abc', '-a'
            action._formatted_action_invocation = orgstr
            return orgstr
        return_list = []
        option_map = getattr(action, "map_long_option", {})
        if option_map is None:
            option_map = {}
        short_long = {}
        for option in options:
            if len(option) == 2 or option[2] == " ":
                continue
            if not option.startswith("--"):
                raise ArgumentError(
                    'long optional argument without "--": [%s]' % (option), self
                )
            xxoption = option[2:]
            if xxoption.split()[0] not in option_map:
                shortened = xxoption.replace("-", "")
                if shortened not in short_long or len(short_long[shortened]) < len(
                    xxoption
                ):
                    short_long[shortened] = xxoption
        # now short_long has been filled out to the longest with dashes
        # **and** we keep the right option ordering from add_argument
        for option in options:
            if len(option) == 2 or option[2] == " ":
                return_list.append(option)
            if option[2:] == short_long.get(option.replace("-", "")):
                return_list.append(option.replace(" ", "=", 1))
        action._formatted_action_invocation = ", ".join(return_list)
        return action._formatted_action_invocation
