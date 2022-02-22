# encoding: utf-8
"""
The *pathspec* package provides pattern matching for file paths. So far
this only includes Git's wildmatch pattern matching (the style used for
".gitignore" files).

The following classes are imported and made available from the root of
the `pathspec` package:

- :class:`pathspec.pathspec.PathSpec`

- :class:`pathspec.pattern.Pattern`

- :class:`pathspec.pattern.RegexPattern`

- :class:`pathspec.util.RecursionError`

The following functions are also imported:

- :func:`pathspec.util.iter_tree`
- :func:`pathspec.util.lookup_pattern`
- :func:`pathspec.util.match_files`
"""
from __future__ import unicode_literals

__author__ = "Caleb P. Burns"
__copyright__ = "Copyright © 2013-2020 Caleb P. Burns"
__created__ = "2013-10-12"
__credits__ = [
    "dahlia <https://github.com/dahlia>",
    "highb <https://github.com/highb>",
    "029xue <https://github.com/029xue>",
    "mikexstudios <https://github.com/mikexstudios>",
    "nhumrich <https://github.com/nhumrich>",
    "davidfraser <https://github.com/davidfraser>",
    "demurgos <https://github.com/demurgos>",
    "ghickman <https://github.com/ghickman>",
    "nvie <https://github.com/nvie>",
    "adrienverge <https://github.com/adrienverge>",
    "AndersBlomdell <https://github.com/AndersBlomdell>",
    "highb <https://github.com/highb>",
    "thmxv <https://github.com/thmxv>",
    "wimglenn <https://github.com/wimglenn>",
    "hugovk <https://github.com/hugovk>",
    "dcecile <https://github.com/dcecile>",
    "mroutis <https://github.com/mroutis>",
    "jdufresne <https://github.com/jdufresne>",
    "groodt <https://github.com/groodt>",
    "ftrofin <https://github.com/ftrofin>",
    "pykong <https://github.com/pykong>",
    "nhhollander <https://github.com/nhhollander>",
]
__email__ = "cpburnz@gmail.com"
__license__ = "MPL 2.0"
__project__ = "pathspec"
__status__ = "Development"
__updated__ = "2020-11-07"
__version__ = "0.8.1"

from .pathspec import PathSpec
from .pattern import Pattern, RegexPattern
from .util import iter_tree, lookup_pattern, match_files, RecursionError

# Load pattern implementations.
from . import patterns

# Expose `GitIgnorePattern` class in the root module for backward
# compatibility with v0.4.
from .patterns.gitwildmatch import GitIgnorePattern
