# encoding: utf-8
"""
This module implements Git's wildmatch pattern matching which itself is
derived from Rsync's wildmatch. Git uses wildmatch for its ".gitignore"
files.
"""
from __future__ import unicode_literals

import re
import warnings

from .. import util
from ..compat import unicode
from ..pattern import RegexPattern

#: The encoding to use when parsing a byte string pattern.
_BYTES_ENCODING = 'latin1'


class GitWildMatchPattern(RegexPattern):
	"""
	The :class:`GitWildMatchPattern` class represents a compiled Git
	wildmatch pattern.
	"""

	# Keep the dict-less class hierarchy.
	__slots__ = ()

	@classmethod
	def pattern_to_regex(cls, pattern):
		"""
		Convert the pattern into a regular expression.

		*pattern* (:class:`unicode` or :class:`bytes`) is the pattern to
		convert into a regular expression.

		Returns the uncompiled regular expression (:class:`unicode`, :class:`bytes`,
		or :data:`None`), and whether matched files should be included
		(:data:`True`), excluded (:data:`False`), or if it is a
		null-operation (:data:`None`).
		"""
		if isinstance(pattern, unicode):
			return_type = unicode
		elif isinstance(pattern, bytes):
			return_type = bytes
			pattern = pattern.decode(_BYTES_ENCODING)
		else:
			raise TypeError("pattern:{!r} is not a unicode or byte string.".format(pattern))

		pattern = pattern.strip()

		if pattern.startswith('#'):
			# A pattern starting with a hash ('#') serves as a comment
			# (neither includes nor excludes files). Escape the hash with a
			# back-slash to match a literal hash (i.e., '\#').
			regex = None
			include = None

		elif pattern == '/':
			# EDGE CASE: According to `git check-ignore` (v2.4.1), a single
			# '/' does not match any file.
			regex = None
			include = None

		elif pattern:

			if pattern.startswith('!'):
				# A pattern starting with an exclamation mark ('!') negates the
				# pattern (exclude instead of include). Escape the exclamation
				# mark with a back-slash to match a literal exclamation mark
				# (i.e., '\!').
				include = False
				# Remove leading exclamation mark.
				pattern = pattern[1:]
			else:
				include = True

			if pattern.startswith('\\'):
				# Remove leading back-slash escape for escaped hash ('#') or
				# exclamation mark ('!').
				pattern = pattern[1:]

			# Split pattern into segments.
			pattern_segs = pattern.split('/')

			# Normalize pattern to make processing easier.

			if not pattern_segs[0]:
				# A pattern beginning with a slash ('/') will only match paths
				# directly on the root directory instead of any descendant
				# paths. So, remove empty first segment to make pattern relative
				# to root.
				del pattern_segs[0]

			elif len(pattern_segs) == 1 or (len(pattern_segs) == 2 and not pattern_segs[1]):
				# A single pattern without a beginning slash ('/') will match
				# any descendant path. This is equivalent to "**/{pattern}". So,
				# prepend with double-asterisks to make pattern relative to
				# root.
				# EDGE CASE: This also holds for a single pattern with a
				# trailing slash (e.g. dir/).
				if pattern_segs[0] != '**':
					pattern_segs.insert(0, '**')

			else:
				# EDGE CASE: A pattern without a beginning slash ('/') but
				# contains at least one prepended directory (e.g.
				# "dir/{pattern}") should not match "**/dir/{pattern}",
				# according to `git check-ignore` (v2.4.1).
				pass

			if not pattern_segs[-1] and len(pattern_segs) > 1:
				# A pattern ending with a slash ('/') will match all descendant
				# paths if it is a directory but not if it is a regular file.
				# This is equivilent to "{pattern}/**". So, set last segment to
				# double asterisks to include all descendants.
				pattern_segs[-1] = '**'

			# Build regular expression from pattern.
			output = ['^']
			need_slash = False
			end = len(pattern_segs) - 1
			for i, seg in enumerate(pattern_segs):
				if seg == '**':
					if i == 0 and i == end:
						# A pattern consisting solely of double-asterisks ('**')
						# will match every path.
						output.append('.+')
					elif i == 0:
						# A normalized pattern beginning with double-asterisks
						# ('**') will match any leading path segments.
						output.append('(?:.+/)?')
						need_slash = False
					elif i == end:
						# A normalized pattern ending with double-asterisks ('**')
						# will match any trailing path segments.
						output.append('/.*')
					else:
						# A pattern with inner double-asterisks ('**') will match
						# multiple (or zero) inner path segments.
						output.append('(?:/.+)?')
						need_slash = True
				elif seg == '*':
					# Match single path segment.
					if need_slash:
						output.append('/')
					output.append('[^/]+')
					need_slash = True
				else:
					# Match segment glob pattern.
					if need_slash:
						output.append('/')
					output.append(cls._translate_segment_glob(seg))
					if i == end and include is True:
						# A pattern ending without a slash ('/') will match a file
						# or a directory (with paths underneath it). E.g., "foo"
						# matches "foo", "foo/bar", "foo/bar/baz", etc.
						# EDGE CASE: However, this does not hold for exclusion cases
						# according to `git check-ignore` (v2.4.1).
						output.append('(?:/.*)?')
					need_slash = True
			output.append('$')
			regex = ''.join(output)

		else:
			# A blank pattern is a null-operation (neither includes nor
			# excludes files).
			regex = None
			include = None

		if regex is not None and return_type is bytes:
			regex = regex.encode(_BYTES_ENCODING)

		return regex, include

	@staticmethod
	def _translate_segment_glob(pattern):
		"""
		Translates the glob pattern to a regular expression. This is used in
		the constructor to translate a path segment glob pattern to its
		corresponding regular expression.

		*pattern* (:class:`str`) is the glob pattern.

		Returns the regular expression (:class:`str`).
		"""
		# NOTE: This is derived from `fnmatch.translate()` and is similar to
		# the POSIX function `fnmatch()` with the `FNM_PATHNAME` flag set.

		escape = False
		regex = ''
		i, end = 0, len(pattern)
		while i < end:
			# Get next character.
			char = pattern[i]
			i += 1

			if escape:
				# Escape the character.
				escape = False
				regex += re.escape(char)

			elif char == '\\':
				# Escape character, escape next character.
				escape = True

			elif char == '*':
				# Multi-character wildcard. Match any string (except slashes),
				# including an empty string.
				regex += '[^/]*'

			elif char == '?':
				# Single-character wildcard. Match any single character (except
				# a slash).
				regex += '[^/]'

			elif char == '[':
				# Braket expression wildcard. Except for the beginning
				# exclamation mark, the whole braket expression can be used
				# directly as regex but we have to find where the expression
				# ends.
				# - "[][!]" matchs ']', '[' and '!'.
				# - "[]-]" matchs ']' and '-'.
				# - "[!]a-]" matchs any character except ']', 'a' and '-'.
				j = i
				# Pass brack expression negation.
				if j < end and pattern[j] == '!':
					j += 1
				# Pass first closing braket if it is at the beginning of the
				# expression.
				if j < end and pattern[j] == ']':
					j += 1
				# Find closing braket. Stop once we reach the end or find it.
				while j < end and pattern[j] != ']':
					j += 1

				if j < end:
					# Found end of braket expression. Increment j to be one past
					# the closing braket:
					#
					#  [...]
					#   ^   ^
					#   i   j
					#
					j += 1
					expr = '['

					if pattern[i] == '!':
						# Braket expression needs to be negated.
						expr += '^'
						i += 1
					elif pattern[i] == '^':
						# POSIX declares that the regex braket expression negation
						# "[^...]" is undefined in a glob pattern. Python's
						# `fnmatch.translate()` escapes the caret ('^') as a
						# literal. To maintain consistency with undefined behavior,
						# I am escaping the '^' as well.
						expr += '\\^'
						i += 1

					# Build regex braket expression. Escape slashes so they are
					# treated as literal slashes by regex as defined by POSIX.
					expr += pattern[i:j].replace('\\', '\\\\')

					# Add regex braket expression to regex result.
					regex += expr

					# Set i to one past the closing braket.
					i = j

				else:
					# Failed to find closing braket, treat opening braket as a
					# braket literal instead of as an expression.
					regex += '\\['

			else:
				# Regular character, escape it for regex.
				regex += re.escape(char)

		return regex

	@staticmethod
	def escape(s):
		"""
		Escape special characters in the given string.

		*s* (:class:`unicode` or :class:`bytes`) a filename or a string
		that you want to escape, usually before adding it to a `.gitignore`

		Returns the escaped string (:class:`unicode`, :class:`bytes`)
		"""
		# Reference: https://git-scm.com/docs/gitignore#_pattern_format
		meta_characters = r"[]!*#?"

		return "".join("\\" + x if x in meta_characters else x for x in s)

util.register_pattern('gitwildmatch', GitWildMatchPattern)


class GitIgnorePattern(GitWildMatchPattern):
	"""
	The :class:`GitIgnorePattern` class is deprecated by :class:`GitWildMatchPattern`.
	This class only exists to maintain compatibility with v0.4.
	"""

	def __init__(self, *args, **kw):
		"""
		Warn about deprecation.
		"""
		self._deprecated()
		return super(GitIgnorePattern, self).__init__(*args, **kw)

	@staticmethod
	def _deprecated():
		"""
		Warn about deprecation.
		"""
		warnings.warn("GitIgnorePattern ('gitignore') is deprecated. Use GitWildMatchPattern ('gitwildmatch') instead.", DeprecationWarning, stacklevel=3)

	@classmethod
	def pattern_to_regex(cls, *args, **kw):
		"""
		Warn about deprecation.
		"""
		cls._deprecated()
		return super(GitIgnorePattern, cls).pattern_to_regex(*args, **kw)

# Register `GitIgnorePattern` as "gitignore" for backward compatibility
# with v0.4.
util.register_pattern('gitignore', GitIgnorePattern)
