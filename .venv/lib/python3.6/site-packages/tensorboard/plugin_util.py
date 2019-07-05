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
"""Provides utilities that may be especially useful to plugins."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from tensorboard._vendor import bleach
# pylint: disable=g-bad-import-order
# Google-only: import markdown_freewisdom
import markdown
import six

_ALLOWED_ATTRIBUTES = {
    'a': ['href', 'title'],
    'img': ['src', 'title', 'alt'],
}

_ALLOWED_TAGS = [
    'ul',
    'ol',
    'li',
    'p',
    'pre',
    'code',
    'blockquote',
    'h1',
    'h2',
    'h3',
    'h4',
    'h5',
    'h6',
    'hr',
    'br',
    'strong',
    'em',
    'a',
    'img',
    'table',
    'thead',
    'tbody',
    'td',
    'tr',
    'th',
]


def markdown_to_safe_html(markdown_string):
  """Convert Markdown to HTML that's safe to splice into the DOM.

  Arguments:
    markdown_string: A Unicode string or UTF-8--encoded bytestring
      containing Markdown source. Markdown tables are supported.

  Returns:
    A string containing safe HTML.
  """
  warning = ''
  # Convert to utf-8 whenever we have a binary input.
  if isinstance(markdown_string, six.binary_type):
    markdown_string_decoded = markdown_string.decode('utf-8')
    # Remove null bytes and warn if there were any, since it probably means
    # we were given a bad encoding.
    markdown_string = markdown_string_decoded.replace(u'\x00', u'')
    num_null_bytes = len(markdown_string_decoded) - len(markdown_string)
    if num_null_bytes:
      warning = ('<!-- WARNING: discarded %d null bytes in markdown string '
                 'after UTF-8 decoding -->\n') % num_null_bytes

  string_html = markdown.markdown(
      markdown_string, extensions=['markdown.extensions.tables'])
  string_sanitized = bleach.clean(
      string_html, tags=_ALLOWED_TAGS, attributes=_ALLOWED_ATTRIBUTES)
  return warning + string_sanitized
