# ----------------------------------------------------------------------------
# pyglet
# Copyright (c) 2006-2008 Alex Holkner
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions
# are met:
#
#  * Redistributions of source code must retain the above copyright
#    notice, this list of conditions and the following disclaimer.
#  * Redistributions in binary form must reproduce the above copyright
#    notice, this list of conditions and the following disclaimer in
#    the documentation and/or other materials provided with the
#    distribution.
#  * Neither the name of pyglet nor the names of its
#    contributors may be used to endorse or promote products
#    derived from this software without specific prior written
#    permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
# "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
# LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
# FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
# COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
# INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
# BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
# LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
# ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.
# ----------------------------------------------------------------------------
from __future__ import print_function

import pyglet


def debug_print(enabled_or_option='debug'):
    """Get a debug printer that is enabled based on a boolean input or a pyglet option.
    The debug print function returned should be used in an assert. This way it can be
    optimized out when running python with the -O flag.

    Usage example::

        from pyglet.debug import debug_print
        _debug_media = debug_print('debug_media')

        def some_func():
            assert _debug_media('My debug statement')

    :parameters:
        `enabled_or_options` : bool or str
            If a bool is passed, debug printing is enabled if it is True. If str is passed
            debug printing is enabled if the pyglet option with that name is True.

    :returns: Function for debug printing.
    """
    if isinstance(enabled_or_option, bool):
        enabled = enabled_or_option
    else:
        enabled = pyglet.options.get(enabled_or_option, False)

    if enabled:
        def _debug_print(*args, **kwargs):
            print(*args, **kwargs)
            return True

    else:
        def _debug_print(*args, **kwargs):
            return True

    return _debug_print

