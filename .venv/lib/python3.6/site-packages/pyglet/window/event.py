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

"""Events for :py:mod:`pyglet.window`.

See :py:class:`~pyglet.window.Window` for a description of the window event types.
"""
from __future__ import print_function
from builtins import object

__docformat__ = 'restructuredtext'
__version__ = '$Id$'

import sys

from pyglet.window import key
from pyglet.window import mouse


class WindowExitHandler(object):
    """Determine if the window should be closed.

    This event handler watches for the ESC key or the window close event
    and sets `self.has_exit` to True when either is pressed.  An instance
    of this class is automatically attached to all new `pyglet.window.Window`
    objects.

    :deprecated: This class's functionality is provided directly on :py:class:`~pyglet.window.Window`
        in pyglet 1.1.
    """

    has_exit = False
    """True if the user wants to close the window."""

    def on_close(self):
        self.has_exit = True

    def on_key_press(self, symbol, modifiers):
        if symbol == key.ESCAPE:
            self.has_exit = True


class WindowEventLogger(object):
    """Print all events to a file.

    When this event handler is added to a window it prints out all events
    and their parameters; useful for debugging or discovering which events
    you need to handle.

    Example::

        win = window.Window()
        win.push_handlers(WindowEventLogger())

    """
    def __init__(self, logfile=None):
        """Create a `WindowEventLogger` which writes to `logfile`.

        :Parameters:
            `logfile` : file-like object
                The file to write to.  If unspecified, stdout will be used.

        """
        if logfile is None:
            logfile = sys.stdout
        self.file = logfile

    def on_key_press(self, symbol, modifiers):
        print('on_key_press(symbol=%s, modifiers=%s)' % (
            key.symbol_string(symbol), key.modifiers_string(modifiers)), file=self.file)

    def on_key_release(self, symbol, modifiers):
        print('on_key_release(symbol=%s, modifiers=%s)' % (
            key.symbol_string(symbol), key.modifiers_string(modifiers)), file=self.file)

    def on_text(self, text):
        print('on_text(text=%r)' % text, file=self.file)

    def on_text_motion(self, motion):
        print('on_text_motion(motion=%s)' % (
            key.motion_string(motion)), file=self.file)

    def on_text_motion_select(self, motion):
        print('on_text_motion_select(motion=%s)' % (
            key.motion_string(motion)), file=self.file)

    def on_mouse_motion(self, x, y, dx, dy):
        print('on_mouse_motion(x=%d, y=%d, dx=%d, dy=%d)' % (
            x, y, dx, dy), file=self.file)

    def on_mouse_drag(self, x, y, dx, dy, buttons, modifiers):
        print('on_mouse_drag(x=%d, y=%d, dx=%d, dy=%d, '\
                            'buttons=%s, modifiers=%s)' % (
              x, y, dx, dy,
              mouse.buttons_string(buttons), key.modifiers_string(modifiers)), file=self.file)

    def on_mouse_press(self, x, y, button, modifiers):
        print('on_mouse_press(x=%d, y=%d, button=%r, '\
                            'modifiers=%s)' % (x, y,
            mouse.buttons_string(button), key.modifiers_string(modifiers)), file=self.file)

    def on_mouse_release(self, x, y, button, modifiers):
        print('on_mouse_release(x=%d, y=%d, button=%r, '\
                            'modifiers=%s)' % (x, y, 
            mouse.buttons_string(button), key.modifiers_string(modifiers)), file=self.file)

    def on_mouse_scroll(self, x, y, dx, dy):
        print('on_mouse_scroll(x=%f, y=%f, dx=%f, dy=%f)' % (
            x, y, dx, dy), file=self.file)

    def on_close(self):
        print('on_close()', file=self.file)

    def on_mouse_enter(self, x, y):
        print('on_mouse_enter(x=%d, y=%d)' % (x, y), file=self.file)

    def on_mouse_leave(self, x, y):
        print('on_mouse_leave(x=%d, y=%d)' % (x, y), file=self.file)

    def on_expose(self):
        print('on_expose()', file=self.file)

    def on_resize(self, width, height):
        print('on_resize(width=%d, height=%d)' % (width, height), file=self.file)

    def on_move(self, x, y):
        print('on_move(x=%d, y=%d)' % (x, y), file=self.file)

    def on_activate(self):
        print('on_activate()', file=self.file)

    def on_deactivate(self):
        print('on_deactivate()', file=self.file)

    def on_show(self):
        print('on_show()', file=self.file)

    def on_hide(self):
        print('on_hide()', file=self.file)

    def on_context_lost(self):
        print('on_context_lost()', file=self.file)

    def on_context_state_lost(self):
        print('on_context_state_lost()', file=self.file)

    def on_draw(self):
        print('on_draw()', file=self.file)
