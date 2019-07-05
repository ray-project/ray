from __future__ import absolute_import
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
# $Id:$

__docformat__ = 'restructuredtext'
__version__ = '$Id: $'

import ctypes

from pyglet import app
from .base import PlatformEventLoop

from pyglet.libs.win32 import _kernel32, _user32, types, constants
from pyglet.libs.win32.constants import *
from pyglet.libs.win32.types import *

class Win32EventLoop(PlatformEventLoop):
    def __init__(self):
        super(Win32EventLoop, self).__init__()

        self._next_idle_time = None

        # Force immediate creation of an event queue on this thread -- note
        # that since event loop is created on pyglet.app import, whatever
        # imports pyglet.app _must_ own the main run loop.
        msg = types.MSG()
        _user32.PeekMessageW(ctypes.byref(msg), 0,
                             constants.WM_USER, constants.WM_USER, 
                             constants.PM_NOREMOVE)

        self._event_thread = _kernel32.GetCurrentThreadId()

        self._wait_objects = []
        self._recreate_wait_objects_array()

        self._timer_proc = types.TIMERPROC(self._timer_proc_func)
        self._timer = _user32.SetTimer(
            0, 0, constants.USER_TIMER_MAXIMUM, self._timer_proc)

    def add_wait_object(self, object, func):
        self._wait_objects.append((object, func))
        self._recreate_wait_objects_array()

    def remove_wait_object(self, object):
        for i, (_object, _) in enumerate(self._wait_objects):
            if object == _object:
                del self._wait_objects[i]
                break
        self._recreate_wait_objects_array()

    def _recreate_wait_objects_array(self):
        if not self._wait_objects:
            self._wait_objects_n = 0
            self._wait_objects_array = None
            return

        self._wait_objects_n = len(self._wait_objects)
        self._wait_objects_array = \
            (HANDLE * self._wait_objects_n)(*[o for o, f in self._wait_objects])

    def start(self):
        if _kernel32.GetCurrentThreadId() != self._event_thread:
            raise RuntimeError('EventLoop.run() must be called from the same ' +
                               'thread that imports pyglet.app')

        self._timer_func = None
        self._polling = False
        self._allow_polling = True

    def step(self, timeout=None):
        self.dispatch_posted_events()

        msg = types.MSG()
        if timeout is None:
            timeout = constants.INFINITE
        else:
            timeout = int(timeout * 1000) # milliseconds

        result = _user32.MsgWaitForMultipleObjects(
            self._wait_objects_n,
            self._wait_objects_array,
            False,
            timeout,
            constants.QS_ALLINPUT)
        result -= constants.WAIT_OBJECT_0

        if result == self._wait_objects_n:
            while _user32.PeekMessageW(ctypes.byref(msg),
                                       0, 0, 0, constants.PM_REMOVE):
                _user32.TranslateMessage(ctypes.byref(msg))
                _user32.DispatchMessageW(ctypes.byref(msg))
        elif 0 <= result < self._wait_objects_n:
            object, func = self._wait_objects[result]
            func()

        # Return True if timeout was interrupted.
        return result <= self._wait_objects_n

    def notify(self):
        # Nudge the event loop with a message it will discard.  Note that only
        # user events are actually posted.  The posted event will not
        # interrupt the window move/size drag loop -- it seems there's no way
        # to do this.
        _user32.PostThreadMessageW(self._event_thread, constants.WM_USER, 0, 0)

    def set_timer(self, func, interval):
        if func is None or interval is None:
            interval = constants.USER_TIMER_MAXIMUM
        else:
            interval = int(interval * 1000) # milliseconds
        
        self._timer_func = func
        _user32.SetTimer(0, self._timer, interval, self._timer_proc)
     
    def _timer_proc_func(self, hwnd, msg, timer, t):
        if self._timer_func:
            self._timer_func()
