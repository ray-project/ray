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

'''
'''

__docformat__ = 'restructuredtext'
__version__ = '$Id: $'

import ctypes

from pyglet import app
from pyglet.app.base import PlatformEventLoop

from pyglet.libs.darwin import *

EventLoopTimerProc = ctypes.CFUNCTYPE(None, ctypes.c_void_p, ctypes.c_void_p)


class CarbonEventLoop(PlatformEventLoop):
    def __init__(self):
        self._event_loop = carbon.GetMainEventLoop()
        self._timer = ctypes.c_void_p()
        self._timer_func = None
        self._timer_func_proc = EventLoopTimerProc(self._timer_proc)
        super(CarbonEventLoop, self).__init__()

    def notify(self):
        carbon.SetEventLoopTimerNextFireTime(
            self._timer, ctypes.c_double(0.0))

    def start(self):
        # Create timer
        timer = self._timer
        carbon.InstallEventLoopTimer(self._event_loop,
                                     ctypes.c_double(0.1), #?
                                     ctypes.c_double(kEventDurationForever),
                                     self._timer_func_proc,
                                     None,
                                     ctypes.byref(timer))

    def stop(self):
        carbon.RemoveEventLoopTimer(self._timer)

    def step(self, timeout=None):
        self.dispatch_posted_events()

        event_dispatcher = carbon.GetEventDispatcherTarget()
        e = ctypes.c_void_p()
        if timeout is None:
            timeout = kEventDurationForever
        self._is_running.set()
        # XXX should spin on multiple events after first timeout
        if carbon.ReceiveNextEvent(0, None, ctypes.c_double(timeout),
                                   True, ctypes.byref(e)) == 0:
            carbon.SendEventToEventTarget(e, event_dispatcher)
            carbon.ReleaseEvent(e)
            timed_out = False
        else:
            timed_out = True
        self._is_running.clear()

        return not timed_out

    def set_timer(self, func, interval):
        if interval is None or func is None:
            interval = kEventDurationForever

        self._timer_func = func
        carbon.SetEventLoopTimerNextFireTime(self._timer,
                                             ctypes.c_double(interval))

    def _timer_proc(self, timer, data):
        if self._timer_func:
            self._timer_func()
        
        '''
        self.dispatch_posted_events()

        allow_polling = True

        for window in app.windows:
            # Check for live resizing
            if window._resizing is not None:
                allow_polling = False
                old_width, old_height = window._resizing
                rect = Rect()
                carbon.GetWindowBounds(window._window, 
                                       kWindowContentRgn,
                                       ctypes.byref(rect))
                width = rect.right - rect.left
                height = rect.bottom - rect.top
                if width != old_width or height != old_height:
                    window._resizing = width, height
                    window.switch_to()
                    window.dispatch_event('on_resize', width, height) 
    
            # Check for live dragging
            if window._dragging:
                allow_polling = False

            # Check for deferred recreate
            if window._recreate_deferred:
                # Break out of ReceiveNextEvent so it can be processed
                # in next iteration.
                carbon.QuitEventLoop(self._event_loop)
                self._force_idle = True

        sleep_time = self.idle()

        if sleep_time is None:
            sleep_time = kEventDurationForever
        elif sleep_time < 0.01 and allow_polling and self._allow_polling:
            # Switch event loop to polling.
            carbon.QuitEventLoop(self._event_loop)
            self._force_idle = True
            sleep_time = kEventDurationForever
        carbon.SetEventLoopTimerNextFireTime(timer, ctypes.c_double(sleep_time))
        '''
