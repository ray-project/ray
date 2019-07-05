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

'''Display and screen management.

Rendering is performed on a :class:`Canvas`, which conceptually could be an
off-screen buffer, the content area of a :class:`pyglet.window.Window`, or an 
entire screen. Currently, canvases can only be created with windows (though 
windows can be set fullscreen).

Windows and canvases must belong to a :class`~pyglet.canvas.Display`  On Windows and Mac OS 
X there is only one display, which can be obtained with :func:`get_display`.  
Linux supports multiple displays, corresponding to discrete X11 display 
connections and screens.  :func:`get_display` on  Linux returns the default 
display and screen 0 (``localhost:0.0``); if a particular screen or display is 
required then :class`~pyglet.canvas.Display`can be instantiated directly.

Within a display one or more screens are attached.  A :class:`Screen` often
corresponds to a physical attached monitor, however a monitor or projector set
up to clone another screen will not be listed.  Use :meth:`Display.get_screens`
to get a list of the attached screens; these can then be queried for their 
sizes and virtual positions on the desktop.

The size of a screen is determined by its current mode, which can be changed
by the application; see the documentation for :class:`Screen`.

.. versionadded:: 1.2
'''

import sys
_is_pyglet_docgen = hasattr(sys, 'is_pyglet_docgen') and sys.is_pyglet_docgen

def get_display():
    '''Get the default display device.

    If there is already a :class`~pyglet.canvas.Display`connection, that display will be 
    returned. Otherwise, a default :class`~pyglet.canvas.Display`is created and returned.  
    If multiple display connections are active, an arbitrary one is returned.

    .. versionadded:: 1.2

    :rtype: :class`~pyglet.canvas.Display`
    '''
    # If there's an existing display, return it (return arbitrary display if
    # there are multiple).
    from pyglet.app import displays
    for display in displays:
        return display

    # Otherwise, create a new display and return it.
    return Display()

if _is_pyglet_docgen:
    from pyglet.canvas.base import Display, Screen, Canvas, ScreenMode
else:
    from pyglet import compat_platform
    if compat_platform == 'darwin':
        from pyglet import options as pyglet_options
        if pyglet_options['darwin_cocoa']:
            from pyglet.canvas.cocoa import CocoaDisplay as Display
            from pyglet.canvas.cocoa import CocoaScreen as Screen
            from pyglet.canvas.cocoa import CocoaCanvas as Canvas
        else:
            from pyglet.canvas.carbon import CarbonDisplay as Display
            from pyglet.canvas.carbon import CarbonScreen as Screen
            from pyglet.canvas.carbon import CarbonCanvas as Canvas
    elif compat_platform in ('win32', 'cygwin'):
        from pyglet.canvas.win32 import Win32Display as Display
        from pyglet.canvas.win32 import Win32Screen as Screen
        from pyglet.canvas.win32 import Win32Canvas as Canvas
    else:
        from pyglet.canvas.xlib import XlibDisplay as Display
        from pyglet.canvas.xlib import XlibScreen as Screen
        from pyglet.canvas.xlib import XlibCanvas as Canvas