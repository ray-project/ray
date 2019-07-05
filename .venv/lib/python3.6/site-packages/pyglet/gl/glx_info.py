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

'''Information about version and extensions of current GLX implementation.

Usage::

    from pyglet.gl import glx_info

    if glx_info.have_extension('GLX_NV_float_buffer'):
        # ...

Or, if using more than one display::

    from pyglet.gl.glx_info import GLXInfo

    info = GLXInfo(window._display)
    if info.get_server_vendor() == 'ATI':
        # ...

'''
from builtins import object

__docformat__ = 'restructuredtext'
__version__ = '$Id$'

from ctypes import *

from pyglet.gl.glx import *
from pyglet.compat import asstr

class GLXInfoException(Exception):
    pass

class GLXInfo(object):
    def __init__(self, display=None):
        # Set default display if not set
        if display and not _glx_info.display:
            _glx_info.set_display(display)

        self.display = display

    def set_display(self, display):
        self.display = display

    def check_display(self):
        if not self.display:
            raise GLXInfoException('No X11 display has been set yet.')

    def have_version(self, major, minor=0):
        self.check_display()
        if not glXQueryExtension(self.display, None, None):
            raise GLXInfoException('pyglet requires an X server with GLX')

        server_version = self.get_server_version().split()[0]
        client_version = self.get_client_version().split()[0]

        server = [int(i) for i in server_version.split('.')]
        client = [int(i) for i in client_version.split('.')]
        return (tuple(server) >= (major, minor) and 
                tuple(client) >= (major, minor))

    def get_server_vendor(self):
        self.check_display()
        return asstr(glXQueryServerString(self.display, 0, GLX_VENDOR))
    
    def get_server_version(self):
        # glXQueryServerString was introduced in GLX 1.1, so we need to use the
        # 1.0 function here which queries the server implementation for its
        # version.
        self.check_display()
        major = c_int()
        minor = c_int()
        if not glXQueryVersion(self.display, byref(major), byref(minor)):
            raise GLXInfoException('Could not determine GLX server version')
        return '%s.%s'%(major.value, minor.value)

    def get_server_extensions(self):
        self.check_display()
        return asstr(glXQueryServerString(self.display, 0, GLX_EXTENSIONS)).split()

    def get_client_vendor(self):
        self.check_display()
        return asstr(glXGetClientString(self.display, GLX_VENDOR))

    def get_client_version(self):
        self.check_display()
        return asstr(glXGetClientString(self.display, GLX_VERSION))

    def get_client_extensions(self):
        self.check_display()
        return asstr(glXGetClientString(self.display, GLX_EXTENSIONS)).split()

    def get_extensions(self):
        self.check_display()
        return asstr(glXQueryExtensionsString(self.display, 0)).split()

    def have_extension(self, extension):
        self.check_display()
        if not self.have_version(1, 1):
            return False
        return extension in self.get_extensions()

# Single instance suitable for apps that use only a single display.
_glx_info = GLXInfo()

set_display = _glx_info.set_display
check_display = _glx_info.check_display
have_version = _glx_info.have_version
get_server_vendor = _glx_info.get_server_vendor
get_server_version = _glx_info.get_server_version
get_server_extensions = _glx_info.get_server_extensions
get_client_vendor = _glx_info.get_client_vendor
get_client_version = _glx_info.get_client_version
get_client_extensions = _glx_info.get_client_extensions
get_extensions = _glx_info.get_extensions
have_extension = _glx_info.have_extension
