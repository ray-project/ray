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

'''Cached information about version and extensions of current WGL
implementation.
'''
from builtins import object

__docformat__ = 'restructuredtext'
__version__ = '$Id: glx_info.py 615 2007-02-07 13:17:05Z Alex.Holkner $'

from ctypes import *
import warnings

from pyglet.gl.lib import MissingFunctionException
from pyglet.gl.gl import *
from pyglet.gl import gl_info
from pyglet.gl.wgl import *
from pyglet.gl.wglext_arb import *
from pyglet.compat import asstr

class WGLInfoException(Exception):
    pass

class WGLInfo(object):
    def get_extensions(self):
        if not gl_info.have_context():
            warnings.warn("Can't query WGL until a context is created.")
            return []

        try:
            return asstr(wglGetExtensionsStringEXT()).split()
        except MissingFunctionException:
            return asstr(cast(glGetString(GL_EXTENSIONS), c_char_p).value).split()

    def have_extension(self, extension):
        return extension in self.get_extensions()

_wgl_info = WGLInfo()

get_extensions = _wgl_info.get_extensions
have_extension = _wgl_info.have_extension
