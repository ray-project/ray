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

'''Information about version and extensions of current GL implementation.

Usage::
    
    from pyglet.gl import gl_info

    if gl_info.have_extension('GL_NV_register_combiners'):
        # ...

If you are using more than one context, you can set up a separate GLInfo
object for each context.  Call `set_active_context` after switching to the
context::

    from pyglet.gl.gl_info import GLInfo

    info = GLInfo()
    info.set_active_context()

    if info.have_version(2, 1):
        # ...

'''
from builtins import range
from builtins import object

__docformat__ = 'restructuredtext'
__version__ = '$Id: $'

from ctypes import *
import warnings

from pyglet.gl.gl import *
from pyglet.compat import asstr

class GLInfo(object):
    '''Information interface for a single GL context.

    A default instance is created automatically when the first OpenGL context
    is created.  You can use the module functions as a convenience for 
    this default instance's methods.

    If you are using more than one context, you must call `set_active_context`
    when the context is active for this `GLInfo` instance.
    '''
    have_context = False
    version = '0.0.0'
    vendor = ''
    renderer = ''
    extensions = set()

    _have_info = False

    def set_active_context(self):
        '''Store information for the currently active context.

        This method is called automatically for the default context.
        '''
        self.have_context = True
        if not self._have_info:
            self.vendor = asstr(cast(glGetString(GL_VENDOR), c_char_p).value)
            self.renderer = asstr(cast(glGetString(GL_RENDERER),
                                       c_char_p).value)
            self.version = asstr(cast(glGetString(GL_VERSION), c_char_p).value)
            if self.have_version(3):
                from pyglet.gl.glext_arb import glGetStringi, GL_NUM_EXTENSIONS
                num_extensions = GLint()
                glGetIntegerv(GL_NUM_EXTENSIONS, num_extensions)
                self.extensions = (asstr(cast(glGetStringi(GL_EXTENSIONS, i),
                                         c_char_p).value) for i in range(num_extensions.value))
            else:
                self.extensions = asstr(cast(glGetString(GL_EXTENSIONS),
                         c_char_p).value).split()
            if self.extensions:
                self.extensions = set(self.extensions)
            self._have_info = True

    def remove_active_context(self):
        self.have_context = False
        self._have_info = False

    def have_extension(self, extension):
        '''Determine if an OpenGL extension is available.

        :Parameters:
            `extension` : str
                The name of the extension to test for, including its
                ``GL_`` prefix.

        :return: True if the extension is provided by the driver.
        :rtype: bool
        '''
        if not self.have_context:
            warnings.warn('No GL context created yet.')
        return extension in self.extensions

    def get_extensions(self):
        '''Get a list of available OpenGL extensions.

        :return: a list of the available extensions.
        :rtype: list of str
        '''
        if not self.have_context:
            warnings.warn('No GL context created yet.')
        return self.extensions

    def get_version(self):
        '''Get the current OpenGL version.

        :return: the OpenGL version
        :rtype: str
        '''
        if not self.have_context:
            warnings.warn('No GL context created yet.')
        return self.version

    def have_version(self, major, minor=0, release=0):
        '''Determine if a version of OpenGL is supported.

        :Parameters:
            `major` : int
                The major revision number (typically 1 or 2).
            `minor` : int
                The minor revision number.
            `release` : int
                The release number.  

        :rtype: bool
        :return: True if the requested or a later version is supported.
        '''

        if not self.have_context:
            warnings.warn('No GL context created yet.')
        if 'None' in self.version: 
            return False 
        ver = '%s.0.0' % self.version.split(' ', 1)[0]
        imajor, iminor, irelease = [int(v) for v in ver.split('.', 3)[:3]]
        return imajor > major or \
           (imajor == major and iminor > minor) or \
           (imajor == major and iminor == minor and irelease >= release)

    def get_renderer(self):
        '''Determine the renderer string of the OpenGL context.

        :rtype: str
        '''
        if not self.have_context:
            warnings.warn('No GL context created yet.')
        return self.renderer

    def get_vendor(self):
        '''Determine the vendor string of the OpenGL context.

        :rtype: str
        '''
        if not self.have_context:
            warnings.warn('No GL context created yet.')
        return self.vendor

# Single instance useful for apps with only a single context (or all contexts
# have same GL driver, common case). 
_gl_info = GLInfo()

set_active_context = _gl_info.set_active_context
remove_active_context = _gl_info.remove_active_context
have_extension = _gl_info.have_extension
get_extensions = _gl_info.get_extensions
get_version = _gl_info.get_version
have_version = _gl_info.have_version
get_renderer = _gl_info.get_renderer
get_vendor = _gl_info.get_vendor

def have_context():
    '''Determine if a default OpenGL context has been set yet.

    :rtype: bool
    '''
    return _gl_info.have_context
