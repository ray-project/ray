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

'''Information about version and extensions of current GLU implementation.

Usage::

    from pyglet.gl import glu_info

    if glu_info.have_extension('GLU_EXT_nurbs_tessellator'):
        # ...

If multiple contexts are in use you can use a separate GLUInfo object for each
context.  Call `set_active_context` after switching to the desired context for
each GLUInfo::

    from pyglet.gl.glu_info import GLUInfo

    info = GLUInfo()
    info.set_active_context()
    if info.have_version(1, 3):
        # ...

Note that GLUInfo only returns meaningful information if a context has been
created.
'''
from builtins import object

__docformat__ = 'restructuredtext'
__version__ = '$Id$'

from ctypes import *
import warnings

from pyglet.gl.glu import *
from pyglet.compat import asstr

class GLUInfo(object):
    '''Information interface for the GLU library. 

    A default instance is created automatically when the first OpenGL context
    is created.  You can use the module functions as a convenience for 
    this default instance's methods.

    If you are using more than one context, you must call `set_active_context`
    when the context is active for this `GLUInfo` instance.
    '''
    have_context = False
    version = '0.0.0'
    extensions = []

    _have_info = False

    def set_active_context(self):
        '''Store information for the currently active context.

        This method is called automatically for the default context.
        '''
        self.have_context = True
        if not self._have_info:
            self.extensions = \
                asstr(cast(gluGetString(GLU_EXTENSIONS), c_char_p).value).split()
            self.version = asstr(cast(gluGetString(GLU_VERSION), c_char_p).value)
            self._have_info = True

    def have_version(self, major, minor=0, release=0):
        '''Determine if a version of GLU is supported.

        :Parameters:
            `major` : int
                The major revision number (typically 1).
            `minor` : int
                The minor revision number.
            `release` : int
                The release number.  

        :rtype: bool
        :return: True if the requested or a later version is supported.
        '''
        if not self.have_context:
            warnings.warn('No GL context created yet.')
        ver = '%s.0.0' % self.version.split(' ', 1)[0]
        imajor, iminor, irelease = [int(v) for v in ver.split('.', 3)[:3]]
        return imajor > major or \
           (imajor == major and iminor > minor) or \
           (imajor == major and iminor == minor and irelease >= release)

    def get_version(self):
        '''Get the current GLU version.

        :return: the GLU version
        :rtype: str
        '''
        if not self.have_context:
            warnings.warn('No GL context created yet.')
        return self.version

    def have_extension(self, extension):
        '''Determine if a GLU extension is available.

        :Parameters:
            `extension` : str
                The name of the extension to test for, including its
                ``GLU_`` prefix.

        :return: True if the extension is provided by the implementation.
        :rtype: bool
        '''
        if not self.have_context:
            warnings.warn('No GL context created yet.')
        return extension in self.extensions

    def get_extensions(self):
        '''Get a list of available GLU extensions.

        :return: a list of the available extensions.
        :rtype: list of str
        '''
        if not self.have_context:
            warnings.warn('No GL context created yet.')
        return self.extensions

# Single instance useful for apps with only a single context (or all contexts
# have same GLU driver, common case). 
_glu_info = GLUInfo()

set_active_context = _glu_info.set_active_context
have_version = _glu_info.have_version
get_version = _glu_info.get_version
have_extension = _glu_info.have_extension
get_extensions = _glu_info.get_extensions
