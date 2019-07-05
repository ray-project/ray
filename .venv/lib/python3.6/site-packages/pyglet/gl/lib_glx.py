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
__version__ = '$Id$'

from ctypes import *

import pyglet.lib
from pyglet.gl.lib import missing_function, decorate_function

from pyglet.compat import asbytes

__all__ = ['link_GL', 'link_GLU', 'link_GLX']

gl_lib = pyglet.lib.load_library('GL')
glu_lib = pyglet.lib.load_library('GLU')

# Look for glXGetProcAddressARB extension, use it as fallback (for
# ATI fglrx and DRI drivers).
try:
    glXGetProcAddressARB = getattr(gl_lib, 'glXGetProcAddressARB')
    glXGetProcAddressARB.restype = POINTER(CFUNCTYPE(None))
    glXGetProcAddressARB.argtypes = [POINTER(c_ubyte)]
    _have_getprocaddress = True
except AttributeError:
    _have_getprocaddress = False
    
def link_GL(name, restype, argtypes, requires=None, suggestions=None):
    try:
        func = getattr(gl_lib, name)
        func.restype = restype
        func.argtypes = argtypes
        decorate_function(func, name)
        return func
    except AttributeError:
        if _have_getprocaddress:
            # Fallback if implemented but not in ABI
            bname = cast(pointer(create_string_buffer(asbytes(name))), POINTER(c_ubyte))
            addr = glXGetProcAddressARB(bname)
            if addr:
                ftype = CFUNCTYPE(*((restype,) + tuple(argtypes)))
                func = cast(addr, ftype)
                decorate_function(func, name)
                return func

    return missing_function(name, requires, suggestions)

link_GLX = link_GL

def link_GLU(name, restype, argtypes, requires=None, suggestions=None):
    try:
        func = getattr(glu_lib, name)
        func.restype = restype
        func.argtypes = argtypes
        decorate_function(func, name)
        return func
    except AttributeError:
        return missing_function(name, requires, suggestions)

