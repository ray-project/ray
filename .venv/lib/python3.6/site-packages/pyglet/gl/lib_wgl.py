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
from builtins import object

__docformat__ = 'restructuredtext'
__version__ = '$Id: lib_glx.py 597 2007-02-03 16:13:07Z Alex.Holkner $'

import ctypes
from ctypes import *

import pyglet
from pyglet.gl.lib import missing_function, decorate_function
from pyglet.compat import asbytes

__all__ = ['link_GL', 'link_GLU', 'link_WGL']

_debug_trace = pyglet.options['debug_trace']

gl_lib = ctypes.windll.opengl32
glu_lib = ctypes.windll.glu32
wgl_lib = gl_lib

if _debug_trace:
    from pyglet.lib import _TraceLibrary
    gl_lib = _TraceLibrary(gl_lib)
    glu_lib = _TraceLibrary(glu_lib)
    wgl_lib = _TraceLibrary(wgl_lib)

try:
    wglGetProcAddress = wgl_lib.wglGetProcAddress
    wglGetProcAddress.restype = CFUNCTYPE(POINTER(c_int))
    wglGetProcAddress.argtypes = [c_char_p]
    _have_get_proc_address = True
except AttributeError:
    _have_get_proc_address = False

class_slots = ['name', 'requires', 'suggestions', 'ftype','func']

def makeWGLFunction(func):
    class WGLFunction(object):
        __slots__ = class_slots
        __call__ = func
        
    return WGLFunction

class WGLFunctionProxy(object):
    __slots__ = class_slots

    def __init__(self, name, ftype, requires, suggestions):
        assert _have_get_proc_address
        self.name = name
        self.ftype = ftype
        self.requires = requires
        self.suggestions = suggestions
        self.func = None

    def __call__(self, *args, **kwargs):
        from pyglet.gl import current_context
        if not current_context:
            raise Exception(
                'Call to function "%s" before GL context created' % self.name)
        address = wglGetProcAddress(asbytes(self.name))
        if cast(address, POINTER(c_int)):  # check cast because address is func
            self.func = cast(address, self.ftype)
            decorate_function(self.func, self.name)
        else:
            self.func = missing_function(
                self.name, self.requires, self.suggestions)

        self.__class__ = makeWGLFunction(self.func)
        
        return self.func(*args, **kwargs)

def link_GL(name, restype, argtypes, requires=None, suggestions=None):
    try:
        func = getattr(gl_lib, name)
        func.restype = restype
        func.argtypes = argtypes
        decorate_function(func, name)
        return func
    except AttributeError:
        # Not in opengl32.dll. Try and get a pointer from WGL.
        try:
            fargs = (restype,) + tuple(argtypes)
            ftype = ctypes.WINFUNCTYPE(*fargs)
            if _have_get_proc_address:
                from pyglet.gl import gl_info
                if gl_info.have_context():
                    address = wglGetProcAddress(name)
                    if address:
                        func = cast(address, ftype)
                        decorate_function(func, name)
                        return func
                else:
                    # Insert proxy until we have a context
                    return WGLFunctionProxy(name, ftype, requires, suggestions)
        except:
            pass

        return missing_function(name, requires, suggestions)

def link_GLU(name, restype, argtypes, requires=None, suggestions=None):
    try:
        func = getattr(glu_lib, name)
        func.restype = restype
        func.argtypes = argtypes
        decorate_function(func, name)
        return func
    except AttributeError:
        # Not in glu32.dll. Try and get a pointer from WGL.
        try:
            fargs = (restype,) + tuple(argtypes)
            ftype = ctypes.WINFUNCTYPE(*fargs)
            if _have_get_proc_address:
                from pyglet.gl import gl_info
                if gl_info.have_context():
                    address = wglGetProcAddress(name)
                    if address:
                        func = cast(address, ftype)
                        decorate_function(func, name)
                        return func
                else:
                    # Insert proxy until we have a context
                    return WGLFunctionProxy(name, ftype, requires, suggestions)
        except:
            pass

        return missing_function(name, requires, suggestions)

link_WGL = link_GL