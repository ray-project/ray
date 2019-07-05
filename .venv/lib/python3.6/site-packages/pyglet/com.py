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

'''Minimal Windows COM interface.

Allows pyglet to use COM interfaces on Windows without comtypes.  Unlike
comtypes, this module does not provide property interfaces, read typelibs,
nice-ify return values or permit Python implementations of COM interfaces.  We
don't need anything that sophisticated to work with DirectX.

All interfaces should derive from IUnknown (defined in this module).  The
Python COM interfaces are actually pointers to the implementation (take note
when translating methods that take an interface as argument).

Interfaces can define methods::

    class IDirectSound8(com.IUnknown):
        _methods_ = [
            ('CreateSoundBuffer', com.STDMETHOD()),
            ('GetCaps', com.STDMETHOD(LPDSCAPS)),
            ...
        ]

Only use STDMETHOD or METHOD for the method types (not ordinary ctypes
function types).  The 'this' pointer is bound automatically... e.g., call::

    device = IDirectSound8()
    DirectSoundCreate8(None, ctypes.byref(device), None)

    caps = DSCAPS()
    device.GetCaps(caps)

Because STDMETHODs use HRESULT as the return type, there is no need to check
the return value.

Don't forget to manually manage memory... call Release() when you're done with
an interface.
'''
from builtins import object

import ctypes
import sys

from pyglet.debug import debug_print

_debug_com = debug_print('debug_com')

if sys.platform != 'win32':
    raise ImportError('pyglet.com requires a Windows build of Python')

class GUID(ctypes.Structure):
    _fields_ = [
        ('Data1', ctypes.c_ulong),
        ('Data2', ctypes.c_ushort),
        ('Data3', ctypes.c_ushort),
        ('Data4', ctypes.c_ubyte * 8)
    ]

    def __init__(self, l, w1, w2, b1, b2, b3, b4, b5, b6, b7, b8):
        self.Data1 = l
        self.Data2 = w1
        self.Data3 = w2
        self.Data4[:] = (b1, b2, b3, b4, b5, b6, b7, b8)

    def __repr__(self):
        b1, b2, b3, b4, b5, b6, b7, b8 = self.Data4
        return 'GUID(%x, %x, %x, %x, %x, %x, %x, %x, %x, %x, %x)' % (
            self.Data1, self.Data2, self.Data3, b1, b2, b3, b4, b5, b6, b7, b8)

LPGUID = ctypes.POINTER(GUID)
IID = GUID
REFIID = ctypes.POINTER(IID)

class METHOD(object):
    '''COM method.'''
    def __init__(self, restype, *args):
        self.restype = restype
        self.argtypes = args

    def get_field(self):
        return ctypes.WINFUNCTYPE(self.restype, *self.argtypes)

class STDMETHOD(METHOD):
    '''COM method with HRESULT return value.'''
    def __init__(self, *args):
        super(STDMETHOD, self).__init__(ctypes.HRESULT, *args)

class COMMethodInstance(object):
    '''Binds a COM interface method.'''
    def __init__(self, name, i, method):
        self.name = name
        self.i = i
        self.method = method

    def __get__(self, obj, tp):
        if obj is not None:
            def _call(*args):
                assert _debug_com('COM: IN {}({}, {})'.format(self.name, obj.__class__.__name__, args))
                ret = self.method.get_field()(self.i, self.name)(obj, *args)
                assert _debug_com('COM: OUT {}({}, {})'.format(self.name, obj.__class__.__name__, args))
                assert _debug_com('COM: RETURN {}'.format(ret))
                return ret
            return _call

        raise AttributeError()

class COMInterface(ctypes.Structure):
    '''Dummy struct to serve as the type of all COM pointers.'''
    _fields_ = [
        ('lpVtbl', ctypes.c_void_p),
    ]

class InterfaceMetaclass(type(ctypes.POINTER(COMInterface))):
    '''Creates COM interface pointers.'''
    def __new__(cls, name, bases, dct):
        methods = []
        for base in bases[::-1]:
            methods.extend(base.__dict__.get('_methods_', ()))
        methods.extend(dct.get('_methods_', ()))

        for i, (n, method) in enumerate(methods):
            dct[n] = COMMethodInstance(n, i, method)

        dct['_type_'] = COMInterface

        return super(InterfaceMetaclass, cls).__new__(cls, name, bases, dct)

# future.utils.with_metaclass does not work here, as the base class is from _ctypes.lib
# See https://wiki.python.org/moin/PortingToPy3k/BilingualQuickRef
Interface = InterfaceMetaclass(str('Interface'), (ctypes.POINTER(COMInterface),), {
    '__doc__': 'Base COM interface pointer.',
    })

class IUnknown(Interface):
    _methods_ = [
        ('QueryInterface', STDMETHOD(REFIID, ctypes.c_void_p)),
        ('AddRef', METHOD(ctypes.c_int)),
        ('Release', METHOD(ctypes.c_int))
    ]

