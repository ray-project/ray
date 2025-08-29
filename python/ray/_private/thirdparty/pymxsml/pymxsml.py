# 2025 - Modified by MetaX Integrated Circuits (Shanghai) Co., Ltd. All Rights Reserved.

#####
# Copyright (c) 2011-2021, NVIDIA Corporation.  All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
#    * Redistributions of source code must retain the above copyright notice,
#      this list of conditions and the following disclaimer.
#    * Redistributions in binary form must reproduce the above copyright
#      notice, this list of conditions and the following disclaimer in the
#      documentation and/or other materials provided with the distribution.
#    * Neither the name of the NVIDIA Corporation nor the names of its
#      contributors may be used to endorse or promote products derived from
#      this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
# THE POSSIBILITY OF SUCH DAMAGE.
#####

##
# Python bindings for the MXSML library
##
from ctypes import *
from ctypes.util import find_library
import sys
import os
import threading
import string


MXSML_SUCCESS                        = 0
MXSML_ERROR_UNINITIALIZED            = 1
MXSML_ERROR_INVALID_ARGUMENT         = 2
MXSML_ERROR_NOT_SUPPORTED            = 3
MXSML_ERROR_NO_PERMISSION            = 4
MXSML_ERROR_ALREADY_INITIALIZED      = 5
MXSML_ERROR_NOT_FOUND                = 6
MXSML_ERROR_INSUFFICIENT_SIZE        = 7
MXSML_ERROR_INSUFFICIENT_POWER       = 8
MXSML_ERROR_DRIVER_NOT_LOADED        = 9
MXSML_ERROR_TIMEOUT                  = 10
MXSML_ERROR_IRQ_ISSUE                = 11
MXSML_ERROR_LIBRARY_NOT_FOUND        = 12
MXSML_ERROR_FUNCTION_NOT_FOUND       = 13
MXSML_ERROR_CORRUPTED_INFOROM        = 14
MXSML_ERROR_GPU_IS_LOST              = 15
MXSML_ERROR_RESET_REQUIRED           = 16
MXSML_ERROR_OPERATING_SYSTEM         = 17
MXSML_ERROR_LIB_RM_VERSION_MISMATCH  = 18
MXSML_ERROR_IN_USE                   = 19
MXSML_ERROR_MEMORY                   = 20
MXSML_ERROR_NO_DATA                  = 21
MXSML_ERROR_VGPU_ECC_NOT_SUPPORTED   = 22
MXSML_ERROR_INSUFFICIENT_RESOURCES   = 23
MXSML_ERROR_FREQ_NOT_SUPPORTED       = 24
MXSML_ERROR_UNKNOWN                  = 999


# buffer size
MXSML_DEVICE_NAME_BUFFER_SIZE                 = 64

## Lib loading ##
mxsmlLib = None
libLoadLock = threading.Lock()
_mxsmlLib_refcount = 0 # Incremented on each mxsmlInit and decremented on mxsmlShutdown


## Device structures
class struct_c_mxsmlDevice_t(Structure):
    pass # opaque handle
c_mxsmlDevice_t = POINTER(struct_c_mxsmlDevice_t)

## Error Checking ##
class MXSMLError(Exception):
    _valClassMapping = dict()
    # List of currently known error codes
    _errcode_to_string = {
        MXSML_ERROR_UNINITIALIZED:       "Uninitialized",
        MXSML_ERROR_INVALID_ARGUMENT:    "Invalid Argument",
        MXSML_ERROR_NOT_SUPPORTED:       "Not Supported",
        MXSML_ERROR_NO_PERMISSION:       "Insufficient Permissions",
        MXSML_ERROR_ALREADY_INITIALIZED: "Already Initialized",
        MXSML_ERROR_NOT_FOUND:           "Not Found",
        MXSML_ERROR_INSUFFICIENT_SIZE:   "Insufficient Size",
        MXSML_ERROR_INSUFFICIENT_POWER:  "Insufficient External Power",
        MXSML_ERROR_DRIVER_NOT_LOADED:   "Driver Not Loaded",
        MXSML_ERROR_TIMEOUT:             "Timeout",
        MXSML_ERROR_IRQ_ISSUE:           "Interrupt Request Issue",
        MXSML_ERROR_LIBRARY_NOT_FOUND:   "MXSML Shared Library Not Found",
        MXSML_ERROR_FUNCTION_NOT_FOUND:  "Function Not Found",
        MXSML_ERROR_CORRUPTED_INFOROM:   "Corrupted infoROM",
        MXSML_ERROR_GPU_IS_LOST:         "GPU is lost",
        MXSML_ERROR_RESET_REQUIRED:      "GPU requires restart",
        MXSML_ERROR_OPERATING_SYSTEM:    "The operating system has blocked the request.",
        MXSML_ERROR_LIB_RM_VERSION_MISMATCH: "RM has detected an MXSML/RM version mismatch.",
        MXSML_ERROR_MEMORY:              "Insufficient Memory",
        MXSML_ERROR_UNKNOWN:             "Unknown Error",
        }
    def __new__(typ, value):
        '''
        Maps value to a proper subclass of MXSMLError.
        See _extractMXSMLErrorsAsClasses function for more details
        '''
        if typ == MXSMLError:
            typ = MXSMLError._valClassMapping.get(value, typ)
        obj = Exception.__new__(typ)
        obj.value = value
        return obj
    def __str__(self):
        try:
            if self.value not in MXSMLError._errcode_to_string:
                MXSMLError._errcode_to_string[self.value] = str(mxsmlErrorString(self.value))
            return MXSMLError._errcode_to_string[self.value]
        except MXSMLError:
            return "MXSML Error with code %d" % self.value
    def __eq__(self, other):
        return self.value == other.value

def mxsmlExceptionClass(mxsmlErrorCode):
    if mxsmlErrorCode not in MXSMLError._valClassMapping:
        raise ValueError('mxsmlErrorCode %s is not valid' % mxsmlErrorCode)
    return MXSMLError._valClassMapping[mxsmlErrorCode]

def _extractMXSMLErrorsAsClasses():
    '''
    Generates a hierarchy of classes on top of MXSMLError class.

    Each MXSML Error gets a new MXSMLError subclass. This way try,except blocks can filter appropriate
    exceptions more easily.

    MXSMLError is a parent class. Each MXSML_ERROR_* gets it's own subclass.
    e.g. MXSML_ERROR_ALREADY_INITIALIZED will be turned into MXSMLError_AlreadyInitialized
    '''
    this_module = sys.modules[__name__]
    mxsmlErrorsNames = [x for x in dir(this_module) if x.startswith("MXSML_ERROR_")]
    for err_name in mxsmlErrorsNames:
        # e.g. Turn MXSML_ERROR_ALREADY_INITIALIZED into MXSMLError_AlreadyInitialized
        class_name = "MXSMLError_" + string.capwords(err_name.replace("MXSML_ERROR_", ""), "_").replace("_", "")
        err_val = getattr(this_module, err_name)
        def gen_new(val):
            def new(typ):
                obj = MXSMLError.__new__(typ, val)
                return obj
            return new
        new_error_class = type(class_name, (MXSMLError,), {'__new__': gen_new(err_val)})
        new_error_class.__module__ = __name__
        setattr(this_module, class_name, new_error_class)
        MXSMLError._valClassMapping[err_val] = new_error_class
_extractMXSMLErrorsAsClasses()

def _mxsmlCheckReturn(ret):
    if (ret != MXSML_SUCCESS):
        raise MXSMLError(ret)
    return ret

## Function access ##
_mxsmlGetFunctionPointer_cache = dict() # function pointers are cached to prevent unnecessary libLoadLock locking
def _mxsmlGetFunctionPointer(name):
    global mxsmlLib

    if name in _mxsmlGetFunctionPointer_cache:
        return _mxsmlGetFunctionPointer_cache[name]

    libLoadLock.acquire()
    try:
        # ensure library was loaded
        if (mxsmlLib == None):
            raise MXSMLError(MXSML_ERROR_UNINITIALIZED)
        try:
            _mxsmlGetFunctionPointer_cache[name] = getattr(mxsmlLib, name)
            return _mxsmlGetFunctionPointer_cache[name]
        except AttributeError:
            raise MXSMLError(MXSML_ERROR_FUNCTION_NOT_FOUND)
    finally:
        # lock is always freed
        libLoadLock.release()

## C function wrappers ##
def mxsmlInitWithFlags(flags):
    _LoadMxsmlLibrary()

    #
    # Initialize the library
    #
    fn = _mxsmlGetFunctionPointer("mxSmlExInit")
    ret = fn(flags)
    _mxsmlCheckReturn(ret)

    # Atomically update refcount
    global _mxsmlLib_refcount
    libLoadLock.acquire()
    _mxsmlLib_refcount += 1
    libLoadLock.release()
    return None

def mxsmlInit():
    mxsmlInitWithFlags(0)
    return None

def _LoadMxsmlLibrary():
    '''
    Load the library if it isn't loaded already
    '''
    global mxsmlLib

    if (mxsmlLib == None):
        # lock to ensure only one caller loads the library
        libLoadLock.acquire()

        try:
            # ensure the library still isn't loaded
            if (mxsmlLib == None):
                try:
                    if (sys.platform[:3] == "win"):
                        # cdecl calling convention
                        try:
                            # Check for mxsml.dll in System32 first for DCH drivers
                            mxsmlLib = CDLL(os.path.join(os.getenv("WINDIR", "C:/Windows"), "System32/mxsml.dll"))
                        except OSError as ose:
                            _mxsmlCheckReturn(MXSML_ERROR_LIBRARY_NOT_FOUND)
                    else:
                        # assume linux
                        if (os.path.isfile("/opt/mxdriver/lib/libmxsml.so")):
                            mxsmlLib = CDLL("/opt/mxdriver/lib/libmxsml.so")
                        else:
                            mxsmlLib = CDLL("libmxsml.so")
                except OSError as ose:
                    _mxsmlCheckReturn(MXSML_ERROR_LIBRARY_NOT_FOUND)
                if (mxsmlLib == None):
                    _mxsmlCheckReturn(MXSML_ERROR_LIBRARY_NOT_FOUND)
        finally:
            # lock is always freed
            libLoadLock.release()

def mxsmlShutdown():
    #
    # Leave the library loaded, but shutdown the interface
    #
    fn = _mxsmlGetFunctionPointer("mxSmlExShutdown")
    ret = fn()
    _mxsmlCheckReturn(ret)

    # Atomically update refcount
    global _mxsmlLib_refcount
    libLoadLock.acquire()
    if (0 < _mxsmlLib_refcount):
        _mxsmlLib_refcount -= 1
    libLoadLock.release()
    return None

# Added in 2.285
def mxsmlErrorString(result):
    fn = _mxsmlGetFunctionPointer("mxSmlExErrorString")
    fn.restype = c_char_p # otherwise return is an int
    ret = fn(result)
    return ret
## Device get functions
def mxsmlDeviceGetCount():
    c_count = c_uint()
    fn = _mxsmlGetFunctionPointer("mxSmlExDeviceGetCount")
    ret = fn(byref(c_count))
    _mxsmlCheckReturn(ret)
    return c_count.value

def mxsmlDeviceGetHandleByIndex(index):
    c_index = c_uint(index)
    device = c_mxsmlDevice_t()
    fn = _mxsmlGetFunctionPointer("mxSmlExDeviceGetHandleByIndex")
    ret = fn(c_index, byref(device))
    _mxsmlCheckReturn(ret)
    return device


def mxsmlDeviceGetName(handle):
    c_name = create_string_buffer(MXSML_DEVICE_NAME_BUFFER_SIZE)
    fn = _mxsmlGetFunctionPointer("mxSmlExDeviceGetName")
    ret = fn(handle, c_name, c_uint(MXSML_DEVICE_NAME_BUFFER_SIZE))
    _mxsmlCheckReturn(ret)
    return c_name.value
