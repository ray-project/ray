# objective-ctypes
#
# Copyright (c) 2011, Phillip Nguyen
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions
# are met:
#
# Redistributions of source code must retain the above copyright
# notice, this list of conditions and the following disclaimer.
# Redistributions in binary form must reproduce the above copyright
# notice, this list of conditions and the following disclaimer in the
# documentation and/or other materials provided with the distribution.
# Neither the name of objective-ctypes nor the names of its
# contributors may be used to endorse or promote products derived from
# this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
# "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
# LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
# FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
# COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
# INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
# BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
# LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
# ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.

import sys
import platform
import struct

from ctypes import *
from ctypes import util

from .cocoatypes import *

__LP64__ = (8*struct.calcsize("P") == 64)
__i386__ = (platform.machine() == 'i386')

if sizeof(c_void_p) == 4:
    c_ptrdiff_t = c_int32
elif sizeof(c_void_p) == 8:
    c_ptrdiff_t = c_int64

######################################################################

objc = cdll.LoadLibrary(util.find_library('objc'))

######################################################################

# BOOL class_addIvar(Class cls, const char *name, size_t size, uint8_t alignment, const char *types)
objc.class_addIvar.restype = c_bool
objc.class_addIvar.argtypes = [c_void_p, c_char_p, c_size_t, c_uint8, c_char_p]

# BOOL class_addMethod(Class cls, SEL name, IMP imp, const char *types)
objc.class_addMethod.restype = c_bool

# BOOL class_addProtocol(Class cls, Protocol *protocol)
objc.class_addProtocol.restype = c_bool
objc.class_addProtocol.argtypes = [c_void_p, c_void_p]

# BOOL class_conformsToProtocol(Class cls, Protocol *protocol)
objc.class_conformsToProtocol.restype = c_bool
objc.class_conformsToProtocol.argtypes = [c_void_p, c_void_p]

# Ivar * class_copyIvarList(Class cls, unsigned int *outCount)
# Returns an array of pointers of type Ivar describing instance variables.
# The array has *outCount pointers followed by a NULL terminator.
# You must free() the returned array.
objc.class_copyIvarList.restype = POINTER(c_void_p)
objc.class_copyIvarList.argtypes = [c_void_p, POINTER(c_uint)]

# Method * class_copyMethodList(Class cls, unsigned int *outCount)
# Returns an array of pointers of type Method describing instance methods.
# The array has *outCount pointers followed by a NULL terminator.
# You must free() the returned array.
objc.class_copyMethodList.restype = POINTER(c_void_p)
objc.class_copyMethodList.argtypes = [c_void_p, POINTER(c_uint)]

# objc_property_t * class_copyPropertyList(Class cls, unsigned int *outCount)
# Returns an array of pointers of type objc_property_t describing properties.
# The array has *outCount pointers followed by a NULL terminator.
# You must free() the returned array.
objc.class_copyPropertyList.restype = POINTER(c_void_p)
objc.class_copyPropertyList.argtypes = [c_void_p, POINTER(c_uint)]

# Protocol ** class_copyProtocolList(Class cls, unsigned int *outCount)
# Returns an array of pointers of type Protocol* describing protocols.
# The array has *outCount pointers followed by a NULL terminator.
# You must free() the returned array.
objc.class_copyProtocolList.restype = POINTER(c_void_p)
objc.class_copyProtocolList.argtypes = [c_void_p, POINTER(c_uint)]

# id class_createInstance(Class cls, size_t extraBytes)
objc.class_createInstance.restype = c_void_p
objc.class_createInstance.argtypes = [c_void_p, c_size_t]

# Method class_getClassMethod(Class aClass, SEL aSelector)
# Will also search superclass for implementations.
objc.class_getClassMethod.restype = c_void_p
objc.class_getClassMethod.argtypes = [c_void_p, c_void_p]

# Ivar class_getClassVariable(Class cls, const char* name)
objc.class_getClassVariable.restype = c_void_p
objc.class_getClassVariable.argtypes = [c_void_p, c_char_p]

# Method class_getInstanceMethod(Class aClass, SEL aSelector)
# Will also search superclass for implementations.
objc.class_getInstanceMethod.restype = c_void_p
objc.class_getInstanceMethod.argtypes = [c_void_p, c_void_p]

# size_t class_getInstanceSize(Class cls)
objc.class_getInstanceSize.restype = c_size_t
objc.class_getInstanceSize.argtypes = [c_void_p]

# Ivar class_getInstanceVariable(Class cls, const char* name)
objc.class_getInstanceVariable.restype = c_void_p
objc.class_getInstanceVariable.argtypes = [c_void_p, c_char_p]

# const char *class_getIvarLayout(Class cls)
objc.class_getIvarLayout.restype = c_char_p
objc.class_getIvarLayout.argtypes = [c_void_p]

# IMP class_getMethodImplementation(Class cls, SEL name)
objc.class_getMethodImplementation.restype = c_void_p
objc.class_getMethodImplementation.argtypes = [c_void_p, c_void_p]

# IMP class_getMethodImplementation_stret(Class cls, SEL name)
objc.class_getMethodImplementation_stret.restype = c_void_p
objc.class_getMethodImplementation_stret.argtypes = [c_void_p, c_void_p]

# const char * class_getName(Class cls)
objc.class_getName.restype = c_char_p
objc.class_getName.argtypes = [c_void_p]

# objc_property_t class_getProperty(Class cls, const char *name)
objc.class_getProperty.restype = c_void_p
objc.class_getProperty.argtypes = [c_void_p, c_char_p]

# Class class_getSuperclass(Class cls)
objc.class_getSuperclass.restype = c_void_p
objc.class_getSuperclass.argtypes = [c_void_p]

# int class_getVersion(Class theClass)
objc.class_getVersion.restype = c_int
objc.class_getVersion.argtypes = [c_void_p]

# const char *class_getWeakIvarLayout(Class cls)
objc.class_getWeakIvarLayout.restype = c_char_p
objc.class_getWeakIvarLayout.argtypes = [c_void_p]

# BOOL class_isMetaClass(Class cls)
objc.class_isMetaClass.restype = c_bool
objc.class_isMetaClass.argtypes = [c_void_p]

# IMP class_replaceMethod(Class cls, SEL name, IMP imp, const char *types)
objc.class_replaceMethod.restype = c_void_p
objc.class_replaceMethod.argtypes = [c_void_p, c_void_p, c_void_p, c_char_p]

# BOOL class_respondsToSelector(Class cls, SEL sel)
objc.class_respondsToSelector.restype = c_bool
objc.class_respondsToSelector.argtypes = [c_void_p, c_void_p]

# void class_setIvarLayout(Class cls, const char *layout)
objc.class_setIvarLayout.restype = None
objc.class_setIvarLayout.argtypes = [c_void_p, c_char_p]

# Class class_setSuperclass(Class cls, Class newSuper)
objc.class_setSuperclass.restype = c_void_p
objc.class_setSuperclass.argtypes = [c_void_p, c_void_p]

# void class_setVersion(Class theClass, int version)
objc.class_setVersion.restype = None
objc.class_setVersion.argtypes = [c_void_p, c_int]

# void class_setWeakIvarLayout(Class cls, const char *layout)
objc.class_setWeakIvarLayout.restype = None
objc.class_setWeakIvarLayout.argtypes = [c_void_p, c_char_p]

######################################################################

# const char * ivar_getName(Ivar ivar)
objc.ivar_getName.restype = c_char_p
objc.ivar_getName.argtypes = [c_void_p]

# ptrdiff_t ivar_getOffset(Ivar ivar)
objc.ivar_getOffset.restype = c_ptrdiff_t
objc.ivar_getOffset.argtypes = [c_void_p]

# const char * ivar_getTypeEncoding(Ivar ivar)
objc.ivar_getTypeEncoding.restype = c_char_p
objc.ivar_getTypeEncoding.argtypes = [c_void_p]

######################################################################

# char * method_copyArgumentType(Method method, unsigned int index)
# You must free() the returned string.
objc.method_copyArgumentType.restype = c_char_p
objc.method_copyArgumentType.argtypes = [c_void_p, c_uint]

# char * method_copyReturnType(Method method)
# You must free() the returned string.
objc.method_copyReturnType.restype = c_char_p
objc.method_copyReturnType.argtypes = [c_void_p]

# void method_exchangeImplementations(Method m1, Method m2)
objc.method_exchangeImplementations.restype = None
objc.method_exchangeImplementations.argtypes = [c_void_p, c_void_p]

# void method_getArgumentType(Method method, unsigned int index, char *dst, size_t dst_len)
# Functionally similar to strncpy(dst, parameter_type, dst_len).
objc.method_getArgumentType.restype = None
objc.method_getArgumentType.argtypes = [c_void_p, c_uint, c_char_p, c_size_t]

# IMP method_getImplementation(Method method)
objc.method_getImplementation.restype = c_void_p
objc.method_getImplementation.argtypes = [c_void_p]

# SEL method_getName(Method method)
objc.method_getName.restype = c_void_p
objc.method_getName.argtypes = [c_void_p]

# unsigned method_getNumberOfArguments(Method method)
objc.method_getNumberOfArguments.restype = c_uint
objc.method_getNumberOfArguments.argtypes = [c_void_p]

# void method_getReturnType(Method method, char *dst, size_t dst_len)
# Functionally similar to strncpy(dst, return_type, dst_len)
objc.method_getReturnType.restype = None
objc.method_getReturnType.argtypes = [c_void_p, c_char_p, c_size_t]

# const char * method_getTypeEncoding(Method method)
objc.method_getTypeEncoding.restype = c_char_p
objc.method_getTypeEncoding.argtypes = [c_void_p]

# IMP method_setImplementation(Method method, IMP imp)
objc.method_setImplementation.restype = c_void_p
objc.method_setImplementation.argtypes = [c_void_p, c_void_p]

######################################################################

# Class objc_allocateClassPair(Class superclass, const char *name, size_t extraBytes)
objc.objc_allocateClassPair.restype = c_void_p
objc.objc_allocateClassPair.argtypes = [c_void_p, c_char_p, c_size_t]

# Protocol **objc_copyProtocolList(unsigned int *outCount)
# Returns an array of *outcount pointers followed by NULL terminator.
# You must free() the array.
objc.objc_copyProtocolList.restype = POINTER(c_void_p)
objc.objc_copyProtocolList.argtypes = [POINTER(c_int)]

# id objc_getAssociatedObject(id object, void *key)
objc.objc_getAssociatedObject.restype = c_void_p
objc.objc_getAssociatedObject.argtypes = [c_void_p, c_void_p]

# id objc_getClass(const char *name)
objc.objc_getClass.restype = c_void_p
objc.objc_getClass.argtypes = [c_char_p]

# int objc_getClassList(Class *buffer, int bufferLen)
# Pass None for buffer to obtain just the total number of classes.
objc.objc_getClassList.restype = c_int
objc.objc_getClassList.argtypes = [c_void_p, c_int]

# id objc_getMetaClass(const char *name)
objc.objc_getMetaClass.restype = c_void_p
objc.objc_getMetaClass.argtypes = [c_char_p]

# Protocol *objc_getProtocol(const char *name)
objc.objc_getProtocol.restype = c_void_p
objc.objc_getProtocol.argtypes = [c_char_p]

# You should set return and argument types depending on context.
# id objc_msgSend(id theReceiver, SEL theSelector, ...)
# id objc_msgSendSuper(struct objc_super *super, SEL op,  ...)

# void objc_msgSendSuper_stret(struct objc_super *super, SEL op, ...)
objc.objc_msgSendSuper_stret.restype = None

# double objc_msgSend_fpret(id self, SEL op, ...)
# objc.objc_msgSend_fpret.restype = c_double

# void objc_msgSend_stret(void * stretAddr, id theReceiver, SEL theSelector,  ...)
objc.objc_msgSend_stret.restype = None

# void objc_registerClassPair(Class cls)
objc.objc_registerClassPair.restype = None
objc.objc_registerClassPair.argtypes = [c_void_p]

# void objc_removeAssociatedObjects(id object)
objc.objc_removeAssociatedObjects.restype = None
objc.objc_removeAssociatedObjects.argtypes = [c_void_p]

# void objc_setAssociatedObject(id object, void *key, id value, objc_AssociationPolicy policy)
objc.objc_setAssociatedObject.restype = None
objc.objc_setAssociatedObject.argtypes = [c_void_p, c_void_p, c_void_p, c_int]

######################################################################

# id object_copy(id obj, size_t size)
objc.object_copy.restype = c_void_p
objc.object_copy.argtypes = [c_void_p, c_size_t]

# id object_dispose(id obj)
objc.object_dispose.restype = c_void_p
objc.object_dispose.argtypes = [c_void_p]

# Class object_getClass(id object)
objc.object_getClass.restype = c_void_p
objc.object_getClass.argtypes = [c_void_p]

# const char *object_getClassName(id obj)
objc.object_getClassName.restype = c_char_p
objc.object_getClassName.argtypes = [c_void_p]

# Ivar object_getInstanceVariable(id obj, const char *name, void **outValue)
objc.object_getInstanceVariable.restype = c_void_p
objc.object_getInstanceVariable.argtypes=[c_void_p, c_char_p, c_void_p]

# id object_getIvar(id object, Ivar ivar)
objc.object_getIvar.restype = c_void_p
objc.object_getIvar.argtypes = [c_void_p, c_void_p]

# Class object_setClass(id object, Class cls)
objc.object_setClass.restype = c_void_p
objc.object_setClass.argtypes = [c_void_p, c_void_p]

# Ivar object_setInstanceVariable(id obj, const char *name, void *value)
# Set argtypes based on the data type of the instance variable.
objc.object_setInstanceVariable.restype = c_void_p

# void object_setIvar(id object, Ivar ivar, id value)
objc.object_setIvar.restype = None
objc.object_setIvar.argtypes = [c_void_p, c_void_p, c_void_p]

######################################################################

# const char *property_getAttributes(objc_property_t property)
objc.property_getAttributes.restype = c_char_p
objc.property_getAttributes.argtypes = [c_void_p]

# const char *property_getName(objc_property_t property)
objc.property_getName.restype = c_char_p
objc.property_getName.argtypes = [c_void_p]

######################################################################

# BOOL protocol_conformsToProtocol(Protocol *proto, Protocol *other)
objc.protocol_conformsToProtocol.restype = c_bool
objc.protocol_conformsToProtocol.argtypes = [c_void_p, c_void_p]

class OBJC_METHOD_DESCRIPTION(Structure):
    _fields_ = [ ("name", c_void_p), ("types", c_char_p) ]

# struct objc_method_description *protocol_copyMethodDescriptionList(Protocol *p, BOOL isRequiredMethod, BOOL isInstanceMethod, unsigned int *outCount)
# You must free() the returned array.
objc.protocol_copyMethodDescriptionList.restype = POINTER(OBJC_METHOD_DESCRIPTION)
objc.protocol_copyMethodDescriptionList.argtypes = [c_void_p, c_bool, c_bool, POINTER(c_uint)]

# objc_property_t * protocol_copyPropertyList(Protocol *protocol, unsigned int *outCount)
objc.protocol_copyPropertyList.restype = c_void_p
objc.protocol_copyPropertyList.argtypes = [c_void_p, POINTER(c_uint)]

# Protocol **protocol_copyProtocolList(Protocol *proto, unsigned int *outCount)
objc.protocol_copyProtocolList = POINTER(c_void_p)
objc.protocol_copyProtocolList.argtypes = [c_void_p, POINTER(c_uint)]

# struct objc_method_description protocol_getMethodDescription(Protocol *p, SEL aSel, BOOL isRequiredMethod, BOOL isInstanceMethod)
objc.protocol_getMethodDescription.restype = OBJC_METHOD_DESCRIPTION
objc.protocol_getMethodDescription.argtypes = [c_void_p, c_void_p, c_bool, c_bool]

# const char *protocol_getName(Protocol *p)
objc.protocol_getName.restype = c_char_p
objc.protocol_getName.argtypes = [c_void_p]

######################################################################

# const char* sel_getName(SEL aSelector)
objc.sel_getName.restype = c_char_p
objc.sel_getName.argtypes = [c_void_p]

# SEL sel_getUid(const char *str)
# Use sel_registerName instead.

# BOOL sel_isEqual(SEL lhs, SEL rhs)
objc.sel_isEqual.restype = c_bool
objc.sel_isEqual.argtypes = [c_void_p, c_void_p]

# SEL sel_registerName(const char *str)
objc.sel_registerName.restype = c_void_p
objc.sel_registerName.argtypes = [c_char_p]

######################################################################

def ensure_bytes(x):
    if isinstance(x, bytes):
        return x
    return x.encode('ascii')

######################################################################

def get_selector(name):
    return c_void_p(objc.sel_registerName(ensure_bytes(name)))

def get_class(name):
    return c_void_p(objc.objc_getClass(ensure_bytes(name)))

def get_object_class(obj):
    return c_void_p(objc.object_getClass(obj))

def get_metaclass(name):
    return c_void_p(objc.objc_getMetaClass(ensure_bytes(name)))

def get_superclass_of_object(obj):
    cls = c_void_p(objc.object_getClass(obj))
    return c_void_p(objc.class_getSuperclass(cls))


# http://www.sealiesoftware.com/blog/archive/2008/10/30/objc_explain_objc_msgSend_stret.html
# http://www.x86-64.org/documentation/abi-0.99.pdf  (pp.17-23)
# executive summary: on x86-64, who knows?
def x86_should_use_stret(restype):
    """Try to figure out when a return type will be passed on stack."""
    if type(restype) != type(Structure):
        return False
    if not __LP64__ and sizeof(restype) <= 8:
        return False
    if __LP64__ and sizeof(restype) <= 16:  # maybe? I don't know?
        return False
    return True

# http://www.sealiesoftware.com/blog/archive/2008/11/16/objc_explain_objc_msgSend_fpret.html
def should_use_fpret(restype):
    """Determine if objc_msgSend_fpret is required to return a floating point type."""
    if not __i386__: 
        # Unneeded on non-intel processors
        return False
    if __LP64__ and restype == c_longdouble:
        # Use only for long double on x86_64
        return True
    if not __LP64__ and restype in (c_float, c_double, c_longdouble):
        return True
    return False

# By default, assumes that restype is c_void_p
# and that all arguments are wrapped inside c_void_p.
# Use the restype and argtypes keyword arguments to 
# change these values.  restype should be a ctypes type
# and argtypes should be a list of ctypes types for
# the arguments of the message only.
def send_message(receiver, selName, *args, **kwargs):
    if isinstance(receiver, str):
        receiver = get_class(receiver)
    selector = get_selector(selName)
    restype = kwargs.get('restype', c_void_p)
    #print 'send_message', receiver, selName, args, kwargs
    argtypes = kwargs.get('argtypes', [])
    # Choose the correct version of objc_msgSend based on return type.
    if should_use_fpret(restype):
        objc.objc_msgSend_fpret.restype = restype
        objc.objc_msgSend_fpret.argtypes = [c_void_p, c_void_p] + argtypes
        result = objc.objc_msgSend_fpret(receiver, selector, *args)
    elif x86_should_use_stret(restype):
        objc.objc_msgSend_stret.argtypes = [POINTER(restype), c_void_p, c_void_p] + argtypes
        result = restype()
        objc.objc_msgSend_stret(byref(result), receiver, selector, *args)
    else:
        objc.objc_msgSend.restype = restype
        objc.objc_msgSend.argtypes = [c_void_p, c_void_p] + argtypes
        result = objc.objc_msgSend(receiver, selector, *args)
        if restype == c_void_p:
            result = c_void_p(result)
    return result

class OBJC_SUPER(Structure):
    _fields_ = [ ('receiver', c_void_p), ('class', c_void_p) ]

OBJC_SUPER_PTR = POINTER(OBJC_SUPER)

#http://stackoverflow.com/questions/3095360/what-exactly-is-super-in-objective-c
def send_super(receiver, selName, *args, **kwargs):
    #print 'send_super', receiver, selName, args
    if hasattr(receiver, '_as_parameter_'):
        receiver = receiver._as_parameter_
    superclass = get_superclass_of_object(receiver)
    super_struct = OBJC_SUPER(receiver, superclass)
    selector = get_selector(selName)
    restype = kwargs.get('restype', c_void_p)
    argtypes = kwargs.get('argtypes', None)
    objc.objc_msgSendSuper.restype = restype
    if argtypes:
        objc.objc_msgSendSuper.argtypes = [OBJC_SUPER_PTR, c_void_p] + argtypes
    else:
        objc.objc_msgSendSuper.argtypes = None
    result = objc.objc_msgSendSuper(byref(super_struct), selector, *args)
    if restype == c_void_p:
        result = c_void_p(result)
    return result

######################################################################

cfunctype_table = {}

def parse_type_encoding(encoding):
    """Takes a type encoding string and outputs a list of the separated type codes.
    Currently does not handle unions or bitfields and strips out any field width
    specifiers or type specifiers from the encoding.  For Python 3.2+, encoding is
    assumed to be a bytes object and not unicode.

    Examples:
    parse_type_encoding('^v16@0:8') --> ['^v', '@', ':']
    parse_type_encoding('{CGSize=dd}40@0:8{CGSize=dd}16Q32') --> ['{CGSize=dd}', '@', ':', '{CGSize=dd}', 'Q']
    """
    type_encodings = []
    brace_count = 0    # number of unclosed curly braces
    bracket_count = 0  # number of unclosed square brackets
    typecode = b''
    for c in encoding:
        # In Python 3, c comes out as an integer in the range 0-255.  In Python 2, c is a single character string.
        # To fix the disparity, we convert c to a bytes object if necessary.
        if isinstance(c, int):
            c = bytes([c])
            
        if c == b'{':
            # Check if this marked the end of previous type code.
            if typecode and typecode[-1:] != b'^' and brace_count == 0 and bracket_count == 0:
                type_encodings.append(typecode)
                typecode = b''
            typecode += c
            brace_count += 1
        elif c == b'}':
            typecode += c
            brace_count -= 1
            assert(brace_count >= 0)
        elif c == b'[':
            # Check if this marked the end of previous type code.
            if typecode and typecode[-1:] != b'^' and brace_count == 0 and bracket_count == 0:
                type_encodings.append(typecode)
                typecode = b''
            typecode += c
            bracket_count += 1
        elif c == b']':
            typecode += c
            bracket_count -= 1
            assert(bracket_count >= 0)
        elif brace_count or bracket_count:
            # Anything encountered while inside braces or brackets gets stuck on.
            typecode += c
        elif c in b'0123456789':
            # Ignore field width specifiers for now.
            pass
        elif c in b'rnNoORV':
            # Also ignore type specifiers.
            pass
        elif c in b'^cislqCISLQfdBv*@#:b?':
            if typecode and typecode[-1:] == b'^':
                # Previous char was pointer specifier, so keep going.
                typecode += c
            else:
                # Add previous type code to the list.
                if typecode:
                    type_encodings.append(typecode)
                # Start a new type code.
                typecode = c
            
    # Add the last type code to the list
    if typecode:
        type_encodings.append(typecode)

    return type_encodings


# Limited to basic types and pointers to basic types.
# Does not try to handle arrays, arbitrary structs, unions, or bitfields.
# Assume that encoding is a bytes object and not unicode.
def cfunctype_for_encoding(encoding):
    # Check if we've already created a CFUNCTYPE for this encoding.
    # If so, then return the cached CFUNCTYPE.
    if encoding in cfunctype_table:
        return cfunctype_table[encoding]

    # Otherwise, create a new CFUNCTYPE for the encoding.
    typecodes = {b'c':c_char, b'i':c_int, b's':c_short, b'l':c_long, b'q':c_longlong, 
                 b'C':c_ubyte, b'I':c_uint, b'S':c_ushort, b'L':c_ulong, b'Q':c_ulonglong, 
                 b'f':c_float, b'd':c_double, b'B':c_bool, b'v':None, b'*':c_char_p,
                 b'@':c_void_p, b'#':c_void_p, b':':c_void_p, NSPointEncoding:NSPoint,
                 NSSizeEncoding:NSSize, NSRectEncoding:NSRect, NSRangeEncoding:NSRange,
                 PyObjectEncoding:py_object}
    argtypes = []
    for code in parse_type_encoding(encoding):
        if code in typecodes:
            argtypes.append(typecodes[code])
        elif code[0:1] == b'^' and code[1:] in typecodes:
            argtypes.append(POINTER(typecodes[code[1:]]))
        else:
            raise Exception('unknown type encoding: ' + code)

    cfunctype = CFUNCTYPE(*argtypes)

    # Cache the new CFUNCTYPE in the cfunctype_table.
    # We do this mainly because it prevents the CFUNCTYPE 
    # from being garbage-collected while we need it.
    cfunctype_table[encoding] = cfunctype
    return cfunctype

######################################################################

# After calling create_subclass, you must first register
# it with register_subclass before you may use it.
# You can add new methods after the class is registered,
# but you cannot add any new ivars.
def create_subclass(superclass, name):
    if isinstance(superclass, str):
        superclass = get_class(superclass)
    return c_void_p(objc.objc_allocateClassPair(superclass, ensure_bytes(name), 0))

def register_subclass(subclass):
    objc.objc_registerClassPair(subclass)

# types is a string encoding the argument types of the method.
# The first type code of types is the return type (e.g. 'v' if void)
# The second type code must be '@' for id self.
# The third type code must be ':' for SEL cmd.
# Additional type codes are for types of other arguments if any.
def add_method(cls, selName, method, types):
    type_encodings = parse_type_encoding(types)
    assert(type_encodings[1] == b'@')  # ensure id self typecode
    assert(type_encodings[2] == b':')  # ensure SEL cmd typecode
    selector = get_selector(selName)
    cfunctype = cfunctype_for_encoding(types)
    imp = cfunctype(method)
    objc.class_addMethod.argtypes = [c_void_p, c_void_p, cfunctype, c_char_p]
    objc.class_addMethod(cls, selector, imp, types)
    return imp

def add_ivar(cls, name, vartype):
    return objc.class_addIvar(cls, ensure_bytes(name), sizeof(vartype), alignment(vartype), encoding_for_ctype(vartype))

def set_instance_variable(obj, varname, value, vartype):
    objc.object_setInstanceVariable.argtypes = [c_void_p, c_char_p, vartype]
    objc.object_setInstanceVariable(obj, ensure_bytes(varname), value)
    
def get_instance_variable(obj, varname, vartype):
    variable = vartype()
    objc.object_getInstanceVariable(obj, ensure_bytes(varname), byref(variable))
    return variable.value

######################################################################

class ObjCMethod(object):
    """This represents an unbound Objective-C method (really an IMP)."""

    # Note, need to map 'c' to c_byte rather than c_char, because otherwise
    # ctypes converts the value into a one-character string which is generally
    # not what we want at all, especially when the 'c' represents a bool var.
    typecodes = {b'c':c_byte, b'i':c_int, b's':c_short, b'l':c_long, b'q':c_longlong, 
                 b'C':c_ubyte, b'I':c_uint, b'S':c_ushort, b'L':c_ulong, b'Q':c_ulonglong, 
                 b'f':c_float, b'd':c_double, b'B':c_bool, b'v':None, b'Vv':None, b'*':c_char_p,
                 b'@':c_void_p, b'#':c_void_p, b':':c_void_p, b'^v':c_void_p, b'?':c_void_p, 
                 NSPointEncoding:NSPoint, NSSizeEncoding:NSSize, NSRectEncoding:NSRect,
                 NSRangeEncoding:NSRange,
                 PyObjectEncoding:py_object}

    cfunctype_table = {}
    
    def __init__(self, method):
        """Initialize with an Objective-C Method pointer.  We then determine
        the return type and argument type information of the method."""
        self.selector = c_void_p(objc.method_getName(method))
        self.name = objc.sel_getName(self.selector)
        self.pyname = self.name.replace(b':', b'_')
        self.encoding = objc.method_getTypeEncoding(method)
        self.return_type = objc.method_copyReturnType(method)
        self.nargs = objc.method_getNumberOfArguments(method)
        self.imp = c_void_p(objc.method_getImplementation(method))
        self.argument_types = []
        for i in range(self.nargs):
            buffer = c_buffer(512)
            objc.method_getArgumentType(method, i, buffer, len(buffer))
            self.argument_types.append(buffer.value)
        # Get types for all the arguments.
        try:
            self.argtypes = [self.ctype_for_encoding(t) for t in self.argument_types]
        except:
            #print 'no argtypes encoding for %s (%s)' % (self.name, self.argument_types)
            self.argtypes = None
        # Get types for the return type.
        try:
            if self.return_type == b'@':
                self.restype = ObjCInstance
            elif self.return_type == b'#':
                self.restype = ObjCClass
            else:
                self.restype = self.ctype_for_encoding(self.return_type)
        except:
            #print 'no restype encoding for %s (%s)' % (self.name, self.return_type)
            self.restype = None
        self.func = None

    def ctype_for_encoding(self, encoding):
        """Return ctypes type for an encoded Objective-C type."""
        if encoding in self.typecodes:
            return self.typecodes[encoding]
        elif encoding[0:1] == b'^' and encoding[1:] in self.typecodes:
            return POINTER(self.typecodes[encoding[1:]])
        elif encoding[0:1] == b'^' and encoding[1:] in [CGImageEncoding, NSZoneEncoding]:
            # special cases
            return c_void_p
        elif encoding[0:1] == b'r' and encoding[1:] in self.typecodes:
            # const decorator, don't care
            return self.typecodes[encoding[1:]]
        elif encoding[0:2] == b'r^' and encoding[2:] in self.typecodes:
            # const pointer, also don't care
            return POINTER(self.typecodes[encoding[2:]])
        else:
            raise Exception('unknown encoding for %s: %s' % (self.name, encoding))
        
    def get_prototype(self):
        """Returns a ctypes CFUNCTYPE for the method."""
        if self.restype == ObjCInstance or self.restype == ObjCClass:
            # Some hacky stuff to get around ctypes issues on 64-bit.  Can't let
            # ctypes convert the return value itself, because it truncates the pointer
            # along the way.  So instead, we must do set the return type to c_void_p to 
            # ensure we get 64-bit addresses and then convert the return value manually.
            self.prototype = CFUNCTYPE(c_void_p, *self.argtypes)
        else:
            self.prototype = CFUNCTYPE(self.restype, *self.argtypes)
        return self.prototype
    
    def __repr__(self):
        return "<ObjCMethod: %s %s>" % (self.name, self.encoding)

    def get_callable(self):
        """Returns a python-callable version of the method's IMP."""
        if not self.func:
            prototype = self.get_prototype()
            self.func = cast(self.imp, prototype)
            if self.restype == ObjCInstance or self.restype == ObjCClass:
                self.func.restype = c_void_p
            else:
                self.func.restype = self.restype
            self.func.argtypes = self.argtypes
        return self.func
   
    def __call__(self, objc_id, *args):
        """Call the method with the given id and arguments.  You do not need
        to pass in the selector as an argument since it will be automatically 
        provided."""
        f = self.get_callable()
        try:
            result = f(objc_id, self.selector, *args)
            # Convert result to python type if it is a instance or class pointer.
            if self.restype == ObjCInstance:
                result = ObjCInstance(result)
            elif self.restype == ObjCClass:
                result = ObjCClass(result)
            return result
        except ArgumentError as error:
            # Add more useful info to argument error exceptions, then reraise.
            error.args += ('selector = ' + self.name,
                           'argtypes =' + str(self.argtypes),
                           'encoding = ' + self.encoding)
            raise

######################################################################

class ObjCBoundMethod(object):
    """This represents an Objective-C method (an IMP) which has been bound 
    to some id which will be passed as the first parameter to the method."""

    def __init__(self, method, objc_id):
        """Initialize with a method and ObjCInstance or ObjCClass object."""
        self.method = method
        self.objc_id = objc_id

    def __repr__(self):
        return '<ObjCBoundMethod %s (%s)>' % (self.method.name, self.objc_id)

    def __call__(self, *args):
        """Call the method with the given arguments."""
        return self.method(self.objc_id, *args)

######################################################################
 
class ObjCClass(object):
    """Python wrapper for an Objective-C class."""

    # We only create one Python object for each Objective-C class.
    # Any future calls with the same class will return the previously
    # created Python object.  Note that these aren't weak references.
    # After you create an ObjCClass, it will exist until the end of the
    # program.
    _registered_classes = {}

    def __new__(cls, class_name_or_ptr):
        """Create a new ObjCClass instance or return a previously created
        instance for the given Objective-C class.  The argument may be either
        the name of the class to retrieve, or a pointer to the class."""
        # Determine name and ptr values from passed in argument.
        if isinstance(class_name_or_ptr, str):
            name = class_name_or_ptr
            ptr = get_class(name)
        else:
            ptr = class_name_or_ptr
            # Make sure that ptr value is wrapped in c_void_p object
            # for safety when passing as ctypes argument.
            if not isinstance(ptr, c_void_p):
                ptr = c_void_p(ptr)
            name = objc.class_getName(ptr)
            
        # Check if we've already created a Python object for this class
        # and if so, return it rather than making a new one.
        if name in cls._registered_classes:
            return cls._registered_classes[name]

        # Otherwise create a new Python object and then initialize it.
        objc_class = super(ObjCClass, cls).__new__(cls)
        objc_class.ptr = ptr
        objc_class.name = name
        objc_class.instance_methods = {}   # mapping of name -> instance method
        objc_class.class_methods = {}      # mapping of name -> class method
        objc_class._as_parameter_ = ptr    # for ctypes argument passing

        # Store the new class in dictionary of registered classes.
        cls._registered_classes[name] = objc_class

        # Not sure this is necessary...
        objc_class.cache_instance_methods()
        objc_class.cache_class_methods()

        return objc_class

    def __repr__(self):
        return "<ObjCClass: %s at %s>" % (self.name, str(self.ptr.value))
        
    def cache_instance_methods(self):
        """Create and store python representations of all instance methods 
        implemented by this class (but does not find methods of superclass)."""
        count = c_uint()
        method_array = objc.class_copyMethodList(self.ptr, byref(count))
        for i in range(count.value):
            method = c_void_p(method_array[i])
            objc_method = ObjCMethod(method)
            self.instance_methods[objc_method.pyname] = objc_method

    def cache_class_methods(self):
        """Create and store python representations of all class methods 
        implemented by this class (but does not find methods of superclass)."""
        count = c_uint()
        method_array = objc.class_copyMethodList(objc.object_getClass(self.ptr), byref(count))
        for i in range(count.value):
            method = c_void_p(method_array[i])
            objc_method = ObjCMethod(method)
            self.class_methods[objc_method.pyname] = objc_method

    def get_instance_method(self, name):
        """Returns a python representation of the named instance method, 
        either by looking it up in the cached list of methods or by searching
        for and creating a new method object."""
        if name in self.instance_methods:
            return self.instance_methods[name]
        else:
            # If method name isn't in the cached list, it might be a method of
            # the superclass, so call class_getInstanceMethod to check.
            selector = get_selector(name.replace(b'_', b':'))
            method = c_void_p(objc.class_getInstanceMethod(self.ptr, selector))
            if method.value:
                objc_method = ObjCMethod(method)
                self.instance_methods[name] = objc_method
                return objc_method
        return None

    def get_class_method(self, name):
        """Returns a python representation of the named class method, 
        either by looking it up in the cached list of methods or by searching
        for and creating a new method object."""
        if name in self.class_methods:
            return self.class_methods[name]
        else:
            # If method name isn't in the cached list, it might be a method of
            # the superclass, so call class_getInstanceMethod to check.
            selector = get_selector(name.replace(b'_', b':'))
            method = c_void_p(objc.class_getClassMethod(self.ptr, selector))
            if method.value:
                objc_method = ObjCMethod(method)
                self.class_methods[name] = objc_method
                return objc_method
        return None
        
    def __getattr__(self, name):
        """Returns a callable method object with the given name."""
        # If name refers to a class method, then return a callable object
        # for the class method with self.ptr as hidden first parameter.
        name = ensure_bytes(name)
        method = self.get_class_method(name)
        if method:
            return ObjCBoundMethod(method, self.ptr)
        # If name refers to an instance method, then simply return the method.
        # The caller will need to supply an instance as the first parameter.
        method = self.get_instance_method(name)
        if method: 
            return method
        # Otherwise, raise an exception.
        raise AttributeError('ObjCClass %s has no attribute %s' % (self.name, name))

######################################################################

class ObjCInstance(object):
    """Python wrapper for an Objective-C instance."""

    _cached_objects = {} 

    def __new__(cls, object_ptr):
        """Create a new ObjCInstance or return a previously created one
        for the given object_ptr which should be an Objective-C id."""
        # Make sure that object_ptr is wrapped in a c_void_p.
        if not isinstance(object_ptr, c_void_p):
            object_ptr = c_void_p(object_ptr)
        
        # If given a nil pointer, return None.
        if not object_ptr.value:
            return None

        # Check if we've already created an python ObjCInstance for this
        # object_ptr id and if so, then return it.  A single ObjCInstance will
        # be created for any object pointer when it is first encountered.
        # This same ObjCInstance will then persist until the object is 
        # deallocated.
        if object_ptr.value in cls._cached_objects:
            return cls._cached_objects[object_ptr.value]

        # Otherwise, create a new ObjCInstance.
        objc_instance = super(ObjCInstance, cls).__new__(cls)
        objc_instance.ptr = object_ptr
        objc_instance._as_parameter_ = object_ptr
        # Determine class of this object.
        class_ptr = c_void_p(objc.object_getClass(object_ptr))
        objc_instance.objc_class = ObjCClass(class_ptr)

        # Store new object in the dictionary of cached objects, keyed
        # by the (integer) memory address pointed to by the object_ptr.
        cls._cached_objects[object_ptr.value] = objc_instance

        # Create a DeallocationObserver and associate it with this object.
        # When the Objective-C object is deallocated, the observer will remove
        # the ObjCInstance corresponding to the object from the cached objects
        # dictionary, effectively destroying the ObjCInstance.
        observer = send_message(send_message('DeallocationObserver', 'alloc'), 'initWithObject:', objc_instance)
        objc.objc_setAssociatedObject(objc_instance, observer, observer, 0x301)
        # The observer is retained by the object we associate it to.  We release
        # the observer now so that it will be deallocated when the associated
        # object is deallocated.
        send_message(observer, 'release')

        return objc_instance

    def __repr__(self):
        if self.objc_class.name == b'NSCFString':
            # Display contents of NSString objects
            from .cocoalibs import cfstring_to_string
            string = cfstring_to_string(self)
            return "<ObjCInstance %#x: %s (%s) at %s>" % (id(self), self.objc_class.name, string, str(self.ptr.value))

        return "<ObjCInstance %#x: %s at %s>" % (id(self), self.objc_class.name, str(self.ptr.value))

    def __getattr__(self, name):
        """Returns a callable method object with the given name."""
        # Search for named instance method in the class object and if it
        # exists, return callable object with self as hidden argument.
        # Note: you should give self and not self.ptr as a parameter to 
        # ObjCBoundMethod, so that it will be able to keep the ObjCInstance
        # alive for chained calls like MyClass.alloc().init() where the 
        # object created by alloc() is not assigned to a variable.
        name = ensure_bytes(name)
        method = self.objc_class.get_instance_method(name)
        if method:
            return ObjCBoundMethod(method, self)
        # Else, search for class method with given name in the class object.
        # If it exists, return callable object with a pointer to the class 
        # as a hidden argument.
        method = self.objc_class.get_class_method(name)
        if method:
            return ObjCBoundMethod(method, self.objc_class.ptr)
        # Otherwise raise an exception.
        raise AttributeError('ObjCInstance %s has no attribute %s' % (self.objc_class.name, name))

######################################################################

def convert_method_arguments(encoding, args):
    """Used by ObjCSubclass to convert Objective-C method arguments to
    Python values before passing them on to the Python-defined method."""
    new_args = []
    arg_encodings = parse_type_encoding(encoding)[3:]
    for e, a in zip(arg_encodings, args):
        if e == b'@':
            new_args.append(ObjCInstance(a))
        elif e == b'#':
            new_args.append(ObjCClass(a))
        else:
            new_args.append(a)
    return new_args

# ObjCSubclass is used to define an Objective-C subclass of an existing
# class registered with the runtime.  When you create an instance of
# ObjCSubclass, it registers the new subclass with the Objective-C
# runtime and creates a set of function decorators that you can use to
# add instance methods or class methods to the subclass.
#
# Typical usage would be to first create and register the subclass:
#
#     MySubclass = ObjCSubclass('NSObject', 'MySubclassName')
#
# then add methods with:
#
#     @MySubclass.method('v')
#     def methodThatReturnsVoid(self):
#         pass
#
#     @MySubclass.method('Bi')
#     def boolReturningMethodWithInt_(self, x):
#         return True
#
#     @MySubclass.classmethod('@')
#     def classMethodThatReturnsId(self):
#         return self
#
# It is probably a good idea to organize the code related to a single
# subclass by either putting it in its own module (note that you don't
# actually need to expose any of the method names or the ObjCSubclass)
# or by bundling it all up inside a python class definition, perhaps
# called MySubclassImplementation.
#
# It is also possible to add Objective-C ivars to the subclass, however
# if you do so, you must call the __init__ method with register=False,
# and then call the register method after the ivars have been added.
# But rather than creating the ivars in Objective-C land, it is easier
# to just define python-based instance variables in your subclass's init
# method.
# 
# This class is used only to *define* the interface and implementation
# of an Objective-C subclass from python.  It should not be used in
# any other way.  If you want a python representation of the resulting
# class, create it with ObjCClass.
#
# Instances are created as a pointer to the objc object by using:
#
#     myinstance = send_message('MySubclassName', 'alloc')
#     myinstance = send_message(myinstance, 'init')
#
# or wrapped inside an ObjCInstance object by using:
#
#     myclass = ObjCClass('MySubclassName')
#     myinstance = myclass.alloc().init()
#
class ObjCSubclass(object):
    """Use this to create a subclass of an existing Objective-C class.
    It consists primarily of function decorators which you use to add methods
    to the subclass."""

    def __init__(self, superclass, name, register=True):
        self._imp_table = {}
        self.name = name
        self.objc_cls = create_subclass(superclass, name)
        self._as_parameter_ = self.objc_cls
        if register:
            self.register()

    def register(self):
        """Register the new class with the Objective-C runtime."""
        objc.objc_registerClassPair(self.objc_cls)
        # We can get the metaclass only after the class is registered.
        self.objc_metaclass = get_metaclass(self.name)

    def add_ivar(self, varname, vartype):
        """Add instance variable named varname to the subclass.
        varname should be a string.
        vartype is a ctypes type.
        The class must be registered AFTER adding instance variables."""
        return add_ivar(self.objc_cls, varname, vartype)

    def add_method(self, method, name, encoding):
        imp = add_method(self.objc_cls, name, method, encoding)
        self._imp_table[name] = imp

    # http://iphonedevelopment.blogspot.com/2008/08/dynamically-adding-class-objects.html
    def add_class_method(self, method, name, encoding):
        imp = add_method(self.objc_metaclass, name, method, encoding)
        self._imp_table[name] = imp

    def rawmethod(self, encoding):
        """Decorator for instance methods without any fancy shenanigans.
        The function must have the signature f(self, cmd, *args)
        where both self and cmd are just pointers to objc objects."""
        # Add encodings for hidden self and cmd arguments.
        encoding = ensure_bytes(encoding)
        typecodes = parse_type_encoding(encoding)
        typecodes.insert(1, b'@:')
        encoding = b''.join(typecodes)
        def decorator(f):
            name = f.__name__.replace('_', ':')
            self.add_method(f, name, encoding)
            return f
        return decorator
     
    def method(self, encoding):
        """Function decorator for instance methods."""
        # Add encodings for hidden self and cmd arguments.
        encoding = ensure_bytes(encoding)
        typecodes = parse_type_encoding(encoding)
        typecodes.insert(1, b'@:')
        encoding = b''.join(typecodes)
        def decorator(f):
            def objc_method(objc_self, objc_cmd, *args):
                py_self = ObjCInstance(objc_self)
                py_self.objc_cmd = objc_cmd
                args = convert_method_arguments(encoding, args)
                result = f(py_self, *args)
                if isinstance(result, ObjCClass):
                    result = result.ptr.value
                elif isinstance(result, ObjCInstance):
                    result = result.ptr.value
                return result
            name = f.__name__.replace('_', ':')
            self.add_method(objc_method, name, encoding)
            return objc_method
        return decorator
                  
    def classmethod(self, encoding):
        """Function decorator for class methods."""
        # Add encodings for hidden self and cmd arguments.
        encoding = ensure_bytes(encoding)
        typecodes = parse_type_encoding(encoding)
        typecodes.insert(1, b'@:')
        encoding = b''.join(typecodes)
        def decorator(f):
            def objc_class_method(objc_cls, objc_cmd, *args):
                py_cls = ObjCClass(objc_cls)
                py_cls.objc_cmd = objc_cmd
                args = convert_method_arguments(encoding, args)
                result = f(py_cls, *args)
                if isinstance(result, ObjCClass):
                    result = result.ptr.value
                elif isinstance(result, ObjCInstance):
                    result = result.ptr.value
                return result
            name = f.__name__.replace('_', ':')
            self.add_class_method(objc_class_method, name, encoding)
            return objc_class_method
        return decorator

######################################################################

# Instances of DeallocationObserver are associated with every 
# Objective-C object that gets wrapped inside an ObjCInstance.
# Their sole purpose is to watch for when the Objective-C object
# is deallocated, and then remove the object from the dictionary
# of cached ObjCInstance objects kept by the ObjCInstance class.
#
# The methods of the class defined below are decorated with 
# rawmethod() instead of method() because DeallocationObservers
# are created inside of ObjCInstance's __new__ method and we have
# to be careful to not create another ObjCInstance here (which
# happens when the usual method decorator turns the self argument
# into an ObjCInstance), or else get trapped in an infinite recursion.
class DeallocationObserver_Implementation(object):
    DeallocationObserver = ObjCSubclass('NSObject', 'DeallocationObserver', register=False)
    DeallocationObserver.add_ivar('observed_object', c_void_p)
    DeallocationObserver.register()

    @DeallocationObserver.rawmethod('@@')
    def initWithObject_(self, cmd, anObject):
        self = send_super(self, 'init')
        self = self.value
        set_instance_variable(self, 'observed_object', anObject, c_void_p)
        return self
    
    @DeallocationObserver.rawmethod('v')
    def dealloc(self, cmd):
        anObject = get_instance_variable(self, 'observed_object', c_void_p)
        ObjCInstance._cached_objects.pop(anObject, None)
        send_super(self, 'dealloc')

    @DeallocationObserver.rawmethod('v')    
    def finalize(self, cmd):
        # Called instead of dealloc if using garbage collection.  
        # (which would have to be explicitly started with 
        # objc_startCollectorThread(), so probably not too much reason 
        # to have this here, but I guess it can't hurt.)
        anObject = get_instance_variable(self, 'observed_object', c_void_p)
        ObjCInstance._cached_objects.pop(anObject, None)
        send_super(self, 'finalize')
