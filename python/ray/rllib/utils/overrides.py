#
#  Copyright 2015 Mikko Korpela
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

import sys
import dis
__VERSION__ = '0.5'


if sys.version > '3':
    long = int

def overrides(method):
    """Decorator to indicate that the decorated method overrides a method in superclass.
    The decorator code is executed while loading class. Using this method should have minimal runtime performance
    implications.

    This is based on my idea about how to do this and fwc:s highly improved algorithm for the implementation
    fwc:s algorithm : http://stackoverflow.com/a/14631397/308189
    my answer : http://stackoverflow.com/a/8313042/308189

    How to use:
    from overrides import overrides

    class SuperClass(object):

        def method(self):
            return 2

    class SubClass(SuperClass):

        @overrides
        def method(self):
            return 1

    :raises  AssertionError if no match in super classes for the method name
    :return  method with possibly added (if the method doesn't have one) docstring from super class
    """
    # nop for now due to py3 compatibility
    return method
    # for super_class in _get_base_classes(sys._getframe(2), method.__globals__):
    #     if hasattr(super_class, method.__name__):
    #         if not method.__doc__:
    #             method.__doc__ = getattr(super_class, method.__name__).__doc__
    #         return method
    # raise AssertionError('No super class method found for "%s"' % method.__name__)

def _get_base_classes(frame, namespace):
    return [_get_base_class(class_name_components, namespace) for class_name_components in _get_base_class_names(frame)]

def _get_base_class_names(frame):
    """Get baseclass names from the code object"""
    co, lasti = frame.f_code, frame.f_lasti
    code = co.co_code
    i = 0
    extended_arg = 0
    extends = []
    while i <= lasti:
        c = code[i]
        op = ord(c)
        i += 1
        if op >= dis.HAVE_ARGUMENT:
            oparg = ord(code[i]) + ord(code[i+1])*256 + extended_arg
            extended_arg = 0
            i += 2
            if op == dis.EXTENDED_ARG:
                extended_arg = oparg*int(65536)
            if op in dis.hasconst:
                if type(co.co_consts[oparg]) == str:
                    extends = []
            elif op in dis.hasname:
                if dis.opname[op] == 'LOAD_NAME':
                    extends.append(('name', co.co_names[oparg]))
                if dis.opname[op] == 'LOAD_ATTR':
                    extends.append(('attr', co.co_names[oparg]))
    items = []
    previous_item = []
    for t, s in extends:
        if t == 'name':
            if previous_item:
                items.append(previous_item)
            previous_item = [s]
        else:
            previous_item += [s]
    if previous_item:
        items.append(previous_item)
    return items

def _get_base_class(components, namespace):
    obj = namespace[components[0]]
    for component in components[1:]:
        obj = getattr(obj, component)
    return obj
