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
'''Functions for loading dynamic libraries.

These extend and correct ctypes functions.
'''
from __future__ import print_function
from builtins import str
from builtins import object
from past.builtins import basestring

__docformat__ = 'restructuredtext'
__version__ = '$Id: $'

import os
import re
import sys

import ctypes
import ctypes.util

import pyglet

_debug_lib = pyglet.options['debug_lib']
_debug_trace = pyglet.options['debug_trace']

_is_pyglet_docgen = hasattr(sys, 'is_pyglet_docgen') and sys.is_pyglet_docgen

if pyglet.options['search_local_libs']:
    script_path = pyglet.resource.get_script_home()
    _local_lib_paths = [script_path, os.path.join(script_path, 'lib'),]
else:
    _local_lib_paths = None

class _TraceFunction(object):
    def __init__(self, func):
        self.__dict__['_func'] = func

    def __str__(self):
        return self._func.__name__

    def __call__(self, *args, **kwargs):
        return self._func(*args, **kwargs)

    def __getattr__(self, name):
        return getattr(self._func, name)

    def __setattr__(self, name, value):
        setattr(self._func, name, value)

class _TraceLibrary(object):
    def __init__(self, library):
        self._library = library
        print(library)

    def __getattr__(self, name):
        func = getattr(self._library, name)
        f = _TraceFunction(func)
        return f

if _is_pyglet_docgen:
    class LibraryMock(object):
        """Mock library used when generating documentation."""
        def __getattr__(self, name):
            return LibraryMock()

        def __setattr__(self, name, value):
            pass

        def __call__(self, *args, **kwargs):
            return LibraryMock()


class LibraryLoader(object):
    def load_library(self, *names, **kwargs):
        '''Find and load a library.  
        
        More than one name can be specified, they will be tried in order.
        Platform-specific library names (given as kwargs) are tried first.

        Raises ImportError if library is not found.
        '''
        if _is_pyglet_docgen:
            return LibraryMock()

        if 'framework' in kwargs and self.platform == 'darwin':
            return self.load_framework(kwargs['framework'])

        if not names:
            raise ImportError("No library name specified")
        
        platform_names = kwargs.get(self.platform, [])
        if isinstance(platform_names, basestring):
            platform_names = [platform_names]
        elif type(platform_names) is tuple:
            platform_names = list(platform_names)

        if self.platform.startswith('linux'):
            for name in names:
                libname = self.find_library(name)
                platform_names.append(libname or 'lib%s.so' % name)

        platform_names.extend(names)
        for name in platform_names:
            try:
                lib = ctypes.cdll.LoadLibrary(name)
                if _debug_lib:
                    print(name)
                if _debug_trace:
                    lib = _TraceLibrary(lib)
                return lib
            except OSError as o:
                if self.platform == "win32" and o.winerror != 126:
                    print("Unexpected error loading library %s: %s" % (name, str(o)))
                    raise
                path = self.find_library(name)
                if path:
                    try:
                        lib = ctypes.cdll.LoadLibrary(path)
                        if _debug_lib:
                            print(path)
                        if _debug_trace:
                            lib = _TraceLibrary(lib)
                        return lib
                    except OSError:
                        pass
        raise ImportError('Library "%s" not found.' % names[0])

    find_library = lambda self, name: ctypes.util.find_library(name)

    platform = pyglet.compat_platform
    # this is only for library loading, don't include it in pyglet.platform
    if platform == 'cygwin':
        platform = 'win32'

    def load_framework(self, path):
        raise RuntimeError("Can't load framework on this platform.")

class MachOLibraryLoader(LibraryLoader):
    def __init__(self):
        if 'LD_LIBRARY_PATH' in os.environ:
            self.ld_library_path = os.environ['LD_LIBRARY_PATH'].split(':')
        else:
            self.ld_library_path = []

        if _local_lib_paths:
            # search first for local libs
            self.ld_library_path = _local_lib_paths + self.ld_library_path
            os.environ['LD_LIBRARY_PATH'] = ':'.join(self.ld_library_path)

        if 'DYLD_LIBRARY_PATH' in os.environ:
            self.dyld_library_path = os.environ['DYLD_LIBRARY_PATH'].split(':')
        else:
            self.dyld_library_path = []

        if 'DYLD_FALLBACK_LIBRARY_PATH' in os.environ:
            self.dyld_fallback_library_path = \
                os.environ['DYLD_FALLBACK_LIBRARY_PATH'].split(':')
        else:
            self.dyld_fallback_library_path = [
                os.path.expanduser('~/lib'),
                '/usr/local/lib',
                '/usr/lib']
 
    def find_library(self, path):
        '''Implements the dylib search as specified in Apple documentation:

        http://developer.apple.com/documentation/DeveloperTools/Conceptual/DynamicLibraries/100-Articles/DynamicLibraryUsageGuidelines.html

        Before commencing the standard search, the method first checks
        the bundle's ``Frameworks`` directory if the application is running
        within a bundle (OS X .app).
        '''

        libname = os.path.basename(path)
        search_path = []

        if '.' not in libname:
            libname = 'lib' + libname + '.dylib'

        # py2app support
        if (hasattr(sys, 'frozen') and sys.frozen == 'macosx_app' and
                'RESOURCEPATH' in os.environ):
            search_path.append(os.path.join(
                os.environ['RESOURCEPATH'],
                '..',
                'Frameworks',
                libname))

        # pyinstaller.py sets sys.frozen to True, and puts dylibs in
        # Contents/MacOS, which path pyinstaller puts in sys._MEIPASS
        if (hasattr(sys, 'frozen') and hasattr(sys, '_MEIPASS') and
                sys.frozen == True and pyglet.compat_platform == 'darwin'):
            search_path.append(os.path.join(sys._MEIPASS, libname))

        if '/' in path:
            search_path.extend(
                [os.path.join(p, libname) \
                    for p in self.dyld_library_path])
            search_path.append(path)
            search_path.extend(
                [os.path.join(p, libname) \
                    for p in self.dyld_fallback_library_path])
        else:
            search_path.extend(
                [os.path.join(p, libname) \
                    for p in self.ld_library_path])
            search_path.extend(
                [os.path.join(p, libname) \
                    for p in self.dyld_library_path])
            search_path.append(path)
            search_path.extend(
                [os.path.join(p, libname) \
                    for p in self.dyld_fallback_library_path])

        for path in search_path:
            if os.path.exists(path):
                return path

        return None

    def find_framework(self, path):
        '''Implement runtime framework search as described by:

        http://developer.apple.com/documentation/MacOSX/Conceptual/BPFrameworks/Concepts/FrameworkBinding.html
        '''

        # e.g. path == '/System/Library/Frameworks/OpenGL.framework'
        #      name == 'OpenGL'
        # return '/System/Library/Frameworks/OpenGL.framework/OpenGL'
        name = os.path.splitext(os.path.split(path)[1])[0]

        realpath = os.path.join(path, name) 
        if os.path.exists(realpath):
            return realpath

        for dir in ('/Library/Frameworks',
                    '/System/Library/Frameworks'):
            realpath = os.path.join(dir, '%s.framework' % name, name)
            if os.path.exists(realpath):
                return realpath

        return None

    def load_framework(self, path):
        realpath = self.find_framework(path)
        if realpath:
            lib = ctypes.cdll.LoadLibrary(realpath)
            if _debug_lib:
                print(realpath)
            if _debug_trace:
                lib = _TraceLibrary(lib)
            return lib

        raise ImportError("Can't find framework %s." % path)

class LinuxLibraryLoader(LibraryLoader):
    _ld_so_cache = None
    _local_libs_cache = None

    def _find_libs(self, directories):
        cache = {}
        lib_re = re.compile('lib(.*)\.so(?:$|\.)')
        for dir in directories:
            try:
                for file in os.listdir(dir):
                    match = lib_re.match(file)
                    if match:
                        # Index by filename
                        path = os.path.join(dir, file)
                        if file not in cache:
                            cache[file] = path
                        # Index by library name
                        library = match.group(1)
                        if library not in cache:
                            cache[library] = path
            except OSError:
                pass
        return cache

    def _create_ld_so_cache(self):
        # Recreate search path followed by ld.so.  This is going to be
        # slow to build, and incorrect (ld.so uses ld.so.cache, which may
        # not be up-to-date).  Used only as fallback for distros without
        # /sbin/ldconfig.
        #
        # We assume the DT_RPATH and DT_RUNPATH binary sections are omitted.

        directories = []
        try:
            directories.extend(os.environ['LD_LIBRARY_PATH'].split(':'))
        except KeyError:
            pass

        try:
            with open('/etc/ld.so.conf') as fid:
                directories.extend([dir.strip() for dir in fid])
        except IOError:
            pass

        directories.extend(['/lib', '/usr/lib'])

        self._ld_so_cache = self._find_libs(directories)

    def find_library(self, path):

        # search first for local libs
        if _local_lib_paths:
            if not self._local_libs_cache:
                self._local_libs_cache = self._find_libs(_local_lib_paths)
            if path in self._local_libs_cache:
                return self._local_libs_cache[path]

        # ctypes tries ldconfig, gcc and objdump.  If none of these are
        # present, we implement the ld-linux.so search path as described in
        # the man page.

        result = ctypes.util.find_library(path)
        if result:
            return result

        if self._ld_so_cache is None:
            self._create_ld_so_cache()

        return self._ld_so_cache.get(path)

if pyglet.compat_platform == 'darwin':
    loader = MachOLibraryLoader()
elif pyglet.compat_platform.startswith('linux'):
    loader = LinuxLibraryLoader()
else:
    loader = LibraryLoader()
load_library = loader.load_library
