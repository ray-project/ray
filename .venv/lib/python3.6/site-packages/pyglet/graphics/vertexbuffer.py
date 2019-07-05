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
# $Id:$

'''Byte abstractions of Vertex Buffer Objects and vertex arrays.

Use :py:func:`create_buffer` or :py:func:`create_mappable_buffer` to create a
Vertex Buffer Object, or a vertex array if VBOs are not supported by the
current context.

Buffers can optionally be created "mappable" (incorporating the
:py:class:`AbstractMappable` mix-in).  In this case the buffer provides a
:py:meth:`~AbstractMappable.get_region` method which provides the most
efficient path for updating partial data within the buffer.
'''
from builtins import range
from builtins import object

__docformat__ = 'restructuredtext'
__version__ = '$Id: $'

import ctypes
import sys

import pyglet
from pyglet.gl import *

_enable_vbo = pyglet.options['graphics_vbo']

# Enable workaround permanently if any VBO is created on a context that has
# this workaround.  (On systems with multiple contexts where one is
# unaffected, the workaround will be enabled unconditionally on all of the
# contexts anyway.  This is completely unlikely anyway).
_workaround_vbo_finish = False

def create_buffer(size,
                  target=GL_ARRAY_BUFFER,
                  usage=GL_DYNAMIC_DRAW,
                  vbo=True):
    '''Create a buffer of vertex data.

    :Parameters:
        `size` : int
            Size of the buffer, in bytes
        `target` : int
            OpenGL target buffer
        `usage` : int
            OpenGL usage constant
        `vbo` : bool
            True if a `VertexBufferObject` should be created if the driver
            supports it; otherwise only a `VertexArray` is created.

    :rtype: `AbstractBuffer`
    '''
    from pyglet import gl
    if (vbo and
        gl_info.have_version(1, 5) and
        _enable_vbo and
        not gl.current_context._workaround_vbo):
        return VertexBufferObject(size, target, usage)
    else:
        return VertexArray(size)

def create_mappable_buffer(size,
                           target=GL_ARRAY_BUFFER,
                           usage=GL_DYNAMIC_DRAW,
                           vbo=True):
    '''Create a mappable buffer of vertex data.

    :Parameters:
        `size` : int
            Size of the buffer, in bytes
        `target` : int
            OpenGL target buffer
        `usage` : int
            OpenGL usage constant
        `vbo` : bool
            True if a :py:class:`VertexBufferObject` should be created if the driver
            supports it; otherwise only a :py:class:`VertexArray` is created.

    :rtype: :py:class:`AbstractBuffer` with :py:class:`AbstractMappable`
    '''
    from pyglet import gl
    if (vbo and
        gl_info.have_version(1, 5) and
        _enable_vbo and
        not gl.current_context._workaround_vbo):
        return MappableVertexBufferObject(size, target, usage)
    else:
        return VertexArray(size)

class AbstractBuffer(object):
    '''Abstract buffer of byte data.

    :Ivariables:
        `size` : int
            Size of buffer, in bytes
        `ptr` : int
            Memory offset of the buffer, as used by the ``glVertexPointer``
            family of functions
        `target` : int
            OpenGL buffer target, for example ``GL_ARRAY_BUFFER``
        `usage` : int
            OpenGL buffer usage, for example ``GL_DYNAMIC_DRAW``

    '''

    ptr = 0
    size = 0

    def bind(self):
        '''Bind this buffer to its OpenGL target.'''
        raise NotImplementedError('abstract')

    def unbind(self):
        '''Reset the buffer's OpenGL target.'''
        raise NotImplementedError('abstract')

    def set_data(self, data):
        '''Set the entire contents of the buffer.

        :Parameters:
            `data` : sequence of int or ctypes pointer
                The byte array to set.

        '''
        raise NotImplementedError('abstract')

    def set_data_region(self, data, start, length):
        '''Set part of the buffer contents.

        :Parameters:
            `data` : sequence of int or ctypes pointer
                The byte array of data to set
            `start` : int
                Offset to start replacing data
            `length` : int
                Length of region to replace

        '''
        raise NotImplementedError('abstract')

    def map(self, invalidate=False):
        '''Map the entire buffer into system memory.

        The mapped region must be subsequently unmapped with `unmap` before
        performing any other operations on the buffer.

        :Parameters:
            `invalidate` : bool
                If True, the initial contents of the mapped block need not
                reflect the actual contents of the buffer.

        :rtype: ``POINTER(ctypes.c_ubyte)``
        :return: Pointer to the mapped block in memory
        '''
        raise NotImplementedError('abstract')

    def unmap(self):
        '''Unmap a previously mapped memory block.'''
        raise NotImplementedError('abstract')

    def resize(self, size):
        '''Resize the buffer to a new size.

        :Parameters:
            `size` : int
                New size of the buffer, in bytes

        '''

    def delete(self):
        '''Delete this buffer, reducing system resource usage.'''
        raise NotImplementedError('abstract')

class AbstractMappable(object):
    def get_region(self, start, size, ptr_type):
        '''Map a region of the buffer into a ctypes array of the desired
        type.  This region does not need to be unmapped, but will become
        invalid if the buffer is resized.

        Note that although a pointer type is required, an array is mapped.
        For example::

            get_region(0, ctypes.sizeof(c_int) * 20, ctypes.POINTER(c_int * 20))

        will map bytes 0 to 80 of the buffer to an array of 20 ints.

        Changes to the array may not be recognised until the region's
        :py:meth:`AbstractBufferRegion.invalidate` method is called.

        :Parameters:
            `start` : int
                Offset into the buffer to map from, in bytes
            `size` : int
                Size of the buffer region to map, in bytes
            `ptr_type` : ctypes pointer type
                Pointer type describing the array format to create

        :rtype: :py:class:`AbstractBufferRegion`
        '''
        raise NotImplementedError('abstract')

class VertexArray(AbstractBuffer, AbstractMappable):
    '''A ctypes implementation of a vertex array.

    Many of the methods on this class are effectively no-op's, such as
    :py:meth:`bind`, :py:meth:`unbind`, :py:meth:`map`, :py:meth:`unmap` and
    :py:meth:`delete`; they exist in order to present
    a consistent interface with :py:class:`VertexBufferObject`.

    This buffer type is also mappable, and so :py:meth:`get_region` can be used.
    '''

    def __init__(self, size):
        self.size = size

        self.array = (ctypes.c_byte * size)()
        self.ptr = ctypes.cast(self.array, ctypes.c_void_p).value

    def bind(self):
        pass

    def unbind(self):
        pass

    def set_data(self, data):
        ctypes.memmove(self.ptr, data, self.size)

    def set_data_region(self, data, start, length):
        ctypes.memmove(self.ptr + start, data, length)

    def map(self, invalidate=False):
        return self.array

    def unmap(self):
        pass

    def get_region(self, start, size, ptr_type):
        array = ctypes.cast(self.ptr + start, ptr_type).contents
        return VertexArrayRegion(array)

    def delete(self):
        pass

    def resize(self, size):
        array = (ctypes.c_byte * size)()
        ctypes.memmove(array, self.array, min(size, self.size))
        self.size = size
        self.array = array
        self.ptr = ctypes.cast(self.array, ctypes.c_void_p).value


class VertexBufferObject(AbstractBuffer):
    '''Lightweight representation of an OpenGL VBO.

    The data in the buffer is not replicated in any system memory (unless it
    is done so by the video driver).  While this can improve memory usage and
    possibly performance, updates to the buffer are relatively slow.

    This class does not implement :py:class:`AbstractMappable`, and so has no
    :py:meth:`~AbstractMappable.get_region` method.  See 
    :py:class:`MappableVertexBufferObject` for a VBO class
    that does implement :py:meth:`~AbstractMappable.get_region`.
    '''

    def __init__(self, size, target, usage):
        self.size = size
        self.target = target
        self.usage = usage
        self._context = pyglet.gl.current_context

        id = GLuint()
        glGenBuffers(1, id)
        self.id = id.value
        glPushClientAttrib(GL_CLIENT_VERTEX_ARRAY_BIT)
        glBindBuffer(target, self.id)
        glBufferData(target, self.size, None, self.usage)
        glPopClientAttrib()

        global _workaround_vbo_finish
        if pyglet.gl.current_context._workaround_vbo_finish:
            _workaround_vbo_finish = True

    def bind(self):
        glBindBuffer(self.target, self.id)

    def unbind(self):
        glBindBuffer(self.target, 0)

    def set_data(self, data):
        glPushClientAttrib(GL_CLIENT_VERTEX_ARRAY_BIT)
        glBindBuffer(self.target, self.id)
        glBufferData(self.target, self.size, data, self.usage)
        glPopClientAttrib()

    def set_data_region(self, data, start, length):
        glPushClientAttrib(GL_CLIENT_VERTEX_ARRAY_BIT)
        glBindBuffer(self.target, self.id)
        glBufferSubData(self.target, start, length, data)
        glPopClientAttrib()

    def map(self, invalidate=False):
        glPushClientAttrib(GL_CLIENT_VERTEX_ARRAY_BIT)
        glBindBuffer(self.target, self.id)
        if invalidate:
            glBufferData(self.target, self.size, None, self.usage)
        ptr = ctypes.cast(glMapBuffer(self.target, GL_WRITE_ONLY),
                          ctypes.POINTER(ctypes.c_byte * self.size)).contents
        glPopClientAttrib()
        return ptr

    def unmap(self):
        glPushClientAttrib(GL_CLIENT_VERTEX_ARRAY_BIT)
        glUnmapBuffer(self.target)
        glPopClientAttrib()

    def __del__(self):
        try:
            if self.id is not None:
                self._context.delete_buffer(self.id)
        except:
            pass

    def delete(self):
        id = GLuint(self.id)
        glDeleteBuffers(1, id)
        self.id = None

    def resize(self, size):
        # Map, create a copy, then reinitialize.
        temp = (ctypes.c_byte * size)()

        glPushClientAttrib(GL_CLIENT_VERTEX_ARRAY_BIT)
        glBindBuffer(self.target, self.id)
        data = glMapBuffer(self.target, GL_READ_ONLY)
        ctypes.memmove(temp, data, min(size, self.size))
        glUnmapBuffer(self.target)

        self.size = size
        glBufferData(self.target, self.size, temp, self.usage)
        glPopClientAttrib()

class MappableVertexBufferObject(VertexBufferObject, AbstractMappable):
    '''A VBO with system-memory backed store.

    Updates to the data via :py:meth:`set_data`, :py:meth:`set_data_region` and
    :py:meth:`map` will be held in local memory until :py:meth:`bind` is
    called.  The advantage is that fewer OpenGL calls are needed, increasing
    performance.

    There may also be less performance penalty for resizing this buffer.

    Updates to data via :py:meth:`map` are committed immediately.
    '''
    def __init__(self, size, target, usage):
        super(MappableVertexBufferObject, self).__init__(size, target, usage)
        self.data = (ctypes.c_byte * size)()
        self.data_ptr = ctypes.cast(self.data, ctypes.c_void_p).value
        self._dirty_min = sys.maxsize
        self._dirty_max = 0

    def bind(self):
        # Commit pending data
        super(MappableVertexBufferObject, self).bind()
        size = self._dirty_max - self._dirty_min
        if size > 0:
            if size == self.size:
                glBufferData(self.target, self.size, self.data, self.usage)
            else:
                glBufferSubData(self.target, self._dirty_min, size,
                    self.data_ptr + self._dirty_min)
            self._dirty_min = sys.maxsize
            self._dirty_max = 0

    def set_data(self, data):
        super(MappableVertexBufferObject, self).set_data(data)
        ctypes.memmove(self.data, data, self.size)
        self._dirty_min = 0
        self._dirty_max = self.size

    def set_data_region(self, data, start, length):
        ctypes.memmove(self.data_ptr + start, data, length)
        self._dirty_min = min(start, self._dirty_min)
        self._dirty_max = max(start + length, self._dirty_max)

    def map(self, invalidate=False):
        self._dirty_min = 0
        self._dirty_max = self.size
        return self.data

    def unmap(self):
        pass

    def get_region(self, start, size, ptr_type):
        array = ctypes.cast(self.data_ptr + start, ptr_type).contents
        return VertexBufferObjectRegion(self, start, start + size, array)

    def resize(self, size):
        data = (ctypes.c_byte * size)()
        ctypes.memmove(data, self.data, min(size, self.size))
        self.data = data
        self.data_ptr = ctypes.cast(self.data, ctypes.c_void_p).value

        self.size = size
        glPushClientAttrib(GL_CLIENT_VERTEX_ARRAY_BIT)
        glBindBuffer(self.target, self.id)
        glBufferData(self.target, self.size, self.data, self.usage)
        glPopClientAttrib()

        self._dirty_min = sys.maxsize
        self._dirty_max = 0

class AbstractBufferRegion(object):
    '''A mapped region of a buffer.

    Buffer regions are obtained using :py:meth:`~AbstractMappable.get_region`.

    :Ivariables:
        `array` : ctypes array
            Array of data, of the type and count requested by
            :py:meth:`~AbstractMappable.get_region`.

    '''
    def invalidate(self):
        '''Mark this region as changed.

        The buffer may not be updated with the latest contents of the
        array until this method is called.  (However, it may not be updated
        until the next time the buffer is used, for efficiency).
        '''
        pass

class VertexBufferObjectRegion(AbstractBufferRegion):
    '''A mapped region of a VBO.'''
    def __init__(self, buffer, start, end, array):
        self.buffer = buffer
        self.start = start
        self.end = end
        self.array = array

    def invalidate(self):
        buffer = self.buffer
        buffer._dirty_min = min(buffer._dirty_min, self.start)
        buffer._dirty_max = max(buffer._dirty_max, self.end)

class VertexArrayRegion(AbstractBufferRegion):
    '''A mapped region of a vertex array.

    The :py:meth:`~AbstractBufferRegion.invalidate` method is a no-op but is
    provided in order to present a consistent interface with
    :py:meth:`VertexBufferObjectRegion`.
    '''
    def __init__(self, array):
        self.array = array

class IndirectArrayRegion(AbstractBufferRegion):
    '''A mapped region in which data elements are not necessarily contiguous.

    This region class is used to wrap buffer regions in which the data
    must be accessed with some stride.  For example, in an interleaved buffer
    this region can be used to access a single interleaved component as if the
    data was contiguous.
    '''
    def __init__(self, region, size, component_count, component_stride):
        '''Wrap a buffer region.

        Use the `component_count` and `component_stride` parameters to specify
        the data layout of the encapsulated region.  For example, if RGBA
        data is to be accessed as if it were packed RGB, ``component_count``
        would be set to 3 and ``component_stride`` to 4.  If the region
        contains 10 RGBA tuples, the ``size`` parameter is ``3 * 10 = 30``.

        :Parameters:
            `region` : `AbstractBufferRegion`
                The region with interleaved data
            `size` : int
                The number of elements that this region will provide access to.
            `component_count` : int
                The number of elements that are contiguous before some must
                be skipped.
            `component_stride` : int
                The number of elements of interleaved data separating
                the contiguous sections.

        '''
        self.region = region
        self.size = size
        self.count = component_count
        self.stride = component_stride
        self.array = self

    def __repr__(self):
        return 'IndirectArrayRegion(size=%d, count=%d, stride=%d)' % (
            self.size, self.count, self.stride)

    def __getitem__(self, index):
        count = self.count
        if not isinstance(index, slice):
            elem = index // count
            j = index % count
            return self.region.array[elem * self.stride + j]

        start = index.start or 0
        stop = index.stop
        step = index.step or 1
        if start < 0:
            start = self.size + start
        if stop is None:
            stop = self.size
        elif stop < 0:
            stop = self.size + stop

        assert step == 1 or step % count == 0, \
            'Step must be multiple of component count'

        data_start = (start // count) * self.stride + start % count
        data_stop = (stop // count) * self.stride + stop % count
        data_step = step * self.stride

        #  TODO stepped getitem is probably wrong, see setitem for correct.
        value_step = step * count

        # ctypes does not support stepped slicing, so do the work in a list
        # and copy it back.
        data = self.region.array[:]
        value = [0] * ((stop - start) // step)
        stride = self.stride
        for i in range(count):
            value[i::value_step] = \
                data[data_start + i:data_stop + i:data_step]
        return value

    def __setitem__(self, index, value):
        count = self.count
        if not isinstance(index, slice):
            elem = index // count
            j = index % count
            self.region.array[elem * self.stride + j] = value
            return

        start = index.start or 0
        stop = index.stop
        step = index.step or 1
        if start < 0:
            start = self.size + start
        if stop is None:
            stop = self.size
        elif stop < 0:
            stop = self.size + stop

        assert step == 1 or step % count == 0, \
            'Step must be multiple of component count'

        data_start = (start // count) * self.stride + start % count
        data_stop = (stop // count) * self.stride + stop % count

        # ctypes does not support stepped slicing, so do the work in a list
        # and copy it back.
        data = self.region.array[:]
        if step == 1:
            data_step = self.stride
            value_step = count
            for i in range(count):
                data[data_start + i:data_stop + i:data_step] = \
                    value[i::value_step]
        else:
            data_step = (step // count) * self.stride
            data[data_start:data_stop:data_step] = value
        self.region.array[:] = data

    def invalidate(self):
        self.region.invalidate()
