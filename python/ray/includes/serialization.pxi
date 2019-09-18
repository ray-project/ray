from cpython cimport Py_buffer, PY_VERSION_HEX
from libc.string cimport strlen, memcpy
from ray.includes.common cimport PyMemoryView_FromBuffer
from libc.stdint cimport uintptr_t
from cython.parallel cimport prange

# This is the default alignment value for len(buffer) < 2048
DEF kMinorBufferAlign = 8
# This is the default alignment value for len(buffer) >= 2048. Arrow use it for possible SIMD acceleration.
DEF kMajorBufferAlign = 64
DEF kMajorBufferSize = 2048
DEF kMemcopyDefaultBlocksize = 64
DEF kMemcopyDefaultThreshold = 1024 * 1024


cdef int64_t padded_length(int64_t offset, int64_t alignment):
    return ((offset + alignment - 1) // alignment) * alignment


cdef uint8_t* pointer_logical_and(const uint8_t*address, uintptr_t bits):
    return <uint8_t*> ((<uintptr_t>address) & bits)


cdef void parallel_memcopy(uint8_t*dst, const uint8_t*src, int64_t nbytes,
                           uintptr_t block_size, int num_threads):
    # XXX This function is really using `num_threads + 1` threads.
    cdef:
        uint8_t*left = pointer_logical_and(src + block_size - 1, ~(block_size - 1))
        uint8_t*right = pointer_logical_and(src + nbytes, ~(block_size - 1))
        int64_t num_blocks = (right - left) // block_size
        size_t chunk_size
        int64_t prefix, suffix
        int i

    # Update right address
    right = right - (num_blocks % num_threads) * block_size

    # Now we divide these blocks between available threads. The remainder is
    # handled separately.
    chunk_size = (right - left) // num_threads
    prefix = left - src
    suffix = src + nbytes - right
    # Now the data layout is | prefix | k * num_threads * block_size | suffix |.
    # We have chunk_size = k * block_size, therefore the data layout is
    # | prefix | num_threads * chunk_size | suffix |.
    # Each thread gets a "chunk" of k blocks.

    for i in prange(num_threads, nogil=True, schedule='static'):
        memcpy(dst + prefix + i * chunk_size, left + i * chunk_size, chunk_size)
        if i == 0:
            memcpy(dst, src, prefix)
        if i == num_threads - 1:
            memcpy(dst + prefix + num_threads * chunk_size, right, suffix)


# Buffer serialization format:
# |  i64 len(meta)  |  meta  |  align:4  | i32 len(buffers) |           \
#    i32 len(buffer_meta) | i32 len(shape_strides) | i32 len(formats) | \
#    buffer_meta | shape_strides | formats | buffer |
#
# 'buffer_meta' format (designed for random buffer access):
# | i64 buffer_addr, i64 len, i64 itemsize, i32 readonly, i32 ndim, \
#   i32 format_offset, i32 shape_stride_offset |


def unpack_pickle5_buffers(Buffer buf):
    cdef:
        shared_ptr[CBuffer] _buffer = buf.buffer
        const uint8_t*data = buf.buffer.get().Data()
        size_t size = _buffer.get().Size()
        size_t buffer_meta_item_size = sizeof(int64_t) * 3 + sizeof(int32_t) * 4
        int64_t meta_length
        c_string meta_str
        int32_t n_buffers, buffer_meta_length, shape_stride_length, format_length
        int32_t buffer_meta_offset
        const uint8_t*buffer_meta_ptr
        const uint8_t*shape_stride_entry
        const uint8_t*format_entry
        const uint8_t*buffer_entry
        Py_buffer buffer
        int32_t format_offset, shape_offset

    if PY_VERSION_HEX < 0x03060000:
        raise ValueError("This function requires Python >=3.6 for pickle5 support.")

    # NOTE: This part is modified from the numpy implementation.
    # if the python version is below 3.8, the pickle module does not provide
    # built-in support for protocol 5. We try importing the pickle5
    # backport instead.
    if PY_VERSION_HEX >= 0x03080000:
        # we expect protocol 5 to be available in Python 3.8
        import pickle as pickle_module
    elif PY_VERSION_HEX >= 0x03060000:
        try:
            import pickle5 as pickle_module
        except ImportError:
            raise ImportError("Using pickle protocol 5 requires the "
                              "pickle5 module for Python >=3.6 and <3.8")
    else:
        raise ValueError("pickle protocol 5 is not available for Python < 3.6")
    picklebuf_class = pickle_module.PickleBuffer

    meta_length = (<int64_t*> data)[0]
    data += sizeof(int64_t)
    meta_str.append(<char*> data, <size_t> meta_length)
    data += meta_length
    data = <uint8_t*> padded_length(<int64_t> data, 4)
    n_buffers = (<int32_t*> data)[0]
    buffer_meta_length = (<int32_t*> data)[1]
    shape_stride_length = (<int32_t*> data)[2]
    format_length = (<int32_t*> data)[3]
    data += sizeof(int32_t) * 4
    # calculate entry offsets
    shape_stride_entry = data + buffer_meta_length
    format_entry = shape_stride_entry + shape_stride_length
    buffer_entry = <const uint8_t *> padded_length(<int64_t> (format_entry + format_length), kMajorBufferAlign)

    pickled_buffers = []
    # Now read buffer meta
    for buffer_meta_offset in range(0, n_buffers):
        buffer_meta_ptr = data + buffer_meta_offset * buffer_meta_item_size
        buffer.buf = <void*> (buffer_entry + (<int64_t*> buffer_meta_ptr)[0])
        buffer.len = (<int64_t*> buffer_meta_ptr)[1]
        buffer.itemsize = (<int64_t*> buffer_meta_ptr)[2]
        buffer_meta_ptr += sizeof(int64_t) * 3
        buffer.readonly = (<int32_t*> buffer_meta_ptr)[0]
        buffer.ndim = (<int32_t*> buffer_meta_ptr)[1]
        format_offset = (<int32_t*> buffer_meta_ptr)[2]
        shape_offset = (<int32_t*> buffer_meta_ptr)[3]
        if format_offset < 0:
            buffer.format = NULL
        else:
            buffer.format = <char *> (format_entry + format_offset)
        buffer.shape = <Py_ssize_t *> (shape_stride_entry + shape_offset)
        buffer.strides = buffer.shape + buffer.ndim
        buffer.internal = NULL
        buffer.suboffsets = NULL
        pickled_buffers.append(picklebuf_class(PyMemoryView_FromBuffer(&buffer)))
    return meta_str, pickled_buffers


cdef class Pickle5Writer:
    cdef:
        int32_t n_buffers
        c_vector[int32_t] ndims
        c_vector[int32_t] readonlys

        c_vector[int32_t] shape_stride_offsets
        c_vector[Py_ssize_t] shape_strides

        c_vector[char] formats
        c_vector[int32_t] format_offsets
        c_vector[void*] buffers
        int64_t buffer_bytes
        c_vector[int64_t] buffer_offsets
        c_vector[int64_t] buffer_lens
        c_vector[int64_t] itemsizes

    def __cinit__(self):
        self.n_buffers = 0
        self.buffer_bytes = 0

    def buffer_callback(self, pickle_buffer):
        cdef:
            Py_buffer view
            int64_t buf_len
            size_t format_len
        cpython.PyObject_GetBuffer(pickle_buffer, &view, cpython.PyBUF_RECORDS_RO)
        self.n_buffers += 1

        self.ndims.push_back(view.ndim)
        self.readonlys.push_back(view.readonly)
        ## => Shape & strides
        self.shape_stride_offsets.push_back(self.shape_strides.size() * sizeof(Py_ssize_t))
        self.shape_strides.insert(self.shape_strides.end(), view.shape, view.shape + view.ndim)
        self.shape_strides.insert(self.shape_strides.end(), view.strides, view.strides + view.ndim)

        ## => Format
        if view.format:
            self.format_offsets.push_back(self.formats.size())
            format_len = strlen(view.format)
            # Also copy '\0'
            self.formats.insert(self.formats.end(), view.format, view.format + format_len + 1)
        else:
            self.format_offsets.push_back(-1)

        ## => Buffer
        self.buffer_lens.push_back(view.len)
        self.itemsizes.push_back(view.itemsize)

        if view.len < kMajorBufferSize:
            self.buffer_bytes = padded_length(self.buffer_bytes, kMinorBufferAlign)
        else:
            self.buffer_bytes = padded_length(self.buffer_bytes, kMajorBufferAlign)
        self.buffer_offsets.push_back(self.buffer_bytes)
        self.buffer_bytes += view.len
        self.buffers.push_back(view.buf)

    def get_total_bytes(self, const c_string & meta):
        cdef int64_t total_bytes = 0
        total_bytes = sizeof(int64_t) + meta.length()  # len(meta),  meta
        total_bytes = padded_length(total_bytes, 4)  # align to 4
        # i32 len(buffers), i32 len(buffer_meta), i32 len(shape_strides), i32 len(formats)
        total_bytes += sizeof(int32_t) * 4
        # buffer meta
        total_bytes += self.n_buffers * (sizeof(int64_t) * 3 + sizeof(int32_t) * 4)
        total_bytes += self.shape_strides.size() * sizeof(Py_ssize_t)
        total_bytes += self.formats.size()
        total_bytes = padded_length(total_bytes, kMajorBufferAlign)
        total_bytes += self.buffer_bytes
        return total_bytes

    cdef void write_to(self, const c_string & meta, shared_ptr[CBuffer] data, int memcopy_threads):
        # TODO(suquark): Use flatbuffer instead; support memcopy threads
        cdef uint8_t*ptr = data.get().Data()
        cdef int i
        (<int64_t*> ptr)[0] = meta.length()
        ptr += sizeof(int64_t)
        memcpy(ptr, meta.c_str(), meta.length())
        ptr += meta.length()
        ptr = <uint8_t*> padded_length(<int64_t> ptr, 4)
        (<int32_t*> ptr)[0] = self.n_buffers
        (<int32_t*> ptr)[1] = <int32_t> (self.n_buffers * (sizeof(int64_t) * 3 + sizeof(int32_t) * 4))
        (<int32_t*> ptr)[2] = <int32_t> (self.shape_strides.size() * sizeof(Py_ssize_t))
        (<int32_t*> ptr)[3] = <int32_t> (self.formats.size())
        ptr += sizeof(int32_t) * 4
        for i in range(self.n_buffers):
            (<int64_t*> ptr)[0] = self.buffer_offsets[i]
            (<int64_t*> ptr)[1] = self.buffer_lens[i]
            (<int64_t*> ptr)[2] = self.itemsizes[i]
            ptr += sizeof(int64_t) * 3
            (<int32_t*> ptr)[0] = self.readonlys[i]
            (<int32_t*> ptr)[1] = self.ndims[i]
            (<int32_t*> ptr)[2] = self.format_offsets[i]
            (<int32_t*> ptr)[3] = self.shape_stride_offsets[i]
            ptr += sizeof(int32_t) * 4
        memcpy(ptr, self.shape_strides.data(), self.shape_strides.size() * sizeof(Py_ssize_t))
        ptr += self.shape_strides.size() * sizeof(Py_ssize_t)
        memcpy(ptr, self.formats.data(), self.formats.size())
        ptr += self.formats.size()
        ptr = <uint8_t*> padded_length(<int64_t> ptr, kMajorBufferAlign)
        for i in range(self.n_buffers):
            if self.buffer_lens[i] > kMemcopyDefaultThreshold:
                parallel_memcopy(ptr + self.buffer_offsets[i], <const uint8_t*> self.buffers[i], self.buffer_lens[i],
                                 kMemcopyDefaultBlocksize, memcopy_threads)
            else:
                memcpy(ptr + self.buffer_offsets[i], self.buffers[i], self.buffer_lens[i])
