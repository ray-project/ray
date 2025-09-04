# source: buffer.pxi
class Buffer:
    """Cython wrapper class of C++ `ray::Buffer`.

    This class implements the Python 'buffer protocol', which allows
    us to use it for calls into Python libraries without having to
    copy the data.

    See https://docs.python.org/3/c-api/buffer.html for details.
    """

    def __len__(self)->int: ...

    @property
    def size(self)->int: ...

    def to_pybytes(self)->bytes: ...

    # def __getbuffer__(self, buffer, flags:int)->None: ...

    # def __getsegcount__(self, len_out:int)->int: ...

    # def __getreadbuffer__(self, idx:int, p)->int: ...

    # def __getwritebuffer__(self, idx:int, p)->int: ...
