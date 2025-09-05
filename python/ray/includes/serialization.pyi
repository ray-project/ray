# source: serialization.pxi
import contextlib
from typing import Any, Callable, Generator, Sequence, TypeVar, Union

from ray._raylet import ObjectRef
from ray.includes.buffer import Buffer

try:
    from collections.abc import Buffer as _Buffer  # type: ignore[attr-defined]
except ImportError:
    try:
        from typing_extensions import Buffer as _Buffer  # type: ignore[no-redef]
    except ImportError:
        import abc
        # same fake Buffer class as in typing_extensions
        class _Buffer(abc.ABC):  # type: ignore[no-redef] # noqa: B024
            """Base class for classes that implement the buffer protocol.

            The buffer protocol allows Python objects to expose a low-level
            memory buffer interface. Before Python 3.12, it is not possible
            to implement the buffer protocol in pure Python code, or even
            to check whether a class implements the buffer protocol. In
            Python 3.12 and higher, the ``__buffer__`` method allows access
            to the buffer protocol from Python code, and the
            ``collections.abc.Buffer`` ABC allows checking whether a class
            implements the buffer protocol.

            To indicate support for the buffer protocol in earlier versions,
            inherit from this ABC, either in a stub file or at runtime,
            or use ABC registration. This ABC provides no methods, because
            there is no Python-accessible methods shared by pre-3.12 buffer
            classes. It is useful primarily for static checks.

            """

        # As a courtesy, register the most common stdlib buffer classes.
        _Buffer.register(memoryview)
        _Buffer.register(bytearray)
        _Buffer.register(bytes)


_O = TypeVar("_O")
class MessagePackSerializer(object):
    @staticmethod
    def dumps(o:_O, python_serializer:Union[Callable[[_O],Any],None]=None)->bytes: ...

    @classmethod
    def loads(cls, s:_Buffer, python_deserializer:Union[Callable,None]=None)->object: ...

class Pickle5Writer:

    def __dealloc__(self)->None: ...

    def buffer_callback(self, pickle_buffer:object)->None: ...

    def get_total_bytes(self, inband:bytes)->int: ... # TODO: bytes-like object (buffer protocol - 3.12 only!)

    # doesn't exist in the python object
    # def write_to(self, inband:bytes, data:bytes,
    #                    memcopy_threads:int)->None:

# TODO: Can this be made generic? See: CoreWorker.put_object
class SerializedObject(object):

    def __init__(self, metadata:bytes, contained_object_refs:Union[Sequence[ObjectRef],None]=None):
        self._metadata = metadata
        self._contained_object_refs = contained_object_refs or []

    @property
    def total_bytes(self)->int: ...

    @property
    def metadata(self)->bytes: ...

    @property
    def contained_object_refs(self)->Sequence[ObjectRef]: ...


class Pickle5SerializedObject(SerializedObject):

    def __init__(self, metadata:bytes, inband:bytes, writer: Pickle5Writer,contained_object_refs: Union[Sequence[ObjectRef],None]=None)->None: ...


class MessagePackSerializedObject(SerializedObject):

    def __init__(self, metadata:bytes, msgpack_data:bytes, contained_object_refs:Sequence[ObjectRef],
                 nest_serialized_object:Union[SerializedObject,None]=None)->None: ...

    def to_bytes(self)->bytes: ...

class RawSerializedObject(SerializedObject):

    def __init__(self, value:object)->None: ...

class SubBuffer:
    def __init__(self,buffer:_Buffer)->None: ...
    def __len__(self)->int: ...
    def nbytes(self)->int: ...
    def readonly(self)->bool: ...
    def tobytes(self)->bytes: ...

@contextlib.contextmanager
def _temporarily_disable_gc()->Generator[None,None,None]: ...


# I think this should work cross-version?
def split_buffer(buf:Buffer)->tuple[_Buffer,_Buffer]: ...
def unpack_pickle5_buffers(bufferview:_Buffer)->tuple[_Buffer,list[SubBuffer]]: ... # TODO: Buffer typing?
