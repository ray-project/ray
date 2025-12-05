# source: serialization.pxi
from typing import Sequence, Union

from ray._raylet import ObjectRef

# TODO: Can this be made generic? See: CoreWorker.put_object
class SerializedObject(object):

    def __init__(self, metadata: bytes, contained_object_refs: Union[Sequence[ObjectRef],None]=None):
        self._metadata = metadata
        self._contained_object_refs = contained_object_refs or []

    @property
    def total_bytes(self) -> int: ...

    @property
    def metadata(self) -> bytes: ...

    @property
    def contained_object_refs(self) -> Sequence[ObjectRef]: ...
