import ray
from typing import List, Any


#########################
# Basic Implementations #
#########################

@ray.remote
class Basic:
    """
    Able to access the multipart object by index.
    Downside: must download the entire multipart object on ray.get()
    """
    
    def __init__(self, items: List[Any]):
         self.items = items
    def __getitem__(self, index: Any) -> Any:
         return self.items[index]


@ray.remote
class RefBased:
    """
    Does not require you to download the entire multipart object.
    Downside: must generate one object ref per part.
    # Note: does this even have any upside over the Basic implementation?
            Doesn't the driver still need to track all the nested references?
    """

    def __init__(self, items: List[Any]):
         self.items = items
    def __getitem__(self, index: Any) -> Any:
         return self.items[index]


###############################################
# Advanced Implementation - API Brainstorming #
###############################################

class MultiPartObjectRef_ExplicitOffsets:

    # API
    def __init__(self, items: List[Any], offsets: List[int]):
        # Problem: offsets is necessary to index into items array,
        # but how is the user going to know byte offsets? Also,
        # user might want to use items like a dictionary (i.e. with)
        # non-integer indices

        self.offsets = offsets
        self.items = items

    def __getitem__(self, index: Any) -> Any:
         return self.items[index]


class MultiPartObjectRef_ImplicitOffsets:

    # API
    def __init__(self, items: List[Any]):
        # No offsets passed in --> need to be calculated somewhere on the
        # backend. If items uses non-integer indices, the mapping should also
        # be stored somewhere on the backend.

        self.items = items

    def __getitem__(self, index: Any) -> Any:
         return self.items[index]