# source: function_descriptor.pxi
from typing import Any, Callable, Generic, Protocol, TypeVar
from uuid import UUID

from typing_extensions import ParamSpec

from ray.includes.unique_ids import FunctionID

_FDArgs = ParamSpec("_FDArgs")
class _Initializable(Protocol,Generic[_FDArgs]):
    def __init__(self,*args:_FDArgs.args,**kwargs:_FDArgs.kwargs): ...

_FDReturn = TypeVar("_FDReturn")
class FunctionDescriptor(Generic[_FDArgs,_FDReturn]): # TODO: FUNCTION DESCRIPTOR FOR GENERATOR/STREAMING TYPES?
    def __cinit__(self, *args, **kwargs)->None: ...

    def __hash__(self)->int: ...

    def __eq__(self, other)->bool: ...

    def __repr__(self)->str: ...

    def to_dict(self)->dict: ...

    @property
    def repr(self)->str: ...

class EmptyFunctionDescriptor(FunctionDescriptor[Any,None]):

    def __reduce__(self)->tuple[type[EmptyFunctionDescriptor],tuple[()]]: ...

class JavaFunctionDescriptor(FunctionDescriptor[_FDArgs,_FDReturn]):

    def __cinit__(self,
                  class_name:str,
                  function_name:str,
                  signature:str)->None: ...

    def __reduce__(self)->tuple[type[JavaFunctionDescriptor[_FDArgs,_FDReturn]],tuple[str,str,str]]: ...

    @property
    def class_name(self)->str: ...

    @property
    def function_name(self)->str: ...

    @property
    def signature(self)->str: ...


class PythonFunctionDescriptor(FunctionDescriptor[_FDArgs,_FDReturn]):

    def __init__(self, # originally __cinit__
                  module_name:str,
                  function_name:str,
                  class_name:str="",
                  function_source_hash:str=""): ...

    def __reduce__(self)->tuple[type[PythonFunctionDescriptor[_FDArgs,_FDReturn]],tuple[str,str,str,str]]: ...


    @classmethod
    def from_function(cls:type[PythonFunctionDescriptor], function:Callable[_FDArgs,_FDReturn], function_uuid:UUID)->PythonFunctionDescriptor[_FDArgs,_FDReturn]:
        """Create a FunctionDescriptor from a function instance.

        This function is used to create the function descriptor from
        a python function. If a function is a class function, it should
        not be used by this function.

        Args:
            cls: Current class which is required argument for classmethod.
            function: the python function used to create the function
                descriptor.
            function_uuid: Used to uniquely identify a function.
                Ideally we can use the pickled function bytes
                but cloudpickle isn't stable in some cases
                for the same function.

        Returns:
            The FunctionDescriptor instance created according to the function.
        """
        ...

    @classmethod
    def from_class(cls:type[PythonFunctionDescriptor], target_class:type[_Initializable[_FDArgs]])->PythonFunctionDescriptor[_FDArgs,None]:
        """Create a FunctionDescriptor from a class.

        Args:
            cls: Current class which is required argument for classmethod.
            target_class: the python class used to create the function
                descriptor.

        Returns:
            The FunctionDescriptor instance created according to the class.
        """
        ...

    @property
    def module_name(self)->str:
        """Get the module name of current function descriptor.

        Returns:
            The module name of the function descriptor.
        """
        ...

    @property
    def class_name(self)->str:
        """Get the class name of current function descriptor.

        Returns:
            The class name of the function descriptor. It could be
                empty if the function is not a class method.
        """
        ...

    @property
    def function_name(self)->str:
        """Get the function name of current function descriptor.

        Returns:
            The function name of the function descriptor.
        """
        ...

    @property
    def function_hash(self)->str:
        """Get the hash string of the function source code.

        Returns:
            The hex of function hash if the source code is available.
                Otherwise, it will be an empty string.
        """
        ...

    @property
    def function_id(self)->FunctionID:
        """Get the function id calculated from this descriptor.

        Returns:
            The value of ray.ObjectRef that represents the function id.
        """
        ...

    @property
    def repr(self)->str:
        """Get the module_name.Optional[class_name].function_name
            of the descriptor.

        Returns:
            The value of module_name.Optional[class_name].function_name
        """
        ...

    def _get_function_id(self)->FunctionID:
        """Calculate the function id of current function descriptor.

        This function id is calculated from all the fields of function
        descriptor.

        Returns:
            ray.ObjectRef to represent the function descriptor.
        """
        ...

    @staticmethod
    def _get_module_name(object)->str:
        """Get the module name from object. If the module is __main__,
        get the module name from file.

        Returns:
            Module name of object.
        """
        ...

    def is_actor_method(self)->bool:
        """Wether this function descriptor is an actor method.

        Returns:
            True if it's an actor method, False if it's a normal function.
        """
        ...


class CppFunctionDescriptor(FunctionDescriptor):
    def __init__(self, # originally __cinit__
                  function_name:str, caller:str, class_name:str=""): ...

    def __reduce__(self)->tuple[type[CppFunctionDescriptor],tuple[str,str,str]]: ...


    @property
    def function_name(self)->str:
        """Get the function name of current function descriptor.

        Returns:
            The function name of the function descriptor.
        """
        ...

    @property
    def caller(self)->str:
        """Get the caller of current function descriptor.

        Returns:
            The caller of the function descriptor.
        """
        ...

    @property
    def class_name(self)->str:
        """Get the class name of current function descriptor,
        when it is empty, it is a non-member function.

        Returns:
            The class name of the function descriptor.
        """
        ...

    @property
    def repr(self)->str:
        ...
