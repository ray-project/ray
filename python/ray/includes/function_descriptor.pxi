from ray.includes.function_descriptor cimport (
    CFunctionDescriptor,
    CFunctionDescriptorBuilder,
    CPythonFunctionDescriptor,
    CJavaFunctionDescriptor,
    CCppFunctionDescriptor,
    EmptyFunctionDescriptorType,
    JavaFunctionDescriptorType,
    PythonFunctionDescriptorType,
    CppFunctionDescriptorType,
)

import hashlib
import cython
import inspect
import uuid
import ray._private.ray_constants as ray_constants


ctypedef object (*FunctionDescriptor_from_cpp)(const CFunctionDescriptor &)
cdef unordered_map[int, FunctionDescriptor_from_cpp] \
    FunctionDescriptor_constructor_map
cdef CFunctionDescriptorToPython(CFunctionDescriptor function_descriptor):
    cdef int function_descriptor_type = <int>function_descriptor.get().Type()
    it = FunctionDescriptor_constructor_map.find(function_descriptor_type)
    if it == FunctionDescriptor_constructor_map.end():
        raise Exception("Can't construct FunctionDescriptor from type {}"
                        .format(function_descriptor_type))
    else:
        constructor = dereference(it).second
        return constructor(function_descriptor)


@cython.auto_pickle(False)
cdef class FunctionDescriptor:
    def __cinit__(self, *args, **kwargs):
        if type(self) == FunctionDescriptor:
            raise Exception("type {} is abstract".format(type(self).__name__))

    def __hash__(self):
        return hash(self.descriptor.get().ToString())

    def __eq__(self, other):
        return (type(self) == type(other) and
                self.descriptor.get().ToString() ==
                (<FunctionDescriptor>other).descriptor.get().ToString())

    def __repr__(self):
        return <str>self.descriptor.get().ToString()

    def to_dict(self):
        d = {"type": type(self).__name__}
        for k, v in vars(type(self)).items():
            if inspect.isgetsetdescriptor(v):
                d[k] = v.__get__(self)
        return d


FunctionDescriptor_constructor_map[<int>EmptyFunctionDescriptorType] = \
    EmptyFunctionDescriptor.from_cpp


@cython.auto_pickle(False)
cdef class EmptyFunctionDescriptor(FunctionDescriptor):
    def __cinit__(self):
        self.descriptor = CFunctionDescriptorBuilder.Empty()

    def __reduce__(self):
        return EmptyFunctionDescriptor, ()

    @staticmethod
    cdef from_cpp(const CFunctionDescriptor &c_function_descriptor):
        return EmptyFunctionDescriptor()


FunctionDescriptor_constructor_map[<int>JavaFunctionDescriptorType] = \
    JavaFunctionDescriptor.from_cpp


@cython.auto_pickle(False)
cdef class JavaFunctionDescriptor(FunctionDescriptor):
    cdef:
        CJavaFunctionDescriptor *typed_descriptor

    def __cinit__(self,
                  class_name,
                  function_name,
                  signature):
        self.descriptor = CFunctionDescriptorBuilder.BuildJava(
            class_name, function_name, signature)
        self.typed_descriptor = <CJavaFunctionDescriptor*>(
            self.descriptor.get())

    def __reduce__(self):
        return JavaFunctionDescriptor, (self.typed_descriptor.ClassName(),
                                        self.typed_descriptor.FunctionName(),
                                        self.typed_descriptor.Signature())

    @staticmethod
    cdef from_cpp(const CFunctionDescriptor &c_function_descriptor):
        cdef CJavaFunctionDescriptor *typed_descriptor = \
            <CJavaFunctionDescriptor*>(c_function_descriptor.get())
        return JavaFunctionDescriptor(typed_descriptor.ClassName(),
                                      typed_descriptor.FunctionName(),
                                      typed_descriptor.Signature())

    @property
    def class_name(self):
        """Get the class name of current function descriptor.

        Returns:
            The class name of the function descriptor. It could be
                empty if the function is not a class method.
        """
        return <str>self.typed_descriptor.ClassName()

    @property
    def function_name(self):
        """Get the function name of current function descriptor.

        Returns:
            The function name of the function descriptor.
        """
        return <str>self.typed_descriptor.FunctionName()

    @property
    def signature(self):
        """Get the signature of current function descriptor.

        Returns:
            The signature of the function descriptor.
        """
        return <str>self.typed_descriptor.Signature()


FunctionDescriptor_constructor_map[<int>PythonFunctionDescriptorType] = \
    PythonFunctionDescriptor.from_cpp


@cython.auto_pickle(False)
cdef class PythonFunctionDescriptor(FunctionDescriptor):
    cdef:
        CPythonFunctionDescriptor *typed_descriptor
        object _function_id

    def __cinit__(self,
                  module_name,
                  function_name,
                  class_name="",
                  function_source_hash=""):
        self.descriptor = CFunctionDescriptorBuilder.BuildPython(
            module_name, class_name, function_name, function_source_hash)
        self.typed_descriptor = <CPythonFunctionDescriptor*>(
            self.descriptor.get())

    def __reduce__(self):
        return PythonFunctionDescriptor, (self.typed_descriptor.ModuleName(),
                                          self.typed_descriptor.FunctionName(),
                                          self.typed_descriptor.ClassName(),
                                          self.typed_descriptor.FunctionHash())

    @staticmethod
    cdef from_cpp(const CFunctionDescriptor &c_function_descriptor):
        cdef CPythonFunctionDescriptor *typed_descriptor = \
            <CPythonFunctionDescriptor*>(c_function_descriptor.get())
        return PythonFunctionDescriptor(typed_descriptor.ModuleName(),
                                        typed_descriptor.FunctionName(),
                                        typed_descriptor.ClassName(),
                                        typed_descriptor.FunctionHash())

    @classmethod
    def from_function(cls, function, function_uuid):
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
        module_name = cls._get_module_name(function)
        function_name = function.__qualname__
        class_name = ""

        return cls(module_name, function_name, class_name, function_uuid.hex)

    @classmethod
    def from_class(cls, target_class):
        """Create a FunctionDescriptor from a class.

        Args:
            cls: Current class which is required argument for classmethod.
            target_class: the python class used to create the function
                descriptor.

        Returns:
            The FunctionDescriptor instance created according to the class.
        """
        module_name = cls._get_module_name(target_class)
        class_name = target_class.__qualname__
        # Use a random uuid as function hash to solve actor name conflict.
        return cls(module_name, "__init__", class_name, uuid.uuid4().hex)

    @property
    def module_name(self):
        """Get the module name of current function descriptor.

        Returns:
            The module name of the function descriptor.
        """
        return <str>self.typed_descriptor.ModuleName()

    @property
    def class_name(self):
        """Get the class name of current function descriptor.

        Returns:
            The class name of the function descriptor. It could be
                empty if the function is not a class method.
        """
        return <str>self.typed_descriptor.ClassName()

    @property
    def function_name(self):
        """Get the function name of current function descriptor.

        Returns:
            The function name of the function descriptor.
        """
        return <str>self.typed_descriptor.FunctionName()

    @property
    def function_hash(self):
        """Get the hash string of the function source code.

        Returns:
            The hex of function hash if the source code is available.
                Otherwise, it will be an empty string.
        """
        return <str>self.typed_descriptor.FunctionHash()

    @property
    def function_id(self):
        """Get the function id calculated from this descriptor.

        Returns:
            The value of ray.ObjectRef that represents the function id.
        """
        if not self._function_id:
            self._function_id = self._get_function_id()
        return self._function_id

    @property
    def repr(self):
        """Get the module_name.Optional[class_name].function_name
            of the descriptor.

        Returns:
            The value of module_name.Optional[class_name].function_name
        """
        if self.is_actor_method():
            return ".".join(
                [self.module_name, self.class_name, self.function_name])
        else:
            return ".".join(
                [self.module_name, self.function_name])

    def _get_function_id(self):
        """Calculate the function id of current function descriptor.

        This function id is calculated from all the fields of function
        descriptor.

        Returns:
            ray.ObjectRef to represent the function descriptor.
        """
        function_id_hash = hashlib.shake_128()
        # Include the function module and name in the hash.
        function_id_hash.update(self.typed_descriptor.ModuleName())
        function_id_hash.update(self.typed_descriptor.FunctionName())
        function_id_hash.update(self.typed_descriptor.ClassName())
        function_id_hash.update(self.typed_descriptor.FunctionHash())
        # Compute the function ID.
        function_id = function_id_hash.digest(ray_constants.ID_SIZE)
        return ray.FunctionID(function_id)

    @staticmethod
    def _get_module_name(object):
        """Get the module name from object. If the module is __main__,
        get the module name from file.

        Returns:
            Module name of object.
        """
        module_name = object.__module__
        if module_name == "__main__":
            try:
                file_path = inspect.getfile(object)
                n = inspect.getmodulename(file_path)
                if n:
                    module_name = n
            except (TypeError, OSError):
                pass
        return module_name

    def is_actor_method(self):
        """Wether this function descriptor is an actor method.

        Returns:
            True if it's an actor method, False if it's a normal function.
        """
        return not self.typed_descriptor.ClassName().empty()


FunctionDescriptor_constructor_map[<int>CppFunctionDescriptorType] = \
    CppFunctionDescriptor.from_cpp


@cython.auto_pickle(False)
cdef class CppFunctionDescriptor(FunctionDescriptor):
    cdef:
        CCppFunctionDescriptor *typed_descriptor

    def __cinit__(self,
                  function_name, caller, class_name=""):
        self.descriptor = CFunctionDescriptorBuilder.BuildCpp(
            function_name, caller, class_name)
        self.typed_descriptor = <CCppFunctionDescriptor*>(
            self.descriptor.get())

    def __reduce__(self):
        return CppFunctionDescriptor, (self.typed_descriptor.FunctionName(),
                                       self.typed_descriptor.Caller(),
                                       self.typed_descriptor.ClassName())

    @staticmethod
    cdef from_cpp(const CFunctionDescriptor &c_function_descriptor):
        cdef CCppFunctionDescriptor *typed_descriptor = \
            <CCppFunctionDescriptor*>(c_function_descriptor.get())
        return CppFunctionDescriptor(typed_descriptor.FunctionName(),
                                     typed_descriptor.Caller(),
                                     typed_descriptor.ClassName())

    @property
    def function_name(self):
        """Get the function name of current function descriptor.

        Returns:
            The function name of the function descriptor.
        """
        return <str>self.typed_descriptor.FunctionName()

    @property
    def caller(self):
        """Get the caller of current function descriptor.

        Returns:
            The caller of the function descriptor.
        """
        return <str>self.typed_descriptor.Caller()

    @property
    def class_name(self):
        """Get the class name of current function descriptor,
        when it is empty, it is a non-member function.

        Returns:
            The class name of the function descriptor.
        """
        return <str>self.typed_descriptor.ClassName()
