from ray.includes.function_descriptor cimport (
    CFunctionDescriptor,
    CFunctionDescriptorBuilder,
    CPythonFunctionDescriptor,
    CJavaFunctionDescriptor,
    DriverFunctionDescriptorType,
    JavaFunctionDescriptorType,
    PythonFunctionDescriptorType,
)

import hashlib
import cython


cdef dict FunctionDescriptor_type_map = {}
cdef CFunctionDescriptorToPython(CFunctionDescriptor function_descriptor):
    if len(FunctionDescriptor_type_map) == 0:
        for subtype in FunctionDescriptorType.__subclasses__():
            k = getattr(subtype, '__c_function_descriptor_type__')
            FunctionDescriptor_type_map[k] = subtype
    tp = FunctionDescriptor_type_map.get(<int>function_descriptor.get().Type())
    cdef FunctionDescriptorType instance = tp.__new__(tp)
    instance.__setstate__(function_descriptor.get().Serialize())
    return instance


@cython.auto_pickle(False)
cdef class FunctionDescriptorType:
    cdef:
        CFunctionDescriptor descriptor

    def __init__(self):
        raise Exception("type {} is abstract".format(type(self).__name__))

    def __setstate__(self, state):
        self.descriptor = CFunctionDescriptorBuilder.Deserialize(state)

    def __getstate__(self):
        return self.descriptor.get().Serialize()

    def __hash__(self):
        return hash(self.descriptor.get().ToString())

    def __repr__(self):
        return self.descriptor.get().ToString().decode('ascii')


@cython.auto_pickle(False)
cdef class DriverFunctionDescriptor(FunctionDescriptorType):
    __c_function_descriptor_type__ = <int>DriverFunctionDescriptorType

    def __init__(self):
        self.descriptor = CFunctionDescriptorBuilder.BuildDriver()


@cython.auto_pickle(False)
cdef class JavaFunctionDescriptor(FunctionDescriptorType):
    cdef:
        CJavaFunctionDescriptor *typed_descriptor

    __c_function_descriptor_type__ = <int>JavaFunctionDescriptorType

    def __init__(self,
                 str class_name,
                 str function_name,
                 str signature):
        self.descriptor = CFunctionDescriptorBuilder.BuildJava(
            class_name, function_name, signature)
        self.typed_descriptor = <CJavaFunctionDescriptor*>(
            self.descriptor.get())

    def __setstate__(self, state):
        super(JavaFunctionDescriptor, self).__setstate__(state)
        self.typed_descriptor = <CJavaFunctionDescriptor*>(
            self.descriptor.get())

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


@cython.auto_pickle(False)
cdef class PythonFunctionDescriptor(FunctionDescriptorType):
    cdef:
        CPythonFunctionDescriptor *typed_descriptor
        object _function_id

    __c_function_descriptor_type__ = <int>PythonFunctionDescriptorType

    def __init__(self,
                 str module_name,
                 str function_name,
                 str class_name="",
                 str function_source_hash=""):
        self.descriptor = CFunctionDescriptorBuilder.BuildPython(
            module_name, class_name, function_name, function_source_hash)
        self.typed_descriptor = <CPythonFunctionDescriptor*>(
            self.descriptor.get())

    def __setstate__(self, state):
        super(PythonFunctionDescriptor, self).__setstate__(state)
        self.typed_descriptor = <CPythonFunctionDescriptor*>(
            self.descriptor.get())

    @classmethod
    def from_function(cls, function, pickled_function):
        """Create a FunctionDescriptor from a function instance.

        This function is used to create the function descriptor from
        a python function. If a function is a class function, it should
        not be used by this function.

        Args:
            cls: Current class which is required argument for classmethod.
            function: the python function used to create the function
                descriptor.
            pickled_function: This is factored in to ensure that any
                modifications to the function result in a different function
                descriptor.

        Returns:
            The FunctionDescriptor instance created according to the function.
        """
        module_name = function.__module__
        function_name = function.__name__
        class_name = ""

        pickled_function_hash = hashlib.sha1(pickled_function).hexdigest()

        return cls(module_name, function_name, class_name,
                   pickled_function_hash)

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
        module_name = target_class.__module__
        class_name = target_class.__name__
        return cls(module_name, "__init__", class_name)

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
            The value of ray.ObjectID that represents the function id.
        """
        if not hasattr(self, "_function_id"):
            self._function_id = self._get_function_id()
        return self._function_id

    def _get_function_id(self):
        """Calculate the function id of current function descriptor.

        This function id is calculated from all the fields of function
        descriptor.

        Returns:
            ray.ObjectID to represent the function descriptor.
        """
        function_id_hash = hashlib.sha1()
        # Include the function module and name in the hash.
        function_id_hash.update(self.typed_descriptor.ModuleName())
        function_id_hash.update(self.typed_descriptor.FunctionName())
        function_id_hash.update(self.typed_descriptor.ClassName())
        function_id_hash.update(self.typed_descriptor.FunctionHash())
        # Compute the function ID.
        function_id = function_id_hash.digest()
        return ray.FunctionID(function_id)

    def is_actor_method(self):
        """Wether this function descriptor is an actor method.

        Returns:
            True if it's an actor method, False if it's a normal function.
        """
        return not self.typed_descriptor.ClassName().empty()
