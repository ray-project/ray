from libc.stdint cimport uint8_t, uint64_t
from libcpp cimport bool as c_bool
from libcpp.memory cimport unique_ptr, shared_ptr
from libcpp.string cimport string as c_string
from libcpp.unordered_map cimport unordered_map
from libcpp.vector cimport vector as c_vector

from ray.includes.common cimport (
    CLanguage,
)
from ray.includes.unique_ids cimport (
    CActorID,
    CJobID,
    CObjectID,
    CTaskID,
)

cdef extern from "src/ray/protobuf/common.pb.h" nogil:
    cdef cppclass CFunctionDescriptorType \
            "ray::FunctionDescriptorType":
        pass

    cdef CFunctionDescriptorType EmptyFunctionDescriptorType \
        "ray::FunctionDescriptorType::FUNCTION_DESCRIPTOR_NOT_SET"
    cdef CFunctionDescriptorType JavaFunctionDescriptorType \
        "ray::FunctionDescriptorType::kJavaFunctionDescriptor"
    cdef CFunctionDescriptorType PythonFunctionDescriptorType \
        "ray::FunctionDescriptorType::kPythonFunctionDescriptor"
    cdef CFunctionDescriptorType CppFunctionDescriptorType \
        "ray::FunctionDescriptorType::kCppFunctionDescriptor"


cdef extern from "ray/common/function_descriptor.h" nogil:
    cdef cppclass CFunctionDescriptorInterface \
            "ray::CFunctionDescriptorInterface":
        CFunctionDescriptorType Type()
        c_string ToString()
        c_string Serialize()

    ctypedef shared_ptr[CFunctionDescriptorInterface] CFunctionDescriptor \
        "ray::FunctionDescriptor"

    cdef cppclass CFunctionDescriptorBuilder "ray::FunctionDescriptorBuilder":
        @staticmethod
        CFunctionDescriptor Empty()

        @staticmethod
        CFunctionDescriptor BuildJava(const c_string &class_name,
                                      const c_string &function_name,
                                      const c_string &signature)

        @staticmethod
        CFunctionDescriptor BuildPython(const c_string &module_name,
                                        const c_string &class_name,
                                        const c_string &function_name,
                                        const c_string &function_source_hash)

        @staticmethod
        CFunctionDescriptor BuildCpp(const c_string &function_name,
                                     const c_string &caller,
                                     const c_string &class_name)

        @staticmethod
        CFunctionDescriptor Deserialize(const c_string &serialized_binary)

    cdef cppclass CJavaFunctionDescriptor "ray::JavaFunctionDescriptor":
        c_string ClassName()
        c_string FunctionName()
        c_string Signature()

    cdef cppclass CPythonFunctionDescriptor "ray::PythonFunctionDescriptor":
        c_string ModuleName()
        c_string ClassName()
        c_string FunctionName()
        c_string FunctionHash()

    cdef cppclass CCppFunctionDescriptor "ray::CppFunctionDescriptor":
        c_string FunctionName()
        c_string Caller()
        c_string ClassName()
