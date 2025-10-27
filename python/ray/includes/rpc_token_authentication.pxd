from libcpp cimport bool as c_bool

cdef extern from "ray/rpc/authentication/authentication_mode.h" namespace "ray::rpc" nogil:
    cdef enum CAuthenticationMode "ray::rpc::AuthenticationMode":
        DISABLED "ray::rpc::AuthenticationMode::DISABLED"
        TOKEN "ray::rpc::AuthenticationMode::TOKEN"

    CAuthenticationMode GetAuthenticationMode()

cdef extern from "ray/rpc/authentication/authentication_token_loader.h" namespace "ray::rpc" nogil:
    cdef cppclass CAuthenticationTokenLoader "ray::rpc::AuthenticationTokenLoader":
        @staticmethod
        CAuthenticationTokenLoader& instance()
        c_bool HasToken()
        void ResetCache()
