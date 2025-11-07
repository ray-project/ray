from libcpp cimport bool as c_bool
from libcpp.string cimport string
from ray.includes.optional cimport optional


cdef extern from "ray/rpc/authentication/authentication_mode.h" namespace "ray::rpc" nogil:
    cdef enum CAuthenticationMode "ray::rpc::AuthenticationMode":
        DISABLED "ray::rpc::AuthenticationMode::DISABLED"
        TOKEN "ray::rpc::AuthenticationMode::TOKEN"
        K8S "ray::rpc::AuthenticationMode::K8S"

    CAuthenticationMode GetAuthenticationMode()

cdef extern from "ray/rpc/authentication/authentication_token.h" namespace "ray::rpc" nogil:
    cdef cppclass CAuthenticationToken "ray::rpc::AuthenticationToken":
        CAuthenticationToken()
        CAuthenticationToken(string value)
        c_bool empty()
        c_bool Equals(const CAuthenticationToken& other)
        string ToAuthorizationHeaderValue()
        string GetRawValue()
        @staticmethod
        CAuthenticationToken FromMetadata(string metadata_value)

cdef extern from "ray/rpc/authentication/authentication_token_loader.h" namespace "ray::rpc" nogil:
    cdef cppclass CAuthenticationTokenLoader "ray::rpc::AuthenticationTokenLoader":
        @staticmethod
        CAuthenticationTokenLoader& instance()
        c_bool HasToken()
        c_bool ValidateToken(const CAuthenticationToken& token)
        void ResetCache()
        optional[CAuthenticationToken] GetToken()
