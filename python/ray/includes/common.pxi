from libcpp cimport bool as c_bool
from libcpp.string cimport string as c_string
from libcpp.vector cimport vector as c_vector

from ray.includes.common cimport (
    CObjectLocation,
    CGcsClientOptions,
    CPythonGcsClient,
    CPythonGcsPublisher,
    CPythonGcsSubscriber,
    kWorkerSetupHookKeyName,
    kResourceUnitScaling,
    kImplicitResourcePrefix,
    kStreamingGeneratorReturn,
)


cdef class GcsClientOptions:
    """Cython wrapper class of C++ `ray::gcs::GcsClientOptions`."""
    cdef:
        unique_ptr[CGcsClientOptions] inner

    @classmethod
    def from_gcs_address(cls, gcs_address):
        self = GcsClientOptions()
        try:
            ip, port = gcs_address.split(":", 2)
            port = int(port)
            self.inner.reset(
                new CGcsClientOptions(ip, port))
        except Exception:
            raise ValueError(f"Invalid gcs_address: {gcs_address}")
        return self

    cdef CGcsClientOptions* native(self):
        return <CGcsClientOptions*>(self.inner.get())


WORKER_PROCESS_SETUP_HOOK_KEY_NAME_GCS = str(kWorkerSetupHookKeyName)
RESOURCE_UNIT_SCALING = kResourceUnitScaling
IMPLICIT_RESOURCE_PREFIX = kImplicitResourcePrefix.decode()
STREAMING_GENERATOR_RETURN = kStreamingGeneratorReturn
