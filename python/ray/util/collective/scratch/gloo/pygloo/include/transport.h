#include <pybind11/functional.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl_bind.h>

#if GLOO_HAVE_TRANSPORT_TCP

#include <gloo/transport/tcp/address.h>
#include <gloo/transport/tcp/attr.h>
#include <gloo/transport/tcp/buffer.h>
#include <gloo/transport/tcp/context.h>
#include <gloo/transport/tcp/device.h>
#include <gloo/transport/tcp/loop.h>
#include <gloo/transport/tcp/pair.h>
#include <gloo/transport/tcp/unbound_buffer.h>

#endif

#if GLOO_HAVE_TRANSPORT_UV

#include <gloo/transport/uv/address.h>
#include <gloo/transport/uv/context.h>
#include <gloo/transport/uv/device.h>
#include <gloo/transport/uv/pair.h>
#include <gloo/transport/uv/unbound_buffer.h>

#endif


namespace pygloo {
namespace transport {
class PyDevice : public gloo::transport::Device {
public:
  using gloo::transport::Device::Device;

  std::string str() const override {
    PYBIND11_OVERRIDE_PURE(
        std::string,             /* Return type */
        gloo::transport::Device, /* Parent class */
        str, /* Name of function in C++ (must match Python name) */
             /* Argument(s) */
    );
  }

  const std::string &getPCIBusID() const override {
    PYBIND11_OVERRIDE_PURE(
        const std::string &,     /* Return type */
        gloo::transport::Device, /* Parent class */
        getPCIBusID, /* Name of function in C++ (must match Python name) */
                     /* Argument(s) */
    );
  }

  int getInterfaceSpeed() const override {
    PYBIND11_OVERRIDE(int,                     /* Return type */
                      gloo::transport::Device, /* Parent class */
                      getInterfaceSpeed, /* Name of function in C++ (must match
                                            Python name) */
                                         /* Argument(s) */
    );
  }

  bool hasGPUDirect() const override {
    PYBIND11_OVERRIDE(
        bool,                    /* Return type */
        gloo::transport::Device, /* Parent class */
        hasGPUDirect, /* Name of function in C++ (must match Python name) */
                      /* Argument(s) */
    );
  }

  std::shared_ptr<gloo::transport::Context> createContext(int rank,
                                                          int size) override {
    PYBIND11_OVERRIDE_PURE(
        std::shared_ptr<gloo::transport::Context>, /* Return type */
        gloo::transport::Device,                   /* Parent class */
        createContext, /* Name of function in C++ (must match Python name) */
        rank, size     /* Argument(s) */
    );
  }
};

void def_transport_module(pybind11::module &m);
void def_transport_tcp_module(pybind11::module &m);
void def_transport_uv_module(pybind11::module &m);

} // namespace transport
} // namespace pygloo