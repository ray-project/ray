#include <chrono>
#include <gloo/config.h>
#include <transport.h>

namespace pygloo{
namespace transport{
#if GLOO_HAVE_TRANSPORT_TCP

template <typename... Args>
using overload_cast_ = pybind11::detail::overload_cast_impl<Args...>;

void def_transport_tcp_module(pybind11::module &m)
{
    pybind11::module tcp = m.def_submodule("tcp", "This is a tcp module");

    tcp.def("CreateDevice", &gloo::transport::tcp::CreateDevice);

    pybind11::class_<gloo::transport::tcp::attr>(tcp, "attr")
        .def(pybind11::init<>())
        .def(pybind11::init<const char *>())
        .def_readwrite("hostname", &gloo::transport::tcp::attr::hostname)
        .def_readwrite("iface", &gloo::transport::tcp::attr::iface)
        .def_readwrite("ai_family", &gloo::transport::tcp::attr::ai_family)
        .def_readwrite("hostname", &gloo::transport::tcp::attr::hostname)
        .def_readwrite("ai_socktype", &gloo::transport::tcp::attr::ai_socktype)
        .def_readwrite("ai_protocol", &gloo::transport::tcp::attr::ai_protocol)
        .def_readwrite("ai_addr", &gloo::transport::tcp::attr::ai_addr)
        .def_readwrite("ai_addrlen", &gloo::transport::tcp::attr::ai_addrlen);

    pybind11::class_<gloo::transport::tcp::Context, std::shared_ptr<gloo::transport::tcp::Context>>(tcp, "Context")
        .def(pybind11::init<std::shared_ptr<gloo::transport::tcp::Device>, int, int>())
        // .def("createPair", &gloo::transport::tcp::Context::createPair)
        .def("createUnboundBuffer", &gloo::transport::tcp::Context::createUnboundBuffer)
        ;

    pybind11::class_<gloo::transport::tcp::Device,
            std::shared_ptr<gloo::transport::tcp::Device>, gloo::transport::Device>(tcp, "Device")
        .def(pybind11::init<const struct gloo::transport::tcp::attr&>())
        // .def("registerDescriptor", &gloo::transport::tcp::Device::registerDescriptor)
        // .def("unregisterDescriptor", &gloo::transport::tcp::Device::unregisterDescriptor)
        // .def("str", &gloo::transport::tcp::Device::str)
        // .def("getPCIBusID", &gloo::transport::tcp::Device::getPCIBusID)
        // .def("getInterfaceSpeed", &gloo::transport::tcp::Device::getInterfaceSpeed)
        // .def("createContext", &gloo::transport::tcp::Device::createContext, pybind11::return_value_policy::reference);
        ;

}
#else
void def_transport_tcp_module(pybind11::module &m)
{
    pybind11::module tcp = m.def_submodule("tcp", "This is a tcp module");
}
#endif

#if GLOO_HAVE_TRANSPORT_UV
#include <gloo/transport/uv/address.h>
#include <gloo/transport/uv/context.h>
#include <gloo/transport/uv/device.h>
#include <gloo/transport/uv/pair.h>
#include <gloo/transport/uv/unbound_buffer.h>

void def_transport_uv_module(pybind11::module &m)
{
    pybind11::module uv = m.def_submodule("uv", "This is a uv module");
    uv.def("CreateDevice", &gloo::transport::uv::CreateDevice, "CreateDevice");
}
#else
void def_transport_uv_module(pybind11::module &m)
{
    pybind11::module uv = m.def_submodule("uv", "This is a uv module");
}
#endif


void def_transport_module(pybind11::module &m)
{
    pybind11::module transport = m.def_submodule("transport", "This is a transport module");

    pybind11::class_<gloo::transport::Device,std::shared_ptr<gloo::transport::Device>, pygloo::transport::PyDevice>(transport, "Device", pybind11::module_local())
        .def("str", &gloo::transport::Device::str)
        .def("getPCIBusID", &gloo::transport::Device::getPCIBusID)
        .def("getInterfaceSpeed", &gloo::transport::Device::getInterfaceSpeed)
        .def("hasGPUDirect", &gloo::transport::Device::hasGPUDirect)
        .def("createContext", &gloo::transport::Device::createContext);

    def_transport_uv_module(transport);
    def_transport_tcp_module(transport);
}
}
}