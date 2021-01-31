#include <pybind11/functional.h>
#include <pybind11/pybind11.h>

#include <gloo/config.h>
#include <gloo/context.h>

// #include <transport.h>
#include <collective.h>
#include <rendezvous.h>
#include <sstream>

namespace pygloo {
bool transport_tcp_available() { return GLOO_HAVE_TRANSPORT_TCP; }

bool transport_uv_available() { return GLOO_HAVE_TRANSPORT_UV; }
} // namespace pygloo

PYBIND11_MODULE(pygloo, m) {
  m.doc() = "binding gloo from c to python"; // optional module docstring

  m.def("transport_tcp_available", &pygloo::transport_tcp_available,
        "transport_tcp_available");

  m.def("transport_uv_available", &pygloo::transport_uv_available,
        "transport_uv_available");

  pybind11::enum_<pygloo::ReduceOp>(m, "ReduceOp", pybind11::arithmetic())
      .value("SUM", pygloo::ReduceOp::SUM)
      .value("PRODUCT", pygloo::ReduceOp::PRODUCT)
      .value("MIN", pygloo::ReduceOp::MIN)
      .value("MAX", pygloo::ReduceOp::MAX)
      .value("BAND", pygloo::ReduceOp::BAND)
      .value("BOR", pygloo::ReduceOp::BOR)
      .value("BXOR", pygloo::ReduceOp::BXOR)
      .value("UNUSED", pygloo::ReduceOp::UNUSED)
      .export_values();

  pybind11::enum_<gloo::detail::AllreduceOptionsImpl::Algorithm>(
      m, "allreduceAlgorithm", pybind11::arithmetic())
      .value("SUM", gloo::detail::AllreduceOptionsImpl::Algorithm::UNSPECIFIED)
      .value("RING", gloo::detail::AllreduceOptionsImpl::Algorithm::RING)
      .value("BCUBE", gloo::detail::AllreduceOptionsImpl::Algorithm::BCUBE)
      .export_values();

  pybind11::enum_<pygloo::glooDataType_t>(m, "glooDataType_t",
                                          pybind11::arithmetic())
      .value("glooInt8", pygloo::glooDataType_t::glooInt8)
      .value("glooUint8", pygloo::glooDataType_t::glooUint8)
      .value("glooInt32", pygloo::glooDataType_t::glooInt32)
      .value("glooUint32", pygloo::glooDataType_t::glooUint32)
      .value("glooInt64", pygloo::glooDataType_t::glooInt64)
      .value("glooFloat16", pygloo::glooDataType_t::glooFloat16)
      .value("glooFloat32", pygloo::glooDataType_t::glooFloat32)
      .value("glooFloat64", pygloo::glooDataType_t::glooFloat64)
      .export_values();

  m.def("allgather", &pygloo::allgather_wrapper);
  m.def("allgatherv", &pygloo::allgatherv_wrapper);

  m.def("allreduce", &pygloo::allreduce_wrapper,
        pybind11::arg("context") = nullptr, pybind11::arg("sendbuf") = nullptr,
        pybind11::arg("recvbuf") = nullptr, pybind11::arg("size") = nullptr,
        pybind11::arg("datatype") = nullptr,
        pybind11::arg("reduceop") = pygloo::ReduceOp::SUM,
        pybind11::arg("algorithm") = gloo::AllreduceOptions::Algorithm::RING);

  m.def("reduce", &pygloo::reduce_wrapper, pybind11::arg("context") = nullptr,
        pybind11::arg("sendbuf") = nullptr, pybind11::arg("recvbuf") = nullptr,
        pybind11::arg("size") = nullptr, pybind11::arg("datatype") = nullptr,
        pybind11::arg("reduceop") = pygloo::ReduceOp::SUM,
        pybind11::arg("root") = 0);

  m.def("scatter", &pygloo::scatter_wrapper, pybind11::arg("context") = nullptr,
        pybind11::arg("sendbuf") = nullptr, pybind11::arg("recvbuf") = nullptr,
        pybind11::arg("size") = nullptr, pybind11::arg("datatype") = nullptr,
        pybind11::arg("root") = 0);

  m.def("send", &pygloo::send_wrapper);
  m.def("recv", &pygloo::recv_wrapper);

  m.def("broadcast", &pygloo::broadcast_wrapper,
        pybind11::arg("context") = nullptr, pybind11::arg("sendbuf") = nullptr,
        pybind11::arg("recvbuf") = nullptr, pybind11::arg("size") = nullptr,
        pybind11::arg("datatype") = nullptr, pybind11::arg("root") = 0);

  m.def("reduce_scatter", &pygloo::reduce_scatter_wrapper,
        pybind11::arg("context") = nullptr, pybind11::arg("sendbuf") = nullptr,
        pybind11::arg("recvbuf") = nullptr, pybind11::arg("size") = nullptr, pybind11::arg("recvElems") = nullptr,
        pybind11::arg("datatype") = nullptr,
        pybind11::arg("reduceop") = pygloo::ReduceOp::SUM);

  m.def("barrier", &pygloo::barrier);

  pybind11::class_<gloo::Context, std::shared_ptr<gloo::Context>>(m, "Context")
      .def(pybind11::init<int, int, int>(), pybind11::arg("rank") = nullptr,
           pybind11::arg("size") = nullptr, pybind11::arg("base") = 2)
      .def("getDevice", &gloo::Context::getDevice)
      .def_readonly("rank", &gloo::Context::rank)
      .def_readonly("size", &gloo::Context::size)
      .def_readwrite("base", &gloo::Context::base)
      // .def("getPair", &gloo::Context::getPair)
      .def("createUnboundBuffer", &gloo::Context::createUnboundBuffer)
      .def("nextSlot", &gloo::Context::nextSlot)
      .def("closeConnections", &gloo::Context::closeConnections)
      .def("setTimeout", &gloo::Context::setTimeout)
      .def("getTimeout", &gloo::Context::getTimeout);

  pygloo::transport::def_transport_module(m);
  pygloo::rendezvous::def_rendezvous_module(m);
}
