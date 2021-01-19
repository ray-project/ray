#include <pybind11/pybind11.h>
#include <pybind11/functional.h>

#include <gloo/context.h>
#include <gloo/config.h>

#include <gloo/allgather.h>
#include <gloo/allgatherv.h>
#include <gloo/allreduce.h>
#include <gloo/allreduce_ring.h>
#include <gloo/alltoall.h>
#include <gloo/alltoallv.h>
#include <gloo/barrier.h>
#include <gloo/broadcast.h>
#include <gloo/gather.h>
#include <gloo/reduce.h>
#include <gloo/scatter.h>


// #include <transport.h>
#include <rendezvous.h>
#include <collective.h>
#include <sstream>


bool transport_tcp_available(){
    return GLOO_HAVE_TRANSPORT_TCP;
}

bool transport_uv_available(){
    return GLOO_HAVE_TRANSPORT_UV;
}

template <typename T>
void declare_AllreduceRing(pybind11::module &m, const std::string &className)
{
    using Class = gloo::AllreduceRing<T>;
    pybind11::class_<Class>(m, className.c_str())
        .def(pybind11::init<const std::shared_ptr<gloo::Context> &,
                            const std::vector<T *> &,
                            const int,
                            const gloo::ReductionFunction<T> *>())
    // , pybind11::arg("fn")=gloo::ReductionFunction<T>::sum
        .def("run", &gloo::AllreduceRing<T>::run, "run");
}



PYBIND11_MODULE(pygloo, m){
    m.doc() = "binding gloo from c to python"; // optional module docstring

    m.def("transport_tcp_available", &transport_tcp_available, "transport_tcp_available");

    m.def("transport_uv_available", &transport_uv_available, "transport_uv_available");


    m.def("allgather", &gloo::allgather, "allgather");
    m.def("allgatherv", &gloo::allgatherv, "allgatherv");

    m.def("allreduce", &gloo::allreduce, "allreduce");
    // declare_AllreduceRing<int>(m, "AllreduceRing");
    // declare_AllreduceRing<float>(m, "AllreduceRing");
    declare_AllreduceRing<double>(m, "AllreduceRing");

    pybind11::class_<gloo::AllreduceOptions>(m, "AllreduceOptions")
        .def(pybind11::init<const std::shared_ptr<gloo::Context>&>())
        .def("setAlgorithm", &gloo::AllreduceOptions::setAlgorithm)
        .def("setReduceFunction", &gloo::AllreduceOptions::setReduceFunction)
        .def("setTag", &gloo::AllreduceOptions::setTag)
        .def("setMaxSegmentSize", &gloo::AllreduceOptions::setMaxSegmentSize)
        .def("setTimeout", &gloo::AllreduceOptions::setTimeout);


    m.def("alltoall", &gloo::alltoall, "alltoall");
    m.def("alltoallv", &gloo::alltoallv, "alltoallv");
    m.def("barrier", &gloo::barrier, "barrier");
    m.def("broadcast", &gloo::broadcast, "broadcast");
    m.def("gather", &gloo::barrier, "gather");
    m.def("reduce", &gloo::barrier, "reduce");
    m.def("scatter", &gloo::barrier, "scatter");

    pybind11::class_<gloo::Context, std::shared_ptr<gloo::Context>>(m, "Context")
        .def(pybind11::init<int, int, int>(), pybind11::arg("rank")=nullptr, pybind11::arg("size")=nullptr, pybind11::arg("base")=2)
        // .def("getDevice", &gloo::Context::getDevice)
        .def_readonly("rank", &gloo::Context::rank)
        .def_readonly("size", &gloo::Context::size)
        .def_readwrite("base", &gloo::Context::base)
        // .def("getPair", &gloo::Context::getPair)
        .def("createUnboundBuffer", &gloo::Context::createUnboundBuffer)
        .def("nextSlot", &gloo::Context::nextSlot)
        .def("closeConnections", &gloo::Context::closeConnections)
        .def("setTimeout", &gloo::Context::setTimeout)
        .def("getTimeout", &gloo::Context::getTimeout);


    def_transport_module(m);
    def_rendezvous_module(m);

}

    // if (GLOO_USE_REDIS){
    //     #include <gloo/rendezvous/redis_store.h>
    //     pybind11::class_<gloo::rendezvous::RedisStore>(m, "RedisStore" )
    //         .def(pybind11::init<std::string, int>())
    //         .def("check", &gloo::rendezvous::RedisStore::check );
    // }