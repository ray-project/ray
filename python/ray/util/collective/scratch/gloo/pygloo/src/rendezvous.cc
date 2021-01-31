#include <gloo/config.h>

#include <gloo/rendezvous/context.h>
#include <gloo/rendezvous/file_store.h>
#include <gloo/rendezvous/hash_store.h>
#include <gloo/rendezvous/prefix_store.h>

#if GLOO_USE_REDIS
#include <gloo/rendezvous/redis_store.h>
#endif

#include <rendezvous.h>

namespace pygloo {
namespace rendezvous{
void def_rendezvous_module(pybind11::module &m) {
  pybind11::module rendezvous =
      m.def_submodule("rendezvous", "This is a rendezvous module");
  // pybind11::class_<gloo::rendezvous::Context, gloo::Context>(rendezvous,
  // "Context")
  pybind11::class_<gloo::rendezvous::Context, gloo::Context,
                   std::shared_ptr<gloo::rendezvous::Context>>(rendezvous,
                                                               "Context")
      .def(pybind11::init<int, int, int>(), pybind11::arg("rank") = nullptr,
           pybind11::arg("size") = nullptr, pybind11::arg("base") = 2)
      .def("connectFullMesh", &gloo::rendezvous::Context::connectFullMesh);

  pybind11::class_<gloo::rendezvous::Store,
                   std::shared_ptr<gloo::rendezvous::Store>>(rendezvous,
                                                             "Store")
      .def("set", &gloo::rendezvous::Store::set)
      .def("get", &gloo::rendezvous::Store::get);

  pybind11::class_<gloo::rendezvous::FileStore, gloo::rendezvous::Store,
                   std::shared_ptr<gloo::rendezvous::FileStore>>(rendezvous,
                                                                 "FileStore")
      .def(pybind11::init<const std::string &>())
      .def("set", &gloo::rendezvous::FileStore::set)
      .def("get", &gloo::rendezvous::FileStore::get);

  pybind11::class_<gloo::rendezvous::HashStore, gloo::rendezvous::Store,
                   std::shared_ptr<gloo::rendezvous::HashStore>>(rendezvous,
                                                                 "HashStore")
      .def("set", &gloo::rendezvous::HashStore::set)
      .def("get", &gloo::rendezvous::HashStore::get);

  pybind11::class_<gloo::rendezvous::PrefixStore, gloo::rendezvous::Store,
                   std::shared_ptr<gloo::rendezvous::PrefixStore>>(
      rendezvous, "PrefixStore")
      .def(pybind11::init<const std::string &, gloo::rendezvous::Store &>())
      .def("set", &gloo::rendezvous::PrefixStore::set)
      .def("get", &gloo::rendezvous::PrefixStore::get);

#if GLOO_USE_REDIS
  pybind11::class_<gloo::rendezvous::RedisStore, gloo::rendezvous::Store,
                   std::shared_ptr<gloo::rendezvous::RedisStore>>(rendezvous,
                                                                  "RedisStore")
      .def(pybind11::init<const std::string &, int>())
      .def("set", &gloo::rendezvous::RedisStore::set)
      .def("get", &gloo::rendezvous::RedisStore::get);
#endif
}
}
} // namespace pygloo