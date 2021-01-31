#include <pybind11/pybind11.h>
#include <transport.h>

namespace pygloo {
namespace rendezvous {

void def_rendezvous_module(pybind11::module &m);
} // namespace rendezvous
} // namespace pygloo