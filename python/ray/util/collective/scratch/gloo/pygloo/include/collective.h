#include <vector>
#include <pybind11/pybind11.h>
#include <pybind11/numpy.h>

#include <gloo/context.h>

namespace pygloo{

enum class ReduceOp: std::uint8_t {
    SUM = 0,
    PRODUCT,
    MIN,
    MAX,
    BAND, // Bitwise AND
    BOR, // Bitwise OR
    BXOR, // Bitwise XOR
    UNUSED,
};

void allreduce(const std::shared_ptr<gloo::Context> &context, pybind11::array_t<double, pybind11::array::c_style> &sendbuf,
        pybind11::array_t<double, pybind11::array::c_style> &recvbuf,
        size_t count, int datatype, ReduceOp reduceop = ReduceOp::SUM);

}// pygloo