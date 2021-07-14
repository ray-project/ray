#include "ray/common/asio_round_robin.h"
namespace boost {
namespace fibers {
namespace asio {

boost::asio::io_context::id round_robin::service::id;
}
}  // namespace fibers
}  // namespace boost
