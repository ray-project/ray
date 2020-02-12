#include "fwd/boost/error.hpp"

namespace boost {

namespace posix_time {

class ptime;

}  // namespace posix_time

namespace chrono {

class steady_clock;

}  // namespace chrono

namespace asio {

template <typename Time, typename TimeTraits>
class basic_deadline_timer;
template<typename Clock, typename WaitTraits>
class basic_waitable_timer;
template <typename Protocol>
class basic_socket_acceptor;
template <typename Protocol>
class basic_stream_socket;
template <typename Time>
struct time_traits;
template <typename Clock>
struct wait_traits;

class const_buffer;

typedef basic_deadline_timer<posix_time::ptime, asio::time_traits<posix_time::ptime>>
    deadline_timer;
typedef basic_waitable_timer<chrono::steady_clock,
                             asio::wait_traits<chrono::steady_clock>>
    steady_timer;
class mutable_buffer;
class io_context;
typedef io_context io_service;

namespace ip {

class tcp;

}  // namespace ip

#ifdef _WIN32
#else
#define BOOST_ASIO_HAS_LOCAL_SOCKETS 1
namespace local {

class stream_protocol;

}  // namespace local
#endif

}  // namespace asio
}  // namespace boost
