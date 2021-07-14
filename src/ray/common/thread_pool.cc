#include "ray/common/thread_pool.h"

namespace ray {
namespace thread_pool {

CPUThreadPool _cpu_pool(4);
IOThreadPool _io_pool;

}
}
