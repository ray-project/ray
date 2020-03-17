#include <dlfcn.h>
#include <stdint.h>

namespace ray {
namespace api {

uintptr_t dylib_base_addr;

extern "C" void AddressHelperInit() {
  Dl_info info;
  dladdr((void *)AddressHelperInit, &info);
  dylib_base_addr = (uintptr_t)info.dli_fbase;
  return;
}
}  // namespace api
}  // namespace ray