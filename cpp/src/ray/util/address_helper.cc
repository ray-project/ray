#include <dlfcn.h>
#include <stdint.h>

namespace ray {
namespace api {

uintptr_t dynamic_library_base_addr;

extern "C" void GenerateBaseAddressOfCurrentLibrary() {
  Dl_info info;
  dladdr((void *)GenerateBaseAddressOfCurrentLibrary, &info);
  dynamic_library_base_addr = (uintptr_t)info.dli_fbase;
  return;
}
}  // namespace api
}  // namespace ray