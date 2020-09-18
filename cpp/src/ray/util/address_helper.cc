#include <dlfcn.h>
#include <stdint.h>

namespace ray {
namespace api {

uintptr_t GetBaseAddressOfLibraryFromAddr(void *addr) {
  Dl_info info;
  dladdr(addr, &info);
  return (uintptr_t)info.dli_fbase;
}

uintptr_t dynamic_library_base_addr =
    GetBaseAddressOfLibraryFromAddr((void *)GetBaseAddressOfLibraryFromAddr);
}  // namespace api
}  // namespace ray