// Stub implementation when Arrow C++ is not available.
// All methods throw at runtime. The real implementation is in arrow_flight_store.cc
// which is only compiled when @pyarrow provides Arrow headers.

#include "ray/flight_store/arrow_flight_store_api.h"
#include <stdexcept>

namespace ray {
namespace flight_store {

static void no_arrow() {
  throw std::runtime_error(
      "Arrow Flight store not available: Ray was built without Arrow C++ support. "
      "Install pyarrow and rebuild Ray.");
}

ArrowFlightStore::ArrowFlightStore() {}
ArrowFlightStore::~ArrowFlightStore() {}
int ArrowFlightStore::StartServer() { no_arrow(); return 0; }
void ArrowFlightStore::StopServer() {}
std::string ArrowFlightStore::GetUri() const { no_arrow(); return ""; }
void ArrowFlightStore::PutFromIPC(const std::string &, const std::string &) { no_arrow(); }
ObjectTransferInfo ArrowFlightStore::PutFromIPCAndGetTransferInfo(
    const std::string &, const std::string &) { no_arrow(); return {}; }
std::string ArrowFlightStore::GetLocalAsIPC(const std::string &) { no_arrow(); return ""; }
std::string ArrowFlightStore::FetchAsIPC(const std::string &, const std::string &) { no_arrow(); return ""; }
std::string ArrowFlightStore::FetchViaVMAsIPC(
    const std::string &, const std::string &, int64_t) { no_arrow(); return ""; }
void ArrowFlightStore::Delete(const std::string &) {}
size_t ArrowFlightStore::Size() const { return 0; }

}  // namespace flight_store
}  // namespace ray
