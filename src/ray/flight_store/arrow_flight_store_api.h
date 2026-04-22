#pragma once

// Lightweight header for Cython bindings.
// Declares only the IPC-based string interface — no Arrow includes required.
// The implementation links against the full arrow_flight_store cc_library.

#include <cstdint>
#include <string>
#include <sys/types.h>

namespace ray {
namespace flight_store {

struct ObjectTransferInfo {
  std::string flight_uri;
  std::string key;
  pid_t pid;
  int64_t ipc_size;
};

class ArrowFlightStore {
 public:
  ArrowFlightStore();
  ~ArrowFlightStore();

  int StartServer();
  void StopServer();
  std::string GetUri() const;

  void PutFromIPC(const std::string &key, const std::string &ipc_bytes);
  ObjectTransferInfo PutFromIPCAndGetTransferInfo(const std::string &key,
                                                   const std::string &ipc_bytes);
  std::string GetLocalAsIPC(const std::string &key);
  std::string FetchAsIPC(const std::string &uri, const std::string &key);
  std::string FetchViaVMAsIPC(const std::string &flight_uri,
                               const std::string &key, int64_t ipc_size);
  void Delete(const std::string &key);
  size_t Size() const;
};

}  // namespace flight_store
}  // namespace ray
