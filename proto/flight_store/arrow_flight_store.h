#pragma once

#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>

#include <arrow/api.h>
#include <arrow/flight/api.h>
#include <arrow/ipc/api.h>

namespace ray {
namespace flight_store {

/// Info returned by Put for cross-process VM transfer on same node.
struct ObjectTransferInfo {
  std::string flight_uri;
  std::string key;
  pid_t pid;
  uintptr_t ipc_address;
  int64_t ipc_size;
};

/// A Flight-based object store that keeps Arrow tables in the application heap
/// and serves them via Arrow Flight RPC.
///
/// - Put stores tables locally (zero-copy from PyArrow).
/// - DoGet serves tables to remote consumers (pure C++, no GIL).
/// - Fetch retrieves tables from remote stores (pure C++, no GIL).
/// - Tables are auto-deleted after first read (move semantics).
class ArrowFlightStore {
 public:
  ArrowFlightStore();
  ~ArrowFlightStore();

  /// Start the Flight server on a random port.
  /// \return The port the server is listening on.
  int StartServer();

  /// Stop the Flight server.
  void StopServer();

  /// Get the Flight URI (grpc://ip:port).
  std::string GetUri() const;

  /// Store a table by key. Also pre-serializes to IPC bytes for the
  /// process_vm_readv same-node path. Thread-safe.
  void Put(const std::string &key, std::shared_ptr<arrow::Table> table);

  /// Store a table and return transfer info (pid, IPC buffer address/size)
  /// that a same-node consumer can use with FetchViaVM().
  ObjectTransferInfo PutAndGetTransferInfo(const std::string &key,
                                           std::shared_ptr<arrow::Table> table);

  /// Get a locally stored table (does NOT auto-delete).
  /// Returns nullptr if not found.
  std::shared_ptr<arrow::Table> GetLocal(const std::string &key);

  /// Fetch a table from a remote Flight server. Pure C++, no GIL needed.
  /// The remote store auto-deletes the table after serving.
  arrow::Result<std::shared_ptr<arrow::Table>> Fetch(const std::string &uri,
                                                      const std::string &key);

  /// Fetch a table from a same-node producer using process_vm_readv.
  /// Reads the pre-serialized IPC bytes directly from the producer's heap.
  /// Pure C++, no GIL, no gRPC — single syscall.
  /// After fetching, the consumer should notify the producer to delete
  /// (via Flight or out-of-band).
  static arrow::Result<std::shared_ptr<arrow::Table>> FetchViaVM(
      pid_t remote_pid, uintptr_t remote_address, int64_t size);

  /// Delete a stored table (and its IPC buffer).
  void Delete(const std::string &key);

  /// Number of stored tables.
  size_t Size() const;

 private:
  class FlightServerImpl;

  /// Look up and remove a table (used by DoGet for move semantics).
  std::shared_ptr<arrow::Table> PopTable(const std::string &key);

  mutable std::mutex mu_;
  std::unordered_map<std::string, std::shared_ptr<arrow::Table>> tables_;
  /// Pre-serialized IPC buffers for same-node process_vm_readv transfers.
  std::unordered_map<std::string, std::shared_ptr<arrow::Buffer>> ipc_buffers_;

  std::unique_ptr<FlightServerImpl> server_;
  std::unique_ptr<std::thread> server_thread_;
  int port_ = 0;
  std::string ip_;

  /// Cached Flight clients keyed by URI.
  std::mutex client_mu_;
  std::unordered_map<std::string, std::unique_ptr<arrow::flight::FlightClient>> clients_;

  arrow::flight::FlightClient *GetOrCreateClient(const std::string &uri);
};

}  // namespace flight_store
}  // namespace ray
