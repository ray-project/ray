#pragma once

#include <arrow/api.h>
#include <arrow/flight/api.h>
#include <arrow/ipc/api.h>

#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>

namespace ray {
namespace flight_store {

/// Info returned by Put for cross-process transfer.
struct ObjectTransferInfo {
  std::string flight_uri;
  std::string key;
  pid_t pid;
  int64_t ipc_size;  // Estimated IPC stream size for VM transfers.
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

  /// Serialize a stored table to IPC and write it directly into a remote
  /// process's buffer via process_vm_writev. Called on the PRODUCER side
  /// (e.g., from a Flight DoAction handler). The consumer allocates the
  /// buffer and passes its address + PID.
  arrow::Status WriteToRemoteVM(const std::string &key,
                                pid_t remote_pid,
                                uintptr_t remote_address,
                                int64_t remote_size);

  /// Deserialize an IPC buffer that was written into our address space
  /// by a remote producer via process_vm_writev. Called on the CONSUMER side
  /// after the producer has completed the write.
  static arrow::Result<std::shared_ptr<arrow::Table>> DeserializeIPCBuffer(
      const std::shared_ptr<arrow::Buffer> &buffer);

  /// Consumer-side: fetch a table from a same-node producer via process_vm_writev.
  /// Allocates a local buffer, sends a DoAction("write_vm") to the producer's
  /// Flight server, producer serializes IPC and writes directly into our buffer,
  /// then we deserialize locally.
  /// One small Flight RPC (to trigger the write), bulk data via process_vm_writev.
  arrow::Result<std::shared_ptr<arrow::Table>> FetchViaVM(const std::string &flight_uri,
                                                          const std::string &key,
                                                          int64_t ipc_size);

  /// Throwing wrappers for Cython (avoids arrow::Result template issues).
  std::shared_ptr<arrow::Table> FetchOrThrow(const std::string &uri,
                                              const std::string &key);
  std::shared_ptr<arrow::Table> FetchViaVMOrThrow(const std::string &flight_uri,
                                                   const std::string &key,
                                                   int64_t ipc_size);

  /// IPC-based methods for Cython (pass IPC bytes as std::string to avoid
  /// shared_ptr<Table> template issues across the Cython boundary).
  void PutFromIPC(const std::string &key, const std::string &ipc_bytes);
  ObjectTransferInfo PutFromIPCAndGetTransferInfo(const std::string &key,
                                                   const std::string &ipc_bytes);
  std::string GetLocalAsIPC(const std::string &key);
  std::string FetchAsIPC(const std::string &uri, const std::string &key);
  std::string FetchViaVMAsIPC(const std::string &flight_uri,
                               const std::string &key, int64_t ipc_size);

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
  /// Estimated IPC stream sizes (computed cheaply on Put, no serialization).
  std::unordered_map<std::string, int64_t> ipc_sizes_;

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
