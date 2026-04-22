#include "arrow_flight_store.h"

#include <arpa/inet.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/util/logging.h>
#include <ifaddrs.h>
#include <netinet/in.h>
#include <unistd.h>
#ifdef __linux__
#include <sys/uio.h>
#endif

#include <sstream>

namespace ray {
namespace flight_store {

// ---------------------------------------------------------------------------
// Helper: get the node's IP address (first non-loopback IPv4).
// ---------------------------------------------------------------------------
static std::string GetLocalIP() {
  struct ifaddrs *ifaddr = nullptr;
  if (getifaddrs(&ifaddr) == -1) {
    return "127.0.0.1";
  }
  std::string result = "127.0.0.1";
  for (auto *ifa = ifaddr; ifa != nullptr; ifa = ifa->ifa_next) {
    if (ifa->ifa_addr == nullptr || ifa->ifa_addr->sa_family != AF_INET) continue;
    char buf[INET_ADDRSTRLEN];
    inet_ntop(
        AF_INET, &((struct sockaddr_in *)ifa->ifa_addr)->sin_addr, buf, sizeof(buf));
    std::string ip(buf);
    if (ip != "127.0.0.1") {
      result = ip;
      break;
    }
  }
  freeifaddrs(ifaddr);
  return result;
}

// ---------------------------------------------------------------------------
// Inner Flight server implementation.
// ---------------------------------------------------------------------------
class ArrowFlightStore::FlightServerImpl : public arrow::flight::FlightServerBase {
 public:
  explicit FlightServerImpl(ArrowFlightStore *store) : store_(store) {}

  arrow::Status DoGet(const arrow::flight::ServerCallContext & /*context*/,
                      const arrow::flight::Ticket &ticket,
                      std::unique_ptr<arrow::flight::FlightDataStream> *stream) override {
    // Pop the table (move semantics — auto-delete after serving).
    auto table = store_->PopTable(ticket.ticket);
    if (!table) {
      return arrow::Status::KeyError("Object not found: ", ticket.ticket);
    }
    auto batch_reader = std::make_shared<arrow::TableBatchReader>(*table);
    *stream = std::make_unique<arrow::flight::RecordBatchStream>(batch_reader);
    return arrow::Status::OK();
  }

  arrow::Status DoAction(
      const arrow::flight::ServerCallContext & /*context*/,
      const arrow::flight::Action &action,
      std::unique_ptr<arrow::flight::ResultStream> *result_stream) override {
    if (action.type == "write_vm") {
      // Body format: key_len(4 bytes) + key + pid(4) + address(8) + size(8)
      const uint8_t *p = action.body->data();
      uint32_t key_len;
      memcpy(&key_len, p, 4);
      p += 4;
      std::string key(reinterpret_cast<const char *>(p), key_len);
      p += key_len;
      int32_t pid;
      memcpy(&pid, p, 4);
      p += 4;
      uint64_t addr;
      memcpy(&addr, p, 8);
      p += 8;
      int64_t size;
      memcpy(&size, p, 8);

      ARROW_RETURN_NOT_OK(store_->WriteToRemoteVM(
          key, static_cast<pid_t>(pid), static_cast<uintptr_t>(addr), size));

      // Return empty result to signal success.
      std::vector<arrow::flight::Result> results;
      *result_stream =
          std::make_unique<arrow::flight::SimpleResultStream>(std::move(results));
      return arrow::Status::OK();
    }
    return arrow::Status::NotImplemented("Unknown action: ", action.type);
  }

 private:
  ArrowFlightStore *store_;
};

// ---------------------------------------------------------------------------
// ArrowFlightStore implementation.
// ---------------------------------------------------------------------------

ArrowFlightStore::ArrowFlightStore() : ip_(GetLocalIP()) {}

ArrowFlightStore::~ArrowFlightStore() { StopServer(); }

int ArrowFlightStore::StartServer() {
  server_ = std::make_unique<FlightServerImpl>(this);

  auto location = arrow::flight::Location::ForGrpcTcp("0.0.0.0", 0).ValueOrDie();

  arrow::flight::FlightServerOptions options(location);
  ARROW_CHECK_OK(server_->Init(options));
  port_ = server_->port();

  // Run Serve() in a background thread.
  server_thread_ =
      std::make_unique<std::thread>([this]() { ARROW_CHECK_OK(server_->Serve()); });

  return port_;
}

void ArrowFlightStore::StopServer() {
  if (server_) {
    ARROW_CHECK_OK(server_->Shutdown());
    if (server_thread_ && server_thread_->joinable()) {
      server_thread_->join();
    }
    server_.reset();
    server_thread_.reset();
  }
}

std::string ArrowFlightStore::GetUri() const {
  std::ostringstream oss;
  oss << "grpc://" << ip_ << ":" << port_;
  return oss.str();
}

// Estimate the IPC stream size for a table without actually serializing.
// Walks the schema + buffer sizes. Cheap — no data copies.
static int64_t EstimateIPCStreamSize(const std::shared_ptr<arrow::Table> &table) {
  int64_t size = 0;
  // Schema message overhead (approximate).
  size += 1024;
  // For each column, sum the buffer sizes.
  for (int i = 0; i < table->num_columns(); ++i) {
    auto chunked = table->column(i);
    for (int j = 0; j < chunked->num_chunks(); ++j) {
      auto array_data = chunked->chunk(j)->data();
      for (const auto &buf : array_data->buffers) {
        if (buf) {
          // IPC aligns buffers to 8 bytes.
          size += (buf->size() + 7) & ~7;
        }
      }
    }
  }
  // IPC record batch metadata + EOS marker overhead.
  size += 256 * table->num_columns() + 64;
  // Add 20% headroom for IPC framing, flatbuffer metadata, padding.
  size = static_cast<int64_t>(size * 1.2);
  return size;
}

#ifdef __linux__
// Serialize a table to IPC stream format into a pre-allocated buffer.
// Returns the actual number of bytes written. Used by WriteToRemoteVM.
static arrow::Result<int64_t> SerializeTableToIPCBuffer(
    const std::shared_ptr<arrow::Table> &table, uint8_t *dest, int64_t capacity) {
  auto sink = std::make_shared<arrow::io::FixedSizeBufferWriter>(
      std::make_shared<arrow::MutableBuffer>(dest, capacity));
  ARROW_ASSIGN_OR_RAISE(auto writer, arrow::ipc::MakeStreamWriter(sink, table->schema()));
  arrow::TableBatchReader reader(*table);
  std::shared_ptr<arrow::RecordBatch> batch;
  while (true) {
    ARROW_RETURN_NOT_OK(reader.ReadNext(&batch));
    if (!batch) break;
    ARROW_RETURN_NOT_OK(writer->WriteRecordBatch(*batch));
  }
  ARROW_RETURN_NOT_OK(writer->Close());
  ARROW_ASSIGN_OR_RAISE(auto pos, sink->Tell());
  return pos;
}
#endif  // __linux__

void ArrowFlightStore::Put(const std::string &key, std::shared_ptr<arrow::Table> table) {
  int64_t estimated_size = EstimateIPCStreamSize(table);
  std::lock_guard<std::mutex> lock(mu_);
  tables_[key] = std::move(table);
  ipc_sizes_[key] = estimated_size;
}

ObjectTransferInfo ArrowFlightStore::PutAndGetTransferInfo(
    const std::string &key, std::shared_ptr<arrow::Table> table) {
  Put(key, table);
  ObjectTransferInfo info;
  info.flight_uri = GetUri();
  info.key = key;
  info.pid = getpid();
  std::lock_guard<std::mutex> lock(mu_);
  info.ipc_size = ipc_sizes_[key];
  return info;
}

arrow::Status ArrowFlightStore::WriteToRemoteVM(const std::string &key,
                                                pid_t remote_pid,
                                                uintptr_t remote_address,
                                                int64_t remote_size) {
#ifdef __linux__
  // Get the table.
  std::shared_ptr<arrow::Table> table;
  {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = tables_.find(key);
    if (it == tables_.end()) {
      return arrow::Status::KeyError("Object not found: ", key);
    }
    table = it->second;
  }

  // Serialize IPC into a local buffer first.
  ARROW_ASSIGN_OR_RAISE(auto local_buf, arrow::AllocateResizableBuffer(remote_size));
  ARROW_ASSIGN_OR_RAISE(
      auto actual_size,
      SerializeTableToIPCBuffer(table, local_buf->mutable_data(), remote_size));

  // Write directly into the consumer's buffer via process_vm_writev.
  struct iovec local_iov;
  local_iov.iov_base = local_buf->mutable_data();
  local_iov.iov_len = static_cast<size_t>(actual_size);

  struct iovec remote_iov;
  remote_iov.iov_base = reinterpret_cast<void *>(remote_address);
  remote_iov.iov_len = static_cast<size_t>(actual_size);

  ssize_t nwritten = process_vm_writev(remote_pid, &local_iov, 1, &remote_iov, 1, 0);
  if (nwritten < 0) {
    return arrow::Status::IOError("process_vm_writev failed: errno=", errno);
  }
  if (nwritten != actual_size) {
    return arrow::Status::IOError(
        "process_vm_writev partial write: ", nwritten, " of ", actual_size);
  }
  return arrow::Status::OK();
#else
  return arrow::Status::NotImplemented("process_vm_writev is Linux-only");
#endif
}

arrow::Result<std::shared_ptr<arrow::Table>> ArrowFlightStore::DeserializeIPCBuffer(
    const std::shared_ptr<arrow::Buffer> &buffer) {
  arrow::io::BufferReader reader(buffer);
  ARROW_ASSIGN_OR_RAISE(auto stream_reader,
                        arrow::ipc::RecordBatchStreamReader::Open(&reader));
  return stream_reader->ToTable();
}

arrow::Result<std::shared_ptr<arrow::Table>> ArrowFlightStore::FetchViaVM(
    const std::string &flight_uri, const std::string &key, int64_t ipc_size) {
  // 1. Allocate a local buffer for the producer to write into.
  ARROW_ASSIGN_OR_RAISE(auto local_buf, arrow::AllocateResizableBuffer(ipc_size));

  // 2. Build the DoAction body: key_len(4) + key + pid(4) + address(8) + size(8)
  pid_t my_pid = getpid();
  uintptr_t buf_addr = reinterpret_cast<uintptr_t>(local_buf->mutable_data());
  uint32_t key_len = static_cast<uint32_t>(key.size());
  int64_t body_size = 4 + key.size() + 4 + 8 + 8;
  ARROW_ASSIGN_OR_RAISE(auto body_buf, arrow::AllocateBuffer(body_size));
  uint8_t *p = body_buf->mutable_data();
  memcpy(p, &key_len, 4);
  p += 4;
  memcpy(p, key.data(), key.size());
  p += key.size();
  int32_t pid32 = static_cast<int32_t>(my_pid);
  memcpy(p, &pid32, 4);
  p += 4;
  uint64_t addr64 = static_cast<uint64_t>(buf_addr);
  memcpy(p, &addr64, 8);
  p += 8;
  memcpy(p, &ipc_size, 8);

  // 3. Send DoAction to the producer's Flight server.
  auto *client = GetOrCreateClient(flight_uri);
  if (!client) {
    return arrow::Status::IOError("Failed to connect to ", flight_uri);
  }
  arrow::flight::Action action;
  action.type = "write_vm";
  action.body = std::move(body_buf);
  ARROW_ASSIGN_OR_RAISE(auto results, client->DoAction(action));
  // Drain the result stream.
  while (true) {
    ARROW_ASSIGN_OR_RAISE(auto result, results->Next());
    if (!result) break;
  }

  // 4. Producer has written IPC data into our buffer. Deserialize.
  return DeserializeIPCBuffer(std::shared_ptr<arrow::Buffer>(std::move(local_buf)));
}

std::shared_ptr<arrow::Table> ArrowFlightStore::GetLocal(const std::string &key) {
  std::lock_guard<std::mutex> lock(mu_);
  auto it = tables_.find(key);
  if (it == tables_.end()) return nullptr;
  return it->second;
}

std::shared_ptr<arrow::Table> ArrowFlightStore::PopTable(const std::string &key) {
  std::lock_guard<std::mutex> lock(mu_);
  auto it = tables_.find(key);
  if (it == tables_.end()) return nullptr;
  auto table = std::move(it->second);
  tables_.erase(it);
  ipc_sizes_.erase(key);
  return table;
}

arrow::Result<std::shared_ptr<arrow::Table>> ArrowFlightStore::Fetch(
    const std::string &uri, const std::string &key) {
  auto *client = GetOrCreateClient(uri);
  if (!client) {
    return arrow::Status::IOError("Failed to connect to ", uri);
  }

  arrow::flight::Ticket ticket;
  ticket.ticket = key;

  ARROW_ASSIGN_OR_RAISE(auto stream, client->DoGet(ticket));
  ARROW_ASSIGN_OR_RAISE(auto table, stream->ToTable());
  return table;
}

std::shared_ptr<arrow::Table> ArrowFlightStore::FetchOrThrow(const std::string &uri,
                                                             const std::string &key) {
  auto result = Fetch(uri, key);
  if (!result.ok()) {
    throw std::runtime_error("Flight fetch failed: " + result.status().ToString());
  }
  return result.MoveValueUnsafe();
}

std::shared_ptr<arrow::Table> ArrowFlightStore::FetchViaVMOrThrow(
    const std::string &flight_uri, const std::string &key, int64_t ipc_size) {
  auto result = FetchViaVM(flight_uri, key, ipc_size);
  if (!result.ok()) {
    throw std::runtime_error("FetchViaVM failed: " + result.status().ToString());
  }
  return result.MoveValueUnsafe();
}

// ---------------------------------------------------------------------------
// IPC-based methods for Cython (avoids shared_ptr<Table> template issues).
// ---------------------------------------------------------------------------

static std::shared_ptr<arrow::Table> DeserializeIPC(const std::string &ipc_bytes) {
  auto buf = arrow::Buffer::FromString(ipc_bytes);
  arrow::io::BufferReader reader(buf);
  auto stream_reader = arrow::ipc::RecordBatchStreamReader::Open(&reader).ValueOrDie();
  return stream_reader->ToTable().ValueOrDie();
}

static std::string SerializeIPC(const std::shared_ptr<arrow::Table> &table) {
  auto sink = arrow::io::BufferOutputStream::Create().ValueOrDie();
  auto writer = arrow::ipc::MakeStreamWriter(sink, table->schema()).ValueOrDie();
  arrow::TableBatchReader reader(*table);
  std::shared_ptr<arrow::RecordBatch> batch;
  while (true) {
    ARROW_CHECK_OK(reader.ReadNext(&batch));
    if (!batch) break;
    ARROW_CHECK_OK(writer->WriteRecordBatch(*batch));
  }
  ARROW_CHECK_OK(writer->Close());
  auto buf = sink->Finish().ValueOrDie();
  return std::string(reinterpret_cast<const char *>(buf->data()), buf->size());
}

void ArrowFlightStore::PutFromIPC(const std::string &key, const std::string &ipc_bytes) {
  auto table = DeserializeIPC(ipc_bytes);
  Put(key, table);
}

ObjectTransferInfo ArrowFlightStore::PutFromIPCAndGetTransferInfo(
    const std::string &key, const std::string &ipc_bytes) {
  auto table = DeserializeIPC(ipc_bytes);
  return PutAndGetTransferInfo(key, table);
}

std::string ArrowFlightStore::GetLocalAsIPC(const std::string &key) {
  auto table = GetLocal(key);
  if (!table) return "";
  return SerializeIPC(table);
}

std::string ArrowFlightStore::FetchAsIPC(const std::string &uri, const std::string &key) {
  auto table = FetchOrThrow(uri, key);
  return SerializeIPC(table);
}

std::string ArrowFlightStore::FetchViaVMAsIPC(const std::string &flight_uri,
                                              const std::string &key,
                                              int64_t ipc_size) {
  auto table = FetchViaVMOrThrow(flight_uri, key, ipc_size);
  return SerializeIPC(table);
}

void ArrowFlightStore::Delete(const std::string &key) {
  std::lock_guard<std::mutex> lock(mu_);
  tables_.erase(key);
  ipc_sizes_.erase(key);
}

size_t ArrowFlightStore::Size() const {
  std::lock_guard<std::mutex> lock(mu_);
  return tables_.size();
}

arrow::flight::FlightClient *ArrowFlightStore::GetOrCreateClient(const std::string &uri) {
  std::lock_guard<std::mutex> lock(client_mu_);
  auto it = clients_.find(uri);
  if (it != clients_.end()) {
    return it->second.get();
  }

  auto location_result = arrow::flight::Location::Parse(uri);
  if (!location_result.ok()) return nullptr;

  auto client_result = arrow::flight::FlightClient::Connect(*location_result);
  if (!client_result.ok()) return nullptr;
  auto client = std::move(*client_result);

  auto *raw = client.get();
  clients_[uri] = std::move(client);
  return raw;
}

}  // namespace flight_store
}  // namespace ray
