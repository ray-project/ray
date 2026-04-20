#include "arrow_flight_store.h"

#include <arpa/inet.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <ifaddrs.h>
#include <netinet/in.h>
#include <sys/uio.h>
#include <unistd.h>

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

  arrow::flight::Location location;
  ARROW_CHECK_OK(arrow::flight::Location::ForGrpcTcp("0.0.0.0", 0, &location));

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

// Serialize a table to Arrow IPC stream format (contiguous bytes).
static arrow::Result<std::shared_ptr<arrow::Buffer>> SerializeTableToIPC(
    const std::shared_ptr<arrow::Table> &table) {
  ARROW_ASSIGN_OR_RAISE(auto sink, arrow::io::BufferOutputStream::Create());
  ARROW_ASSIGN_OR_RAISE(auto writer, arrow::ipc::MakeStreamWriter(sink, table->schema()));
  arrow::TableBatchReader reader(*table);
  std::shared_ptr<arrow::RecordBatch> batch;
  while (true) {
    ARROW_RETURN_NOT_OK(reader.ReadNext(&batch));
    if (!batch) break;
    ARROW_RETURN_NOT_OK(writer->WriteRecordBatch(*batch));
  }
  ARROW_RETURN_NOT_OK(writer->Close());
  return sink->Finish();
}

void ArrowFlightStore::Put(const std::string &key, std::shared_ptr<arrow::Table> table) {
  // Pre-serialize to IPC for the process_vm_readv path.
  auto ipc_result = SerializeTableToIPC(table);
  std::lock_guard<std::mutex> lock(mu_);
  tables_[key] = std::move(table);
  if (ipc_result.ok()) {
    ipc_buffers_[key] = ipc_result.MoveValueUnsafe();
  }
}

ObjectTransferInfo ArrowFlightStore::PutAndGetTransferInfo(
    const std::string &key, std::shared_ptr<arrow::Table> table) {
  Put(key, table);
  ObjectTransferInfo info;
  info.flight_uri = GetUri();
  info.key = key;
  info.pid = getpid();
  std::lock_guard<std::mutex> lock(mu_);
  auto it = ipc_buffers_.find(key);
  if (it != ipc_buffers_.end()) {
    info.ipc_address = reinterpret_cast<uintptr_t>(it->second->data());
    info.ipc_size = it->second->size();
  } else {
    info.ipc_address = 0;
    info.ipc_size = 0;
  }
  return info;
}

arrow::Result<std::shared_ptr<arrow::Table>> ArrowFlightStore::FetchViaVM(
    pid_t remote_pid, uintptr_t remote_address, int64_t size) {
  // Allocate local buffer.
  ARROW_ASSIGN_OR_RAISE(auto local_buf, arrow::AllocateResizableBuffer(size));

  // Single process_vm_readv syscall — no gRPC, no GIL, no kernel buffering.
  struct iovec local_iov;
  local_iov.iov_base = local_buf->mutable_data();
  local_iov.iov_len = static_cast<size_t>(size);

  struct iovec remote_iov;
  remote_iov.iov_base = reinterpret_cast<void *>(remote_address);
  remote_iov.iov_len = static_cast<size_t>(size);

  ssize_t nread = process_vm_readv(remote_pid, &local_iov, 1, &remote_iov, 1, 0);
  if (nread < 0) {
    return arrow::Status::IOError("process_vm_readv failed: errno=",
                                  errno,
                                  " (hint: check /proc/sys/kernel/yama/ptrace_scope)");
  }
  if (nread != size) {
    return arrow::Status::IOError(
        "process_vm_readv partial read: ", nread, " of ", size, " bytes");
  }

  // Deserialize IPC stream → Arrow table.
  arrow::io::BufferReader reader(local_buf);
  ARROW_ASSIGN_OR_RAISE(auto stream_reader,
                        arrow::ipc::RecordBatchStreamReader::Open(&reader));
  return stream_reader->ToTable();
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
  ipc_buffers_.erase(key);
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

void ArrowFlightStore::Delete(const std::string &key) {
  std::lock_guard<std::mutex> lock(mu_);
  tables_.erase(key);
  ipc_buffers_.erase(key);
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

  arrow::flight::Location location;
  auto status = arrow::flight::Location::Parse(uri, &location);
  if (!status.ok()) return nullptr;

  std::unique_ptr<arrow::flight::FlightClient> client;
  status = arrow::flight::FlightClient::Connect(location, &client);
  if (!status.ok()) return nullptr;

  auto *raw = client.get();
  clients_[uri] = std::move(client);
  return raw;
}

}  // namespace flight_store
}  // namespace ray
