#pragma once

#include <memory>
#include <string>

#include "ray/common/status.h"
#include "ray/gcs/gcs_server/raft_types.h"
#include "ray/protobuf/raft.pb.h"

namespace ray {
namespace gcs {

class RaftStorage {
 public:
  virtual ~RaftStorage() = default;

  virtual Status Initialize() = 0;
  virtual Status Terminate() = 0;

  virtual Status SaveCurrentTerm(int64_t term) = 0;
  virtual Status LoadCurrentTerm(int64_t *term) = 0;

  virtual Status SaveVotedFor(const std::string &voted_for) = 0;
  virtual Status LoadVotedFor(std::string *voted_for) = 0;

  virtual Status SaveLogEntry(const LogEntry &entry) = 0;
  virtual Status LoadLogEntries(std::vector<LogEntry> *entries) = 0;
  virtual Status DeleteLogEntriesFrom(int64_t start_index) = 0;

  virtual Status SaveSnapshot(const Snapshot &snapshot) = 0;
  virtual Status LoadSnapshot(Snapshot *snapshot) = 0;
};

class RedisRaftStorage : public RaftStorage {
 public:
  explicit RedisRaftStorage(const std::string &redis_address);
  ~RedisRaftStorage() override = default;

  Status Initialize() override;
  Status Terminate() override;

  Status SaveCurrentTerm(int64_t term) override;
  Status LoadCurrentTerm(int64_t *term) override;

  Status SaveVotedFor(const std::string &voted_for) override;
  Status LoadVotedFor(std::string *voted_for) override;

  Status SaveLogEntry(const LogEntry &entry) override;
  Status LoadLogEntries(std::vector<LogEntry> *entries) override;
  Status DeleteLogEntriesFrom(int64_t start_index) override;

  Status SaveSnapshot(const Snapshot &snapshot) override;
  Status LoadSnapshot(Snapshot *snapshot) override;

 private:
  std::string redis_address_;
  bool is_initialized_;
};

}  // namespace gcs
}  // namespace ray 