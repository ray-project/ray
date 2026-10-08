// Copyright 2026 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "ray/raylet/gpu_memory_isolator.h"

#include <csignal>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "gtest/gtest.h"

namespace ray {
namespace raylet {
namespace {

class FakeGpuCgroupManager : public CgroupManagerInterface {
 public:
  Status AddProcessToWorkersCgroup(const std::string &) override { return Status::OK(); }
  Status AddProcessToSystemCgroup(const std::string &) override { return Status::OK(); }

  StatusOr<std::string> AddProcessToGpuWorkerCgroup(const std::string &name,
                                                    const std::string &pid) override {
    if (!add_status_.ok()) {
      return add_status_;
    }
    added_.emplace_back(name, pid);
    return "/cgroup/" + name;
  }

  Status DeleteGpuWorkerCgroup(const std::string &name) override {
    deleted_.push_back(name);
    return Status::OK();
  }

  std::string GetSystemCgroupPath() const override { return ""; }
  std::string GetUserCgroupPath() const override { return ""; }
  StatusOr<std::string> GetSystemCgroupConstraintValue(
      const std::string &) const override {
    return std::string{};
  }
  StatusOr<std::string> GetUserCgroupConstraintValue(const std::string &) const override {
    return std::string{};
  }

  Status add_status_ = Status::OK();
  std::vector<std::pair<std::string, std::string>> added_;
  std::vector<std::string> deleted_;
};

class GpuMemoryIsolatorTest : public ::testing::Test {
 protected:
  GpuMemoryIsolatorTest()
      : isolator_(io_service_,
                  cgroup_manager_,
                  {"sudo", "-n", "nvidia-smi"},
                  {"3", "5"},
                  [this](const std::vector<std::string> &args) {
                    commands_.push_back(args);
                    return command_status_;
                  }) {}

  Status Isolate(int64_t gpu_memory_bytes) {
    std::optional<Status> result;
    isolator_.Isolate(
        worker_id_, lease_id_, 123, 1, gpu_memory_bytes, [&result](Status status) {
          result = status;
        });
    while (!result.has_value()) {
      io_service_.run_one();
    }
    return *result;
  }

  void RunUntilDeleted(size_t count) {
    while (cgroup_manager_.deleted_.size() < count) {
      io_service_.run_one();
    }
  }

  instrumented_io_context io_service_;
  boost::asio::executor_work_guard<boost::asio::io_context::executor_type> work_guard_ =
      boost::asio::make_work_guard(io_service_);
  FakeGpuCgroupManager cgroup_manager_;
  std::vector<std::vector<std::string>> commands_;
  Status command_status_ = Status::OK();
  WorkerID worker_id_ = WorkerID::FromRandom();
  LeaseID lease_id_ = LeaseID::FromRandom();
  GpuMemoryIsolator isolator_;
};

TEST_F(GpuMemoryIsolatorTest, IsolateSetsHardAndSoftLimitOnTheWorkerCgroup) {
  ASSERT_TRUE(Isolate(20LL * 1024 * 1024 * 1024).ok());

  ASSERT_EQ(cgroup_manager_.added_.size(), 1);
  EXPECT_EQ(cgroup_manager_.added_[0].first, lease_id_.Hex());
  EXPECT_EQ(cgroup_manager_.added_[0].second, "123");
  std::vector<std::string> expected = {"sudo",
                                       "-n",
                                       "nvidia-smi",
                                       "memory-limits",
                                       "--set",
                                       "--namespace",
                                       "/cgroup/" + lease_id_.Hex(),
                                       "--soft-limit",
                                       "20480",
                                       "--hard-limit",
                                       "20480",
                                       "-i",
                                       "5"};
  ASSERT_EQ(commands_.size(), 1);
  EXPECT_EQ(commands_[0], expected);
}

TEST_F(GpuMemoryIsolatorTest, IsolateRoundsUpToWholeMebibytes) {
  ASSERT_TRUE(Isolate(1024 * 1024 + 1).ok());

  ASSERT_EQ(commands_.size(), 1);
  EXPECT_EQ(commands_[0][8], "2");
  EXPECT_EQ(commands_[0][10], "2");
}

TEST_F(GpuMemoryIsolatorTest, IsolateFailsWithoutRunningTheCommandIfCgroupFails) {
  cgroup_manager_.add_status_ = Status::Invalid("no cgroups");

  Status status = Isolate(1024);

  EXPECT_TRUE(status.IsInvalid()) << status;
  EXPECT_TRUE(commands_.empty());
}

TEST_F(GpuMemoryIsolatorTest, IsolateFailsIfTheCommandFails) {
  command_status_ = Status::IOError("not permitted");

  Status status = Isolate(1024);

  EXPECT_TRUE(status.IsIOError()) << status;
}

TEST_F(GpuMemoryIsolatorTest, ReleaseResetsTheLimitThenDeletesTheCgroup) {
  ASSERT_TRUE(Isolate(1024).ok());

  isolator_.Release(worker_id_);
  RunUntilDeleted(1);

  ASSERT_EQ(commands_.size(), 2);
  EXPECT_EQ(commands_[1][8], "0");
  EXPECT_EQ(commands_[1][10], "max");
  EXPECT_EQ(cgroup_manager_.deleted_, std::vector<std::string>{lease_id_.Hex()});
}

TEST_F(GpuMemoryIsolatorTest, ReleaseDoesNothingForAWorkerThatWasNotIsolated) {
  isolator_.Release(WorkerID::FromRandom());
  io_service_.poll();

  EXPECT_TRUE(commands_.empty());
  EXPECT_TRUE(cgroup_manager_.deleted_.empty());
}

#ifndef _WIN32
class RunCommandTest : public ::testing::Test {
 protected:
  // The raylet ignores SIGCHLD.
  void SetUp() override { previous_ = signal(SIGCHLD, SIG_IGN); }
  void TearDown() override { signal(SIGCHLD, previous_); }

  void (*previous_)(int) = nullptr;
};

TEST_F(RunCommandTest, ReturnsOkWhenTheCommandSucceeds) {
  EXPECT_TRUE(RunCommand({"true"}).ok());
}

TEST_F(RunCommandTest, ReturnsTheExitCodeAndOutputWhenTheCommandFails) {
  Status status = RunCommand({"sh", "-c", "echo not permitted; exit 3"});

  ASSERT_TRUE(status.IsIOError()) << status;
  EXPECT_NE(status.message().find("exited with code 3"), std::string::npos) << status;
  EXPECT_NE(status.message().find("not permitted"), std::string::npos) << status;
}

TEST_F(RunCommandTest, FailsWhenTheCommandDoesNotExist) {
  EXPECT_FALSE(RunCommand({"/nonexistent/command"}).ok());
}
#endif

}  // namespace
}  // namespace raylet
}  // namespace ray
