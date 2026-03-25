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

#include "ray/common/event_memory_monitor.h"

#include <atomic>
#include <boost/chrono.hpp>
#include <boost/thread/latch.hpp>
#include <fstream>
#include <memory>
#include <string>

#include "gtest/gtest.h"
#include "ray/common/cgroup2/cgroup_test_utils.h"
#include "ray/util/logging.h"

namespace ray {

class EventMemoryMonitorTest : public ::testing::Test {
 protected:
  void SetUp() override {
    StatusOr<std::unique_ptr<TempDirectory>> temp_dir_or = TempDirectory::Create();
    RAY_CHECK(temp_dir_or.ok()) << temp_dir_or.status().ToString();
    mock_cgroup_dir_ = std::move(temp_dir_or.value());
    events_file_ =
        std::make_unique<TempFile>(mock_cgroup_dir_->GetPath() + "/memory.events");
  }

  /**
   * @brief Mock the memory events file with the given low and high values.
   * @param path The path to the memory events file.
   * @param low The low value to write to the memory events file.
   * @param high The high value to write to the memory events file.
   */
  void WriteMemoryEventsFile(const std::string &path, int64_t low, int64_t high) {
    std::ofstream events_file(path, std::ios::trunc);
    events_file << "low " << low << "\n";
    events_file << "high " << high << "\n";
    events_file << "max 0\n";
    events_file << "oom 0\n";
    events_file << "oom_kill 0\n";
    events_file << "oom_group_kill 0\n";
    events_file.flush();
    events_file.close();
  }

  std::unique_ptr<TempDirectory> mock_cgroup_dir_;
  std::unique_ptr<TempFile> events_file_;
};

TEST_F(EventMemoryMonitorTest, TestNonexistentCgroupPathFailsGracefully) {
  std::string nonexistent_path = "/nonexistent/cgroup/path";
  StatusSetOr<std::unique_ptr<EventMemoryMonitor>, StatusT::IOError> result =
      EventMemoryMonitor::Create(std::move(nonexistent_path),
                                 [](SystemMemorySnapshot) {});

  ASSERT_TRUE(result.has_error())
      << "Failed to catch invalid cgroup path when creating EventMemoryMonitor";
  ASSERT_TRUE(std::holds_alternative<StatusT::IOError>(result.error()));
}

TEST_F(EventMemoryMonitorTest, TestMissingMemoryEventsFileFailsGracefully) {
  StatusOr<std::unique_ptr<TempDirectory>> empty_dir_or = TempDirectory::Create();
  RAY_CHECK(empty_dir_or.ok()) << empty_dir_or.status().ToString();
  std::unique_ptr<TempDirectory> empty_dir = std::move(empty_dir_or.value());
  StatusSetOr<std::unique_ptr<EventMemoryMonitor>, StatusT::IOError> result =
      EventMemoryMonitor::Create(empty_dir->GetPath(), [](SystemMemorySnapshot) {});

  ASSERT_TRUE(result.has_error())
      << "Failed to catch invalid cgroup configuration when creating EventMemoryMonitor";
  ASSERT_TRUE(std::holds_alternative<StatusT::IOError>(result.error()));
}

TEST_F(EventMemoryMonitorTest, TestSuccessfulCreationWithValidPath) {
  StatusSetOr<std::unique_ptr<EventMemoryMonitor>, StatusT::IOError> result =
      EventMemoryMonitor::Create(mock_cgroup_dir_->GetPath(),
                                 [](SystemMemorySnapshot) {});
  ASSERT_TRUE(result.has_value())
      << "Failed to create EventMemoryMonitor: " << result.message();
  std::unique_ptr<EventMemoryMonitor> monitor = std::move(result.value());
  ASSERT_NE(monitor, nullptr);
}

TEST_F(EventMemoryMonitorTest, TestCallbackCalledWhenHighEventChanges) {
  WriteMemoryEventsFile(events_file_->GetPath(), 0, 0);

  auto callback_latch = std::make_shared<boost::latch>(1);
  KillWorkersCallback callback = [callback_latch](SystemMemorySnapshot) {
    callback_latch->count_down();
  };

  StatusSetOr<std::unique_ptr<EventMemoryMonitor>, StatusT::IOError> result =
      EventMemoryMonitor::Create(std::move(mock_cgroup_dir_->GetPath()),
                                 std::move(callback));
  ASSERT_TRUE(result.has_value())
      << "Failed to create EventMemoryMonitor with valid ctor parameters: "
      << result.message();

  WriteMemoryEventsFile(events_file_->GetPath(), 0, 1);
  callback_latch->wait();
}

TEST_F(EventMemoryMonitorTest, TestNoCallbackWhenValuesUnchanged) {
  WriteMemoryEventsFile(events_file_->GetPath(), 0, 0);

  auto callback_latch = std::make_shared<boost::latch>(1);
  KillWorkersCallback callback = [callback_latch](SystemMemorySnapshot) {
    callback_latch->count_down();
  };

  StatusSetOr<std::unique_ptr<EventMemoryMonitor>, StatusT::IOError> result =
      EventMemoryMonitor::Create(mock_cgroup_dir_->GetPath(), callback);
  ASSERT_TRUE(result.has_value())
      << "Failed to create EventMemoryMonitor: " << result.message();

  WriteMemoryEventsFile(events_file_->GetPath(), 0, 0);
  auto status = callback_latch->wait_for(boost::chrono::milliseconds(1000));
  EXPECT_EQ(status, boost::cv_status::timeout)
      << "Callback should not be called when values haven't changed";
}

TEST_F(EventMemoryMonitorTest, TestNoCallbackWhenIrrelevantEventChanges) {
  WriteMemoryEventsFile(events_file_->GetPath(), 0, 0);

  auto callback_latch = std::make_shared<boost::latch>(1);
  KillWorkersCallback callback = [callback_latch](SystemMemorySnapshot) {
    callback_latch->count_down();
  };

  StatusSetOr<std::unique_ptr<EventMemoryMonitor>, StatusT::IOError> result =
      EventMemoryMonitor::Create(mock_cgroup_dir_->GetPath(), callback);
  ASSERT_TRUE(result.has_value())
      << "Failed to create EventMemoryMonitor: " << result.message();

  WriteMemoryEventsFile(events_file_->GetPath(), 2, 0);
  auto status = callback_latch->wait_for(boost::chrono::milliseconds(1000));
  EXPECT_EQ(status, boost::cv_status::timeout)
      << "Callback should not be called when irrelevant event value changes";
}

TEST_F(EventMemoryMonitorTest, TestMultipleCallbacksOnMultipleChanges) {
  WriteMemoryEventsFile(events_file_->GetPath(), 0, 0);

  auto latch1 = std::make_shared<boost::latch>(1);
  auto latch2 = std::make_shared<boost::latch>(1);
  auto latch3 = std::make_shared<boost::latch>(1);
  std::atomic<int> callback_count{0};
  KillWorkersCallback callback =
      [&callback_count, latch1, latch2, latch3](SystemMemorySnapshot) {
        int count = ++callback_count;
        if (count == 1) {
          latch1->count_down();
        } else if (count == 2) {
          latch2->count_down();
        } else if (count == 3) {
          latch3->count_down();
        }
      };

  StatusSetOr<std::unique_ptr<EventMemoryMonitor>, StatusT::IOError> result =
      EventMemoryMonitor::Create(mock_cgroup_dir_->GetPath(), callback);
  ASSERT_TRUE(result.has_value())
      << "Failed to create EventMemoryMonitor: " << result.message();
  std::unique_ptr<EventMemoryMonitor> monitor = std::move(result.value());

  WriteMemoryEventsFile(events_file_->GetPath(), 0, 1);
  latch1->wait();
  monitor->Enable();
  EXPECT_EQ(callback_count.load(), 1);

  WriteMemoryEventsFile(events_file_->GetPath(), 0, 2);
  latch2->wait();
  monitor->Enable();
  EXPECT_EQ(callback_count.load(), 2);

  WriteMemoryEventsFile(events_file_->GetPath(), 0, 3);
  latch3->wait();
  monitor->Enable();
  EXPECT_EQ(callback_count.load(), 3);
}

}  // namespace ray
