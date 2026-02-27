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

#include "ray/common/pressure_memory_monitor.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <boost/thread/latch.hpp>
#include <fstream>
#include <memory>
#include <string>

#include "gtest/gtest.h"
#include "ray/common/cgroup2/cgroup_test_utils.h"
#include "ray/util/logging.h"

namespace ray {

class PressureMemoryMonitorTest : public ::testing::Test {
 protected:
  void SetUp() override {
    auto temp_dir_or = TempDirectory::Create();
    RAY_CHECK(temp_dir_or.ok()) << temp_dir_or.status().ToString();
    mock_cgroup_dir_ = std::move(temp_dir_or.value());
    pressure_file_ =
        std::make_unique<TempFile>(mock_cgroup_dir_->GetPath() + "/memory.pressure");
  }

  std::unique_ptr<TempDirectory> mock_cgroup_dir_;
  std::unique_ptr<TempFile> pressure_file_;
};

TEST_F(PressureMemoryMonitorTest, TestValidMemoryPsiReturnsTrue) {
  // Valid modes: some, full
  MemoryPsi psi_some = {.mode = "some", .stall_proportion = 0.5f, .stall_duration_s = 2};
  ASSERT_TRUE(psi_some.IsValidMode());
  ASSERT_TRUE(psi_some.IsValid());
  MemoryPsi psi_full = {.mode = "full", .stall_proportion = 0.5f, .stall_duration_s = 2};
  ASSERT_TRUE(psi_full.IsValidMode());
  ASSERT_TRUE(psi_full.IsValid());

  // Valid durations: multiples of 2, max 10
  MemoryPsi psi_variable_dur;
  psi_variable_dur.mode = "some";
  psi_variable_dur.stall_proportion = 0.5f;
  for (uint32_t duration : {2, 4, 6, 8, 10}) {
    psi_variable_dur.stall_duration_s = duration;
    ASSERT_TRUE(psi_variable_dur.IsValidStallDuration())
        << "Duration " << duration << " is a multiple of 2 andshould be valid";
    ASSERT_TRUE(psi_variable_dur.IsValid());
  }

  // Valid proportions: [0, 1]
  MemoryPsi psi_variable_prop;
  psi_variable_prop.mode = "some";
  psi_variable_prop.stall_duration_s = 2;
  for (float proportion : {0.1f, 0.5f, 1.0f}) {
    psi_variable_prop.stall_proportion = proportion;
    ASSERT_TRUE(psi_variable_prop.IsValidStallProportion())
        << "Proportion " << proportion << " should be valid";
    ASSERT_TRUE(psi_variable_prop.IsValid());
  }
}

TEST_F(PressureMemoryMonitorTest, TestInvalidModeReturnsFalse) {
  MemoryPsi psi = {.mode = "invalid", .stall_proportion = 0.5f, .stall_duration_s = 2};
  ASSERT_FALSE(psi.IsValidMode());
  ASSERT_FALSE(psi.IsValid());
  psi.mode = "";
  ASSERT_FALSE(psi.IsValidMode());
  ASSERT_FALSE(psi.IsValid());
}

TEST_F(PressureMemoryMonitorTest, TestInvalidStallDurationReturnsFalse) {
  MemoryPsi psi = {.mode = "some", .stall_proportion = 0.5f, .stall_duration_s = 3};
  ASSERT_FALSE(psi.IsValidStallDuration());
  ASSERT_FALSE(psi.IsValid());
  psi.stall_duration_s = 12;
  ASSERT_FALSE(psi.IsValidStallDuration());
  ASSERT_FALSE(psi.IsValid());
}

TEST_F(PressureMemoryMonitorTest, TestNonexistentCgroupPathFailsGracefully) {
  MemoryPsi psi = {.mode = "some", .stall_proportion = 0.5f, .stall_duration_s = 2};
  std::string nonexistent_path = "/nonexistent/cgroup/path";
  auto result = PressureMemoryMonitor::Create(
      psi, nonexistent_path, [](const SystemMemorySnapshot &) {});

  ASSERT_FALSE(result.ok());
  ASSERT_TRUE(result.status().IsIOError());
}

TEST_F(PressureMemoryMonitorTest, TestMonitorCreationWritesTriggerStringToFile) {
  MemoryPsi psi = {.mode = "some", .stall_proportion = 0.5f, .stall_duration_s = 2};

  auto result = PressureMemoryMonitor::Create(
      psi, mock_cgroup_dir_->GetPath(), [](const SystemMemorySnapshot &) {});
  ASSERT_TRUE(result.ok()) << "Failed to create PressureMemoryMonitor: "
                           << result.status().ToString();

  std::ifstream file(pressure_file_->GetPath());
  std::istreambuf_iterator<char> begin(file);
  std::istreambuf_iterator<char> end;
  std::string content(begin, end);
  file.close();

  std::string expected_trigger = "some 1000000 2000000";
  ASSERT_EQ(content, expected_trigger)
      << "Trigger string not correctly written to memory.pressure file";
}

TEST_F(PressureMemoryMonitorTest,
       TestMemoryMonitorCallsCallbackWhenPressureTriggerFires) {
  // When urgent (out-of-band) data is sent on a TCP socket, the receiving end
  // gets a POLLPRI event, this test uses this to trigger the pressure monitor.

  // Set up listener side socket
  int listener = socket(AF_INET, SOCK_STREAM, 0);
  struct sockaddr_in addr {};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  addr.sin_port = 0;  // Let the OS assign an available port

  ASSERT_EQ(bind(listener, reinterpret_cast<struct sockaddr *>(&addr), sizeof(addr)), 0)
      << "Failed to mock POLLPRI event due to: failure to bind on listener socket: "
      << strerror(errno);
  ASSERT_EQ(listen(listener, 1), 0)
      << "Failed to mock POLLPRI event due to: failure to listen on listener socket: "
      << strerror(errno);
  // Get the assigned port
  socklen_t addr_len = sizeof(addr);
  ASSERT_EQ(getsockname(listener, reinterpret_cast<struct sockaddr *>(&addr), &addr_len),
            0)
      << "Failed to mock POLLPRI event due to: failure to get socket name on listener "
         "socket: "
      << strerror(errno);

  // Set up client side socket
  int client = socket(AF_INET, SOCK_STREAM, 0);
  ASSERT_EQ(connect(client, reinterpret_cast<struct sockaddr *>(&addr), sizeof(addr)), 0)
      << "Failed to mock POLLPRI event due to: failure to connect client socket to "
         "listener socket: "
      << strerror(errno);

  // Accept connection on the listener side
  int listener_fd = accept(listener, nullptr, nullptr);
  close(listener);

  std::shared_ptr<boost::latch> has_called_once = std::make_shared<boost::latch>(1);
  auto callback = [has_called_once](const SystemMemorySnapshot &) {
    has_called_once->count_down();
  };
  std::unique_ptr<PressureMemoryMonitor> monitor =
      std::make_unique<PressureMemoryMonitor>(
          mock_cgroup_dir_->GetPath(), listener_fd, callback);

  // Send OOB (out-of-band) data to trigger POLLPRI on the listener side
  const char oob_data = '!';
  ssize_t sent = send(client, &oob_data, 1, MSG_OOB);
  ASSERT_EQ(sent, 1) << "Failed to mock POLLPRI event due to: failure to send OOB data "
                        "on client socket: "
                     << strerror(errno);

  has_called_once->wait();

  monitor.reset();
  close(client);
}

}  // namespace ray
