// Copyright 2017 The Ray Authors.
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

#include <signal.h>

#include <cstdlib>
#include <iostream>

#include "gtest/gtest.h"
#include "ray/util/logging.h"
#include "ray/util/util.h"

// This test just print some call stack information.
namespace ray {
#ifndef _WIN32

void Sleep() { std::this_thread::sleep_for(std::chrono::milliseconds(100)); }

void TestSendSignal(const std::string &test_name, int signal) {
  pid_t pid;
  pid = fork();
  ASSERT_TRUE(pid >= 0);
  if (pid == 0) {
    while (true) {
      int n = 1000;
      while (n--)
        ;
    }
  } else {
    Sleep();
    RAY_LOG(ERROR) << test_name << ": kill pid " << pid
                   << " with return value=" << kill(pid, signal);
    Sleep();
  }
}

TEST(SignalTest, SendTermSignalTest) { TestSendSignal("SendTermSignalTest", SIGTERM); }

TEST(SignalTest, SendBusSignalTest) { TestSendSignal("SendBusSignalTest", SIGBUS); }

TEST(SignalTest, SIGABRT_Test) {
  pid_t pid;
  pid = fork();
  ASSERT_TRUE(pid >= 0);
  if (pid == 0) {
    // This code will cause SIGABRT sent.
    std::abort();
  } else {
    Sleep();
    RAY_LOG(ERROR) << "SIGABRT_Test: kill pid " << pid
                   << " with return value=" << kill(pid, SIGKILL);
    Sleep();
  }
}

TEST(SignalTest, SIGSEGV_Test) {
  pid_t pid;
  pid = fork();
  ASSERT_TRUE(pid >= 0);
  if (pid == 0) {
    int *pointer = reinterpret_cast<int *>(0x1237896);
    *pointer = 100;
  } else {
    Sleep();
    RAY_LOG(ERROR) << "SIGSEGV_Test: kill pid " << pid
                   << " with return value=" << kill(pid, SIGKILL);
    Sleep();
  }
}

TEST(SignalTest, SIGILL_Test) {
  pid_t pid;
  pid = fork();
  ASSERT_TRUE(pid >= 0);
  if (pid == 0) {
    raise(SIGILL);
  } else {
    Sleep();
    RAY_LOG(ERROR) << "SIGILL_Test: kill pid " << pid
                   << " with return value=" << kill(pid, SIGKILL);
    Sleep();
  }
}

#endif  // !_WIN32
}  // namespace ray

int main(int argc, char **argv) {
  InitShutdownRAII ray_log_shutdown_raii(ray::RayLog::StartRayLog,
                                         ray::RayLog::ShutDownRayLog,
                                         argv[0],
                                         ray::RayLogLevel::INFO,
                                         /*log_dir=*/"");
  ray::RayLog::InstallFailureSignalHandler(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);
  int failed = RUN_ALL_TESTS();
  return failed;
}
