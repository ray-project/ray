#include <signal.h>
#include <cstdlib>
#include <iostream>

#include "gtest/gtest.h"
#include "ray/util/logging.h"
#include "ray/util/signal_handler.h"

// This test just print some call stack information.
namespace ray {

void Sleep() { usleep(100000); }

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

TEST(SignalTest, SendTermSignal_Unset_Test) {
  // SignalHandlers will be automatically uninstalled when it is out of scope.
  auto installed = ray::SignalHandlers("util_test", false);
  // This should not print call stack message.
  TestSendSignal("SendTermSignal_Unset_Test", SIGTERM);
}

TEST(SignalTest, SendTermSignalTest) {
  auto installed = ray::SignalHandlers("util_test", true);
  TestSendSignal("SendTermSignalTest", SIGTERM);
}

TEST(SignalTest, SendIntSignalTest) {
  auto installed = ray::SignalHandlers("util_test", false);
  TestSendSignal("SendIntSignalTest", SIGINT);
}

TEST(SignalTest, SIGSEGV_Test) {
  auto installed = ray::SignalHandlers("util_test", true);
  pid_t pid;
  pid = fork();
  ASSERT_TRUE(pid >= 0);
  if (pid == 0) {
    int *pointer = (int *)0x1237896;
    *pointer = 100;
  } else {
    Sleep();
    RAY_LOG(ERROR) << "SIGSEGV_Test: kill pid " << pid
                   << " with return value=" << kill(pid, SIGKILL);
    Sleep();
  }
}

TEST(SignalTest, SIGILL_Test) {
  auto installed = ray::SignalHandlers("util_test", false);
  pid_t pid;
  pid = fork();
  ASSERT_TRUE(pid >= 0);
  if (pid == 0) {
    // This code will cause SIGILL sent.
    asm("ud2");
  } else {
    Sleep();
    RAY_LOG(ERROR) << "SIGILL_Test: kill pid " << pid
                   << " with return value=" << kill(pid, SIGKILL);
    Sleep();
  }
}

}  // namespace ray

int main(int argc, char **argv) {
  ray::RayLog::StartRayLog("", RAY_DEBUG);
  ::testing::InitGoogleTest(&argc, argv);
  int failed = RUN_ALL_TESTS();
  ray::RayLog::ShutDownRayLog();
  return failed;
}
