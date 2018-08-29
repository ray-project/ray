#include <signal.h>
#include <cstdlib>
#include <iostream>

#include "gtest/gtest.h"
#include "ray/util/logging.h"
#include "ray/util/signal_handler.h"
#include "ray/util/util.h"

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
  DefaultInitShutdown signal_handler_uninstall_wrapper(
      SignalHandlers::InstallSignalHandler, SignalHandlers::UninstallSignalHandler,
      "SendTermSignal_Unset_Test", false);
  // This should not print call stack message.
  TestSendSignal("SendTermSignal_Unset_Test", SIGTERM);
}

TEST(SignalTest, SendTermSignalTest) {
  DefaultInitShutdown signal_handler_uninstall_wrapper(
      SignalHandlers::InstallSignalHandler, SignalHandlers::UninstallSignalHandler,
      "SendTermSignalTest", true);
  TestSendSignal("SendTermSignalTest", SIGTERM);
}

TEST(SignalTest, SendIntSignalTest) {
  DefaultInitShutdown signal_handler_uninstall_wrapper(
      SignalHandlers::InstallSignalHandler, SignalHandlers::UninstallSignalHandler,
      "SendIntSignalTest", true);
  TestSendSignal("SendIntSignalTest", SIGINT);
}

TEST(SignalTest, SIGSEGV_Test) {
  DefaultInitShutdown signal_handler_uninstall_wrapper(
      SignalHandlers::InstallSignalHandler, SignalHandlers::UninstallSignalHandler,
      "SIGSEGV_Test", true);
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
  DefaultInitShutdown signal_handler_uninstall_wrapper(
      SignalHandlers::InstallSignalHandler, SignalHandlers::UninstallSignalHandler,
      "SIGILL_Test", false);
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
  DefaultInitShutdown ray_log_shutdown_wrapper(
      ray::RayLog::StartRayLog, ray::RayLog::ShutDownRayLog, argv[0], RAY_DEBUG, "");
  ::testing::InitGoogleTest(&argc, argv);
  int failed = RUN_ALL_TESTS();
  return failed;
}
