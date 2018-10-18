#include <signal.h>
#include <cstdlib>
#include <iostream>

#include "gtest/gtest.h"
#include "ray/util/logging.h"
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
  InitShutdownRAII ray_log_shutdown_raii(ray::RayLog::StartRayLog,
                                         ray::RayLog::ShutDownRayLog, argv[0],
                                         ray::RayLogLevel::INFO,
                                         /*log_dir=*/"");
  ray::RayLog::InstallFailureSignalHandler();
  ::testing::InitGoogleTest(&argc, argv);
  int failed = RUN_ALL_TESTS();
  return failed;
}
