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

#include "ray/raylet/worker.h"

#include <gtest/gtest.h>
#include <sched.h>
#include <signal.h>
#include <sys/mount.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>

#include <chrono>
#include <cstdint>
#include <fstream>
#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "ray/asio/instrumented_io_context.h"
#include "ray/common/id.h"
#include "ray/common/ray_config.h"
#include "ray/raylet_ipc_client/client_connection.h"
#include "ray/rpc/client_call.h"
#include "ray/util/clock.h"
#include "ray/util/process.h"

namespace ray {
namespace raylet {
namespace {

constexpr int kPass = 0;
constexpr int kFail = 1;
constexpr int kSkip = 77;

// Raylet-side state needed to construct a Worker whose connection is one end of a
// socketpair. The worker process holds the other end until it exits.
class WorkerHarness {
 public:
  WorkerHarness()
      : client_call_manager_(io_service_, /*record_stats=*/false, /*local_address=*/"") {}

  std::shared_ptr<Worker> MakeWorker(int raylet_fd) {
    local_stream_socket socket(io_service_);
    socket.assign(local_stream_protocol(AF_UNIX, 0), raylet_fd);
    auto connection = ClientConnection::Create(
        [](std::shared_ptr<ClientConnection>, int64_t, const std::vector<uint8_t> &) {},
        [](std::shared_ptr<ClientConnection>, const boost::system::error_code &) {},
        std::move(socket),
        "worker",
        {});
    return std::make_shared<Worker>(JobID::FromInt(1),
                                    /*runtime_env_hash=*/0,
                                    WorkerID::FromRandom(),
                                    rpc::Language::PYTHON,
                                    rpc::WorkerType::WORKER,
                                    "127.0.0.1",
                                    connection,
                                    client_call_manager_,
                                    clock_);
  }

  instrumented_io_context &io_service() { return io_service_; }

 private:
  instrumented_io_context io_service_;
  rpc::ClientCallManager client_call_manager_;
  Clock clock_;
};

// Forks a fake worker that keeps `worker_fd` open and waits until signaled.
pid_t StartFakeWorker(int worker_fd, int raylet_fd, bool ignore_sigterm) {
  // Set the disposition before fork() so that a SIGTERM sent right after fork() returns
  // cannot reach the child before it is ignored.
  auto previous = signal(SIGTERM, ignore_sigterm ? SIG_IGN : SIG_DFL);
  pid_t pid = fork();
  if (pid == 0) {
    close(raylet_fd);
    while (true) {
      pause();
    }
  }
  signal(SIGTERM, previous);
  close(worker_fd);
  return pid;
}

// Forks a process that is unrelated to the raylet; it must never be signaled.
pid_t StartBystander() {
  pid_t pid = fork();
  if (pid == 0) {
    while (true) {
      pause();
    }
  }
  return pid;
}

bool WriteFile(const std::string &path, const std::string &content) {
  std::ofstream file(path);
  file << content;
  return static_cast<bool>(file);
}

// Makes the next process created in this PID namespace get `pid`.
bool ReservePid(pid_t pid) {
  return WriteFile("/proc/sys/kernel/ns_last_pid", std::to_string(pid - 1));
}

// Runs `scenario` as PID 1 of a new unprivileged user + PID + mount namespace, where
// /proc/sys/kernel/ns_last_pid can be written to reproduce PID reuse deterministically.
// Returns kSkip if the environment does not allow unprivileged user namespaces.
int RunInPidNamespace(const std::function<int()> &scenario) {
  pid_t helper = fork();
  if (helper == 0) {
    const uid_t uid = getuid();
    const gid_t gid = getgid();
    if (unshare(CLONE_NEWUSER | CLONE_NEWPID | CLONE_NEWNS) != 0 ||
        !WriteFile("/proc/self/setgroups", "deny") ||
        !WriteFile("/proc/self/uid_map", "0 " + std::to_string(uid) + " 1") ||
        !WriteFile("/proc/self/gid_map", "0 " + std::to_string(gid) + " 1")) {
      _exit(kSkip);
    }
    pid_t init = fork();
    if (init == 0) {
      // Containers often mount /proc/sys read-only; a /proc owned by the new namespaces
      // makes ns_last_pid writable. Failure is fine: ReservePid() then reports kSkip.
      if (mount(nullptr, "/", nullptr, MS_REC | MS_PRIVATE, nullptr) == 0) {
        mount("proc", "/proc", "proc", MS_NOSUID | MS_NODEV | MS_NOEXEC, nullptr);
      }
      _exit(scenario());
    }
    int status = 0;
    waitpid(init, &status, 0);
    _exit(WIFEXITED(status) ? WEXITSTATUS(status) : kFail);
  }
  int status = 0;
  waitpid(helper, &status, 0);
  return WIFEXITED(status) ? WEXITSTATUS(status) : kFail;
}

// Terminates the bystander `pid` (a child of the caller) with SIGTERM and returns whether
// it had already been sent SIGKILL. A pending SIGKILL takes precedence over SIGTERM, so
// this does not depend on whether the earlier SIGKILL has finished taking effect.
bool WasSigkilled(pid_t pid) {
  kill(pid, SIGTERM);
  int status = 0;
  return waitpid(pid, &status, 0) == pid && WIFSIGNALED(status) &&
         WTERMSIG(status) == SIGKILL;
}

class WorkerKillTest : public ::testing::Test {
 protected:
  void SetUp() override {
    RayConfig::instance().initialize(R"({"kill_worker_timeout_milliseconds": 100})");
  }
};

TEST_F(WorkerKillTest, ForceKillSignalsLiveWorker) {
  WorkerHarness harness;
  int fds[2];
  ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
  pid_t pid = StartFakeWorker(fds[1], fds[0], /*ignore_sigterm=*/false);
  auto worker = harness.MakeWorker(fds[0]);
  worker->SetProcess(std::make_unique<Process>(pid));

  worker->KillAsync(harness.io_service(), /*force=*/true);

  int status = 0;
  ASSERT_EQ(waitpid(pid, &status, 0), pid);
  ASSERT_TRUE(WIFSIGNALED(status));
  EXPECT_EQ(WTERMSIG(status), SIGKILL);
}

TEST_F(WorkerKillTest, GracefulKillEscalatesToSigkillForLiveWorker) {
  WorkerHarness harness;
  int fds[2];
  ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
  pid_t pid = StartFakeWorker(fds[1], fds[0], /*ignore_sigterm=*/true);
  auto worker = harness.MakeWorker(fds[0]);
  worker->SetProcess(std::make_unique<Process>(pid));

  worker->KillAsync(harness.io_service(), /*force=*/false);
  harness.io_service().run_for(std::chrono::seconds(5));

  int status = 0;
  ASSERT_EQ(waitpid(pid, &status, 0), pid);
  ASSERT_TRUE(WIFSIGNALED(status));
  EXPECT_EQ(WTERMSIG(status), SIGKILL);
}

// Regression test for https://github.com/ray-project/ray/issues/68462: the worker exits
// after SIGTERM, and its PID is reused before the delayed SIGKILL fires.
TEST_F(WorkerKillTest, DelayedKillDoesNotSignalProcessThatReusedPid) {
  int result = RunInPidNamespace([] {
    WorkerHarness harness;
    int fds[2];
    if (socketpair(AF_UNIX, SOCK_STREAM, 0, fds) != 0) {
      return kFail;
    }
    pid_t pid = StartFakeWorker(fds[1], fds[0], /*ignore_sigterm=*/false);
    auto worker = harness.MakeWorker(fds[0]);
    worker->SetProcess(std::make_unique<Process>(pid));

    worker->KillAsync(harness.io_service(), /*force=*/false);
    int status = 0;
    waitpid(pid, &status, 0);
    if (!ReservePid(pid)) {
      return kSkip;
    }
    pid_t bystander = StartBystander();
    if (bystander != pid) {
      kill(bystander, SIGKILL);
      return kSkip;
    }
    harness.io_service().run_for(std::chrono::seconds(1));

    return WasSigkilled(bystander) ? kFail : kPass;
  });
  if (result == kSkip) {
    GTEST_SKIP() << "Unprivileged user and PID namespaces are not available.";
  }
  EXPECT_EQ(result, kPass) << "The raylet signaled the process that reused the PID";
}

// The worker exits and its PID is reused before the raylet processes its registration.
TEST_F(WorkerKillTest, KillDoesNotSignalProcessThatReusedPidBeforeRegistration) {
  int result = RunInPidNamespace([] {
    WorkerHarness harness;
    int fds[2];
    if (socketpair(AF_UNIX, SOCK_STREAM, 0, fds) != 0) {
      return kFail;
    }
    pid_t pid = fork();
    if (pid == 0) {
      _exit(0);  // Exiting closes the worker's end of the raylet connection.
    }
    close(fds[1]);
    int status = 0;
    waitpid(pid, &status, 0);
    if (!ReservePid(pid)) {
      return kSkip;
    }
    pid_t bystander = StartBystander();
    if (bystander != pid) {
      kill(bystander, SIGKILL);
      return kSkip;
    }

    auto worker = harness.MakeWorker(fds[0]);
    worker->SetProcess(std::make_unique<Process>(pid));
    worker->KillAsync(harness.io_service(), /*force=*/true);

    return WasSigkilled(bystander) ? kFail : kPass;
  });
  if (result == kSkip) {
    GTEST_SKIP() << "Unprivileged user and PID namespaces are not available.";
  }
  EXPECT_EQ(result, kPass) << "The raylet signaled the process that reused the PID";
}

}  // namespace
}  // namespace raylet
}  // namespace ray
