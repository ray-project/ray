// Copyright 2025 The Ray Authors.
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

#include "ray/util/process_cleaner.h"

#if defined(__APPLE__) || defined(__linux__)
#include <unistd.h>

#include <array>
#include <atomic>
#include <cstring>

#include "ray/util/invoke_once_token.h"
#include "ray/util/logging.h"
#endif

namespace ray {

#if defined(__APPLE__) || defined(__linux__)
void SpawnSubprocessAndCleanup(std::function<void()> cleanup) {
  static InvokeOnceToken token;
  token.CheckInvokeOnce();

  std::array<int, 2> pipe_fd;  // Intentionally no initialization.
  RAY_CHECK_NE(pipe(pipe_fd.data()), -1)
      << "Fails to create pipe because " << strerror(errno);
  pid_t pid = fork();
  RAY_CHECK_NE(pid, -1) << "Fails to create child process because " << strerror(errno);

  const int pipe_readfd = pipe_fd[0];
  const int pipe_writefd = pipe_fd[1];

  // Handle parent process.
  if (pid > 0) {
    RAY_CHECK_NE(close(pipe_readfd), -1)
        << "Parent process fails to close pipe read because " << strerror(errno);
    // Intentionally leave the pipe write fd leak, which gets closed at parent process
    // termination.
    return;
  }

  // Close write fd.
  RAY_CHECK_NE(close(pipe_writefd), -1)
      << "Child process fails to close pipe write because " << strerror(errno);
  // Handle child process, make it owner for new session so it doesn't killed along with
  // its parent process.
  setsid();

  // Block until parent process exits.
  char buf = 'a';
  [[maybe_unused]] auto x = read(pipe_readfd, &buf, /*count=*/1);
  cleanup();
}
#else
void SpawnSubprocessAndCleanup(std::function<void()> cleanup) {}
#endif

}  // namespace ray
