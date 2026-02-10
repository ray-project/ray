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

#include "ray/util/getenv_trace.h"

#include <cstdio>
#include <cstdlib>
#include <cstring>

namespace ray {

namespace {

constexpr int kMaxEnvNameLogLen = 256;

void LogGetEnvCall(const char *name, const char *file, int line) {
  auto write_one = [&](FILE *fp) {
    std::fprintf(fp, "[RAY_GETENV] %s:%d name=", file, line);
    if (name == nullptr) {
      std::fprintf(fp, "(null)");
    } else {
      for (int i = 0; i < kMaxEnvNameLogLen && name[i] != '\0'; ++i) {
        std::fputc(static_cast<unsigned char>(name[i]), fp);
      }
    }
    std::fprintf(fp, "\n");
    std::fflush(fp);
  };
  write_one(stderr);
  write_one(stdout);
}

}  // namespace

const char *RayGetEnv(const char *name, const char *file, int line) {
  LogGetEnvCall(name, file, line);
  if (name == nullptr) {
    return nullptr;
  }
  return ::getenv(name);
}

}  // namespace ray
