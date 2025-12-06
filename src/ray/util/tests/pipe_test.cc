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

#include "ray/util/pipe.h"

#include <gtest/gtest.h>

#include <string>

#include "absl/strings/str_cat.h"
#include "ray/util/process.h"

namespace ray {

TEST(PipeTest, ParentReadChildWrite) {
  Pipe pipe;
  intptr_t writer_handle = pipe.MakeWriterHandle();

  std::string python_code = absl::StrCat(
      "import os; "
      "os.write(",
      writer_handle,
      ", b'hello from child'); "
      "os.close(",
      writer_handle,
      ")");

  auto [proc, ec] = Process::Spawn({"python", "-c", python_code}, /*decouple=*/false);
  ASSERT_FALSE(ec) << ec.message();

  pipe.CloseWriterHandle();

  auto result = pipe.Read(/*timeout_s=*/5);
  ASSERT_TRUE(result.ok()) << result.status().message();
  EXPECT_EQ(*result, "hello from child");

  int exit_code = proc.Wait();
  EXPECT_EQ(exit_code, 0);
}

TEST(PipeTest, SiblingProcessesCommunicate) {
  Pipe pipe;

  // Spawn writer first
  intptr_t writer_handle = pipe.MakeWriterHandle();
  std::string writer_code = absl::StrCat(
      "import os; "
      "os.write(",
      writer_handle,
      ", b'hello from sibling'); "
      "os.close(",
      writer_handle,
      ")");

  auto [writer_proc, writer_ec] =
      Process::Spawn({"python", "-c", writer_code}, /*decouple=*/false);
  ASSERT_FALSE(writer_ec) << writer_ec.message();
  pipe.CloseWriterHandle();

  // Then spawn reader
  intptr_t reader_handle = pipe.MakeReaderHandle();
  std::string reader_code = absl::StrCat(
      "import os, sys; "
      "data = os.read(",
      reader_handle,
      ", 100); "
      "os.close(",
      reader_handle,
      "); "
      "sys.exit(0 if data == b'hello from sibling' else 1)");

  auto [reader_proc, reader_ec] =
      Process::Spawn({"python", "-c", reader_code}, /*decouple=*/false);
  ASSERT_FALSE(reader_ec) << reader_ec.message();
  pipe.CloseReaderHandle();

  int writer_exit = writer_proc.Wait();
  EXPECT_EQ(writer_exit, 0);

  int reader_exit = reader_proc.Wait();
  EXPECT_EQ(reader_exit, 0);
}

TEST(PipeTest, ReadTimeout) {
  Pipe pipe;
  auto result = pipe.Read(/*timeout_s=*/0.1);
  EXPECT_FALSE(result.ok());
}

TEST(PipeTest, OperationsAfterClose) {
  Pipe pipe;
  pipe.Close();

  auto read_result = pipe.Read();
  EXPECT_FALSE(read_result.ok());

  Pipe pipe2;
  pipe2.Close();
  auto write_result = pipe2.Write("data");
  EXPECT_FALSE(write_result.ok());
}

}  // namespace ray
