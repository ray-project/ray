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

#include "ray/rpc/authentication/authentication_token.h"

#include <sstream>
#include <string>
#include <utility>

#include "gtest/gtest.h"

namespace ray {
namespace rpc {

class AuthenticationTokenTest : public ::testing::Test {};

TEST_F(AuthenticationTokenTest, TestDefaultConstructor) {
  AuthenticationToken token;
  EXPECT_TRUE(token.empty());
}

TEST_F(AuthenticationTokenTest, TestConstructorWithValue) {
  AuthenticationToken token("test-token-value");
  EXPECT_FALSE(token.empty());
  AuthenticationToken expected("test-token-value");
  EXPECT_TRUE(token.Equals(expected));
}

TEST_F(AuthenticationTokenTest, TestMoveConstructor) {
  AuthenticationToken token1("original-token");
  AuthenticationToken token2(std::move(token1));

  EXPECT_FALSE(token2.empty());
  AuthenticationToken expected("original-token");
  EXPECT_TRUE(token2.Equals(expected));
  EXPECT_TRUE(token1.empty());
}

TEST_F(AuthenticationTokenTest, TestMoveAssignment) {
  AuthenticationToken token1("first-token");
  AuthenticationToken token2("second-token");

  token2 = std::move(token1);

  EXPECT_FALSE(token2.empty());
  AuthenticationToken expected("first-token");
  EXPECT_TRUE(token2.Equals(expected));
  EXPECT_TRUE(token1.empty());
}

TEST_F(AuthenticationTokenTest, TestEquals) {
  AuthenticationToken token1("same-token");
  AuthenticationToken token2("same-token");
  AuthenticationToken token3("different-token");

  EXPECT_TRUE(token1.Equals(token2));
  EXPECT_FALSE(token1.Equals(token3));
  EXPECT_TRUE(token1 == token2);
  EXPECT_FALSE(token1 == token3);
  EXPECT_FALSE(token1 != token2);
  EXPECT_TRUE(token1 != token3);
}

TEST_F(AuthenticationTokenTest, TestEqualityDifferentLengths) {
  AuthenticationToken token1("short");
  AuthenticationToken token2("much-longer-token");

  EXPECT_FALSE(token1.Equals(token2));
}

TEST_F(AuthenticationTokenTest, TestEqualityEmptyTokens) {
  AuthenticationToken token1;
  AuthenticationToken token2;

  EXPECT_TRUE(token1.Equals(token2));
}

TEST_F(AuthenticationTokenTest, TestEqualityEmptyVsNonEmpty) {
  AuthenticationToken token1;
  AuthenticationToken token2("non-empty");

  EXPECT_FALSE(token1.Equals(token2));
  EXPECT_FALSE(token2.Equals(token1));
}

TEST_F(AuthenticationTokenTest, TestRedactedOutput) {
  AuthenticationToken token("super-secret-token");

  std::ostringstream oss;
  oss << token;

  std::string output = oss.str();
  EXPECT_EQ(output, "<Redacted Authentication Token>");
  EXPECT_EQ(output.find("super-secret-token"), std::string::npos);
}

TEST_F(AuthenticationTokenTest, TestEmptyString) {
  AuthenticationToken token("");
  EXPECT_TRUE(token.empty());
  AuthenticationToken expected("");
  EXPECT_TRUE(token.Equals(expected));
}
}  // namespace rpc
}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
