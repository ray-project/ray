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

TEST_F(AuthenticationTokenTest, TestSelfMoveAssignment) {
  AuthenticationToken token("test-token");

  // Self-assignment should not break the token
  token = std::move(token);

  EXPECT_FALSE(token.empty());
  AuthenticationToken expected("test-token");
  EXPECT_TRUE(token.Equals(expected));
}

TEST_F(AuthenticationTokenTest, TestEquals) {
  AuthenticationToken token1("same-token");
  AuthenticationToken token2("same-token");
  AuthenticationToken token3("different-token");

  EXPECT_TRUE(token1.Equals(token2));
  EXPECT_FALSE(token1.Equals(token3));
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

TEST_F(AuthenticationTokenTest, TestSpecialCharacters) {
  std::string special = "token-with-special!@#$%^&*()_+={}[]|\\:;\"'<>,.?/~`";
  AuthenticationToken token(special);

  EXPECT_FALSE(token.empty());
  AuthenticationToken expected(special);
  EXPECT_TRUE(token.Equals(expected));
}

TEST_F(AuthenticationTokenTest, TestUnicodeCharacters) {
  std::string unicode = "token-with-unicode-café-😀";
  AuthenticationToken token(unicode);

  EXPECT_FALSE(token.empty());
  AuthenticationToken expected(unicode);
  EXPECT_TRUE(token.Equals(expected));
}

TEST_F(AuthenticationTokenTest, TestBinaryData) {
  std::string binary;
  for (int i = 0; i < 256; ++i) {
    binary += static_cast<char>(i);
  }

  AuthenticationToken token(binary);

  EXPECT_FALSE(token.empty());
  AuthenticationToken expected(binary);
  EXPECT_TRUE(token.Equals(expected));
}

TEST_F(AuthenticationTokenTest, TestLongToken) {
  std::string long_token(10000, 'x');
  AuthenticationToken token(long_token);

  EXPECT_FALSE(token.empty());
  AuthenticationToken expected(long_token);
  EXPECT_TRUE(token.Equals(expected));
}

TEST_F(AuthenticationTokenTest, TestConstTimeComparison) {
  // This test verifies that comparison works correctly
  // Actual timing attack resistance would require specialized timing tests
  AuthenticationToken token1("token-abc");
  AuthenticationToken token2("token-xyz");
  AuthenticationToken token3("token-abc");

  EXPECT_FALSE(token1.Equals(token2));
  EXPECT_TRUE(token1.Equals(token3));
}

TEST_F(AuthenticationTokenTest, TestMoveClearsOriginal) {
  AuthenticationToken token1("test-token");
  AuthenticationToken expected("test-token");

  AuthenticationToken token2(std::move(token1));

  // Original should be empty after move
  EXPECT_TRUE(token1.empty());
  // New token should have the value
  EXPECT_TRUE(token2.Equals(expected));
}

TEST_F(AuthenticationTokenTest, TestMoveAssignmentClearsOriginal) {
  AuthenticationToken token1("test-token");
  AuthenticationToken token2("other-token");
  AuthenticationToken expected("test-token");

  token2 = std::move(token1);

  // Original should be empty after move
  EXPECT_TRUE(token1.empty());
  // New token should have the value
  EXPECT_TRUE(token2.Equals(expected));
}

}  // namespace rpc
}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
