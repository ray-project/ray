// Copyright 2025 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <grpcpp/grpcpp.h>

#include <cstdint>
#include <cstring>
#include <iostream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "ray/common/constants.h"

namespace ray {
namespace rpc {

/// Secure wrapper for authentication tokens.
/// - Wipes memory on destruction
/// - Constant-time comparison
/// - Redacted output when logged or printed
class AuthenticationToken {
 public:
  AuthenticationToken() = default;
  explicit AuthenticationToken(std::string value) : secret_(value.begin(), value.end()) {}

  AuthenticationToken(const AuthenticationToken &other) : secret_(other.secret_) {}
  AuthenticationToken &operator=(const AuthenticationToken &other) {
    if (this != &other) {
      SecureClear();
      secret_ = other.secret_;
    }
    return *this;
  }

  // Move operations
  AuthenticationToken(AuthenticationToken &&other) noexcept {
    MoveFrom(std::move(other));
  }
  AuthenticationToken &operator=(AuthenticationToken &&other) noexcept {
    if (this != &other) {
      SecureClear();
      MoveFrom(std::move(other));
    }
    return *this;
  }
  ~AuthenticationToken() { SecureClear(); }

  bool empty() const noexcept { return secret_.empty(); }

  /// Constant-time equality comparison
  bool Equals(const AuthenticationToken &other) const noexcept {
    return ConstTimeEqual(secret_, other.secret_);
  }

  /// Equality operator (constant-time)
  bool operator==(const AuthenticationToken &other) const noexcept {
    return Equals(other);
  }

  /// Inequality operator
  bool operator!=(const AuthenticationToken &other) const noexcept {
    return !(*this == other);
  }

  /// Set authentication metadata on a gRPC client context
  /// Only call this from client-side code
  void SetMetadata(grpc::ClientContext &context) const {
    if (!secret_.empty()) {
      context.AddMetadata(kAuthTokenKey,
                          kBearerPrefix + std::string(secret_.begin(), secret_.end()));
    }
  }

  /// Get token as Authorization header value
  /// WARNING: This exposes the raw token. Use sparingly.
  /// Returns "Bearer <token>" format suitable for Authorization header
  /// @return Authorization header value, or empty string if token is empty
  std::string ToAuthorizationHeaderValue() const {
    if (secret_.empty()) {
      return "";
    }
    return kBearerPrefix + std::string(secret_.begin(), secret_.end());
  }

  /// Create AuthenticationToken from gRPC metadata value
  /// Strips "Bearer " prefix and creates token object
  /// @param metadata_value The raw value from server metadata (should include "Bearer "
  /// prefix)
  /// @return AuthenticationToken object (empty if format invalid)
  static AuthenticationToken FromMetadata(std::string_view metadata_value) {
    const std::string_view prefix(kBearerPrefix);
    if (metadata_value.size() < prefix.size() ||
        metadata_value.substr(0, prefix.size()) != prefix) {
      return AuthenticationToken();  // Invalid format, return empty
    }
    std::string_view token_part = metadata_value.substr(prefix.size());
    return AuthenticationToken(std::string(token_part));
  }

  friend std::ostream &operator<<(std::ostream &os, const AuthenticationToken &t) {
    return os << "<Redacted Authentication Token>";
  }

 private:
  std::vector<uint8_t> secret_;

  // Constant-time string comparison to avoid timing attacks.
  // https://en.wikipedia.org/wiki/Timing_attack
  static bool ConstTimeEqual(const std::vector<uint8_t> &a,
                             const std::vector<uint8_t> &b) noexcept {
    if (a.size() != b.size()) {
      return false;
    }
    unsigned char diff = 0;
    for (size_t i = 0; i < a.size(); ++i) {
      diff |= a[i] ^ b[i];
    }
    return diff == 0;
  }

  // replace the characters in the memory with 0
  static void ExplicitBurn(void *p, size_t n) noexcept {
#if defined(_MSC_VER)
    SecureZeroMemory(p, n);
#elif defined(__STDC_LIB_EXT1__)
    memset_s(p, n, 0, n);
#else
    // Using array indexing instead of pointer arithmetic
    volatile auto *vp = static_cast<volatile uint8_t *>(p);
    for (size_t i = 0; i < n; ++i) {
      vp[i] = 0;
    }
#endif
  }

  void SecureClear() noexcept {
    if (!secret_.empty()) {
      ExplicitBurn(secret_.data(), secret_.size());
      secret_.clear();
    }
  }

  void MoveFrom(AuthenticationToken &&other) noexcept {
    secret_ = std::move(other.secret_);
    // Clear the moved-from object explicitly for security
    // Note: 'other' is already an rvalue reference, no need to move again
    other.SecureClear();
  }
};

}  // namespace rpc
}  // namespace ray
