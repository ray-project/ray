// Copyright 2025 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <mutex>
#include <string>

#include "nlohmann/json.hpp"
#include "ray/rpc/authentication/authentication_token.h"

namespace ray {
namespace rpc {
namespace k8s {

// True if the Kubernetes client has been successfully initialized.
extern bool k8s_client_initialized;
// Flag to ensure one-time initialization of the Kubernetes client.
extern std::once_flag k8s_client_config_flag;

/// Initializes the Kubernetes client configuration from the in-cluster environment.
void InitK8sClientConfig();

/// Performs an HTTP POST request to the Kubernetes API server.
/// \param path The API path for the request.
/// \param body The JSON body of the request.
/// \param[out] response_json The JSON response from the API server.
/// \return true if the request was successful, false otherwise.
bool K8sApiPost(const std::string &path,
                const nlohmann::json &body,
                nlohmann::json &response_json);

// Validates a token by calling the Kubernetes API server.
/// \param token The token to validate.
/// \return true if the token is valid, false otherwise.
bool ValidateToken(const AuthenticationToken &token);

}  // namespace k8s
}  // namespace rpc
}  // namespace ray
