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

#include "ray/rpc/authentication/k8s_util.h"

#include <boost/asio/connect.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ssl/error.hpp>
#include <boost/asio/ssl/stream.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/version.hpp>
#include <chrono>
#include <fstream>
#include <memory>
#include <string>

#include "nlohmann/json.hpp"
#include "ray/rpc/authentication/k8s_constants.h"
#include "ray/util/logging.h"

namespace ray {
namespace rpc {
namespace k8s {

namespace {

namespace beast = boost::beast;
namespace http = beast::http;
namespace net = boost::asio;
namespace ssl = net::ssl;
using tcp = net::ip::tcp;

// Reads the content of a file into a string.
std::string ReadFile(const std::string &path) {
  std::ifstream file(path);
  if (!file.is_open()) {
    return "";
  }
  return std::string((std::istreambuf_iterator<char>(file)),
                     std::istreambuf_iterator<char>());
}

constexpr int kK8sApiTimeoutSecs = 5;

}  // namespace

bool k8s_client_initialized = false;
std::once_flag k8s_client_config_flag;

static const char *k8s_host = nullptr;
static const char *k8s_port = nullptr;

void InitK8sClientConfig() {
  k8s_host = std::getenv(kK8sServiceHostEnvVar);
  k8s_port = std::getenv(kK8sServicePortEnvVar);
  if (k8s_host == nullptr || k8s_port == nullptr) {
    RAY_LOG(WARNING)
        << kK8sServiceHostEnvVar << " or " << kK8sServicePortEnvVar << " not set. "
        << "Cannot initialize Kubernetes client for k8s authentication mode.";
    k8s_host = nullptr;
    return;
  }

  std::string k8s_sa_token = ReadFile(kK8sSaTokenPath);
  if (k8s_sa_token.empty()) {
    RAY_LOG(WARNING) << "Failed to read Kubernetes service account token from "
                     << kK8sSaTokenPath;
    k8s_host = nullptr;  // Invalidate config
    return;
  }

  std::ifstream ca_cert_file(kK8sCaCertPath);
  if (!ca_cert_file.is_open()) {
    RAY_LOG(WARNING) << "Failed to open Kubernetes CA certificate from "
                     << kK8sCaCertPath;
    k8s_host = nullptr;  // Invalidate config
    return;
  }

  k8s_client_initialized = true;
}

// State for a single asynchronous Kubernetes API request. A single
// steady_timer armed at the deadline cancels the in-flight operation (by
// cancelling the resolver and closing the socket) so that a slow or hung API
// server cannot block the caller past the deadline. The state is kept alive
// by shared_ptr for the duration of the async chain.
struct K8sApiRequestState : public std::enable_shared_from_this<K8sApiRequestState> {
  net::io_context ioc;
  ssl::context ctx{ssl::context::tlsv12_client};
  tcp::resolver resolver{ioc};
  ssl::stream<beast::tcp_stream> stream{ioc, ctx};
  net::steady_timer timer{ioc};
  beast::flat_buffer buffer;
  http::request<http::string_body> request;
  http::response<http::string_body> response;
  std::chrono::steady_clock::time_point deadline;
  beast::error_code ec;
  bool timed_out = false;

  // Runs the request to completion (or to the deadline) and returns the
  // outcome. Blocks in ioc.run() until the async chain finishes or the
  // deadline timer fires.
  Status Run(http::verb method,
             const std::string &path,
             const nlohmann::json *body,
             const std::string &token) {
    deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(kK8sApiTimeoutSecs);

    request = http::request<http::string_body>{method, path, 11};
    request.set(http::field::host, k8s_host);
    request.set(http::field::user_agent, BOOST_BEAST_VERSION_STRING);
    if (body != nullptr) {
      request.set(http::field::content_type, "application/json");
      request.body() = body->dump();
    }
    request.set(http::field::authorization, "Bearer " + token);
    request.prepare_payload();

    beast::error_code setup_ec;
    ctx.load_verify_file(kK8sCaCertPath, setup_ec);
    if (setup_ec) {
      return Status::IOError(
          absl::StrCat("Failed to load CA cert: ", setup_ec.message()));
    }
    ctx.set_verify_mode(ssl::verify_peer, setup_ec);
    if (setup_ec) {
      return Status::IOError(
          absl::StrCat("Failed to set verify mode: ", setup_ec.message()));
    }
    if (!SSL_set_tlsext_host_name(stream.native_handle(), k8s_host)) {
      setup_ec = beast::error_code{static_cast<int>(::ERR_get_error()),
                                   net::error::get_ssl_category()};
      return Status::IOError(
          absl::StrCat("Failed to set SNI hostname: ", setup_ec.message()));
    }

    timer.expires_at(deadline);
    timer.async_wait([self = shared_from_this()](beast::error_code timer_ec) {
      self->OnTimer(timer_ec);
    });
    resolver.async_resolve(
        k8s_host, k8s_port,
        [self = shared_from_this()](beast::error_code resolve_ec,
                                    tcp::resolver::results_type results) {
          self->OnResolve(resolve_ec, results);
        });

    ioc.run();

    if (timed_out) {
      return Status::TimedOut(absl::StrCat("Kubernetes API request timed out after ",
                                           kK8sApiTimeoutSecs, " seconds."));
    }
    if (ec) {
      return Status::IOError(
          absl::StrCat("Kubernetes API request failed: ", ec.message()));
    }
    return Status::OK();
  }

  void OnTimer(beast::error_code timer_ec) {
    if (timer_ec) {
      return;  // Cancelled because the request already finished.
    }
    timed_out = true;
    resolver.cancel();
    beast::get_lowest_layer(stream).close();
  }

  void OnResolve(beast::error_code resolve_ec, tcp::resolver::results_type results) {
    if (resolve_ec) {
      Finish(resolve_ec);
      return;
    }
    beast::get_lowest_layer(stream).expires_at(deadline);
    beast::get_lowest_layer(stream).async_connect(
        results,
        [self = shared_from_this()](beast::error_code connect_ec,
                                    tcp::endpoint /*connected_endpoint*/) {
          self->OnConnect(connect_ec);
        });
  }

  void OnConnect(beast::error_code connect_ec) {
    if (connect_ec) {
      Finish(connect_ec);
      return;
    }
    beast::get_lowest_layer(stream).expires_at(deadline);
    stream.async_handshake(ssl::stream_base::client,
                           [self = shared_from_this()](beast::error_code handshake_ec) {
                             self->OnHandshake(handshake_ec);
                           });
  }

  void OnHandshake(beast::error_code handshake_ec) {
    if (handshake_ec) {
      Finish(handshake_ec);
      return;
    }
    beast::get_lowest_layer(stream).expires_at(deadline);
    http::async_write(stream, request,
                      [self = shared_from_this()](beast::error_code write_ec,
                                                  std::size_t /*bytes_transferred*/) {
                        self->OnWrite(write_ec);
                      });
  }

  void OnWrite(beast::error_code write_ec) {
    if (write_ec) {
      Finish(write_ec);
      return;
    }
    beast::get_lowest_layer(stream).expires_at(deadline);
    http::async_read(stream, buffer, response,
                     [self = shared_from_this()](beast::error_code read_ec,
                                                 std::size_t /*bytes_transferred*/) {
                       self->OnRead(read_ec);
                     });
  }

  void OnRead(beast::error_code read_ec) { Finish(read_ec); }

  void Finish(beast::error_code finish_ec) {
    ec = finish_ec;
    beast::error_code cancel_ec;
    timer.cancel(cancel_ec);
  }
};

const char *MethodName(http::verb method) {
  switch (method) {
    case http::verb::get:
      return "Get";
    case http::verb::put:
      return "Put";
    default:
      return "Post";
  }
}

// Performs one HTTP request against the Kubernetes API server, enforcing the
// kK8sApiTimeoutSecs deadline with a single async helper driven by ioc.run().
// A null body sends a request without a JSON body (GET). The per-verb HTTP
// status handling matches the previous synchronous helpers.
Status K8sApiRequest(http::verb method,
                     const std::string &path,
                     const nlohmann::json *body,
                     nlohmann::json &response_json) {
  if (!k8s_client_initialized || k8s_host == nullptr || k8s_port == nullptr) {
    return Status::Invalid("Kubernetes client configuration is not initialized.");
  }

  std::string k8s_sa_token = ReadFile(kK8sSaTokenPath);

  try {
    auto state = std::make_shared<K8sApiRequestState>();
    RAY_RETURN_NOT_OK(state->Run(method, path, body, k8s_sa_token));

    const http::response<http::string_body> &res = state->response;
    if (method == http::verb::get) {
      if (res.result() != http::status::ok) {
        RAY_LOG(WARNING) << "Kubernetes API Get request returned HTTP status "
                         << res.result_int() << ". Response: " << res.body();
        if (res.result() == http::status::not_found) {
          return Status::NotFound(absl::StrCat("Resource not found: ", res.body()));
        }
        return Status::IOError(
            absl::StrCat("HTTP error ", res.result_int(), ": ", res.body()));
      }
    } else {
      if (res.result() != http::status::ok && res.result() != http::status::created) {
        RAY_LOG(WARNING) << "Kubernetes API " << MethodName(method)
                         << " request returned HTTP status " << res.result_int()
                         << ". Response: " << res.body();
        if (res.result() == http::status::conflict) {
          return Status::AlreadyExists(absl::StrCat("Conflict: ", res.body()));
        }
        return Status::IOError(
            absl::StrCat("HTTP error ", res.result_int(), ": ", res.body()));
      }
    }

    response_json = nlohmann::json::parse(res.body());

    if (method == http::verb::get) {
      auto date_hdr = res[http::field::date];
      if (!date_hdr.empty()) {
        response_json["__api_server_date__"] = std::string(date_hdr);
      }
    }

    beast::error_code ec;
    state->stream.shutdown(ec);
    if (ec == net::error::eof) {
      ec.assign(0, ec.category());
    }
    if (ec) {
      RAY_LOG(WARNING) << "SSL stream shutdown failed: " << ec.message();
    }
  } catch (const std::exception &e) {
    RAY_LOG(ERROR) << "Kubernetes API " << MethodName(method)
                   << " request failed: " << e.what();
    std::string err_msg(e.what());
    if (err_msg.find("Operation aborted") != std::string::npos ||
        err_msg.find("timeout") != std::string::npos) {
      return Status::TimedOut(absl::StrCat("Kubernetes API ", MethodName(method),
                                           " request timed out: ", e.what()));
    }
    return Status::IOError(absl::StrCat("Kubernetes API ", MethodName(method),
                                        " request failed: ", e.what()));
  }

  return Status::OK();
}

Status K8sApiPost(const std::string &path,
                  const nlohmann::json &body,
                  nlohmann::json &response_json) {
  return K8sApiRequest(http::verb::post, path, &body, response_json);
}

Status K8sApiGet(const std::string &path, nlohmann::json &response_json) {
  return K8sApiRequest(http::verb::get, path, nullptr, response_json);
}

Status K8sApiPut(const std::string &path,
                 const nlohmann::json &body,
                 nlohmann::json &response_json) {
  return K8sApiRequest(http::verb::put, path, &body, response_json);
}


bool ValidateToken(const AuthenticationToken &token) {
  std::string token_str = token.GetRawValue();

  nlohmann::json token_review_req = {{"apiVersion", kAuthenticationAPIVersion},
                                     {"kind", kTokenReviewKind},
                                     {"spec", {{"token", token_str}}}};
  nlohmann::json token_review_resp;

  if (!k8s::K8sApiPost(
           kAuthenticationV1TokenReviewPath, token_review_req, token_review_resp)
           .ok()) {
    RAY_LOG(WARNING) << "Kubernetes TokenReview request failed.";
    return false;
  }

  if (!token_review_resp.contains("status") ||
      !token_review_resp["status"].contains("authenticated") ||
      !token_review_resp["status"]["authenticated"].get<bool>()) {
    std::string error_msg = "Invalid token";
    if (token_review_resp.contains("status") &&
        token_review_resp["status"].contains("error")) {
      error_msg = token_review_resp["status"]["error"].get<std::string>();
    }
    RAY_LOG(WARNING) << "Kubernetes token review failed: " << error_msg;
    return false;
  }

  const char *ray_cluster_name_env = std::getenv(kRayClusterNameEnvVar);
  const char *ray_cluster_namespace_env = std::getenv(kRayClusterNamespaceEnvVar);

  if (ray_cluster_name_env == nullptr || ray_cluster_namespace_env == nullptr) {
    RAY_LOG(WARNING) << kRayClusterNameEnvVar << " or " << kRayClusterNamespaceEnvVar
                     << " env var not set, "
                     << "authorization check failed.";
    return false;
  }

  nlohmann::json spec;
  spec["resourceAttributes"] = {{"group", kRayResourceGroup},
                                {"resource", kRayClusterResourceName},
                                {"name", ray_cluster_name_env},
                                {"verb", kRayClusterRayUserVerb},
                                {"namespace", ray_cluster_namespace_env}};

  auto user_info = token_review_resp["status"]["user"];
  if (user_info.contains("username")) {
    spec["user"] = user_info["username"];
  }
  if (user_info.contains("groups")) {
    spec["groups"] = user_info["groups"];
  }
  if (user_info.contains("extra")) {
    spec["extra"] = user_info["extra"];
  }

  nlohmann::json subject_access_review_req = {{"apiVersion", kAuthorizationAPIVersion},
                                              {"kind", kSubjectAccessReviewKind},
                                              {"spec", spec}};
  nlohmann::json subject_access_review_resp;

  if (!k8s::K8sApiPost(kAuthorizationV1SubjectAccessReviewPath,
                       subject_access_review_req,
                       subject_access_review_resp)
           .ok()) {
    RAY_LOG(WARNING) << "Kubernetes SubjectAccessReview request failed.";
    return false;
  }

  if (!subject_access_review_resp.contains("status") ||
      !subject_access_review_resp["status"].contains("allowed") ||
      !subject_access_review_resp["status"]["allowed"].get<bool>()) {
    RAY_LOG(WARNING) << "User '" << user_info["username"].get<std::string>()
                     << "' is not authorized to access RayCluster '"
                     << ray_cluster_name_env << "' with verb 'ray-user'.";
    return false;
  }

  return true;
}

}  // namespace k8s
}  // namespace rpc
}  // namespace ray
