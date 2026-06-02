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
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/version.hpp>
#include <chrono>
#include <fstream>
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

Status EstablishSslConnection(net::io_context &ioc,
                              ssl::context &ctx,
                              ssl::stream<beast::tcp_stream> &stream,
                              tcp::resolver &resolver) {
  ctx.load_verify_file(kK8sCaCertPath);
  ctx.set_verify_mode(ssl::verify_peer);

  beast::get_lowest_layer(stream).expires_after(std::chrono::seconds(kK8sApiTimeoutSecs));

  if (!SSL_set_tlsext_host_name(stream.native_handle(), k8s_host)) {
    beast::error_code ec{static_cast<int>(::ERR_get_error()),
                         net::error::get_ssl_category()};
    return Status::IOError(absl::StrCat("Failed to set SNI hostname: ", ec.message()));
  }

  auto const results = resolver.resolve(k8s_host, k8s_port);
  beast::get_lowest_layer(stream).connect(results);
  stream.handshake(ssl::stream_base::client);
  return Status::OK();
}

Status K8sApiPost(const std::string &path,
                  const nlohmann::json &body,
                  nlohmann::json &response_json) {
  if (!k8s_client_initialized || k8s_host == nullptr || k8s_port == nullptr) {
    return Status::Invalid("Kubernetes client configuration is not initialized.");
  }

  static std::string k8s_sa_token = ReadFile(kK8sSaTokenPath);

  try {
    net::io_context ioc;
    ssl::context ctx(ssl::context::tlsv12_client);
    tcp::resolver resolver(ioc);
    ssl::stream<beast::tcp_stream> stream(ioc, ctx);

    RAY_RETURN_NOT_OK(EstablishSslConnection(ioc, ctx, stream, resolver));

    http::request<http::string_body> req{http::verb::post, path, 11};
    req.set(http::field::host, k8s_host);
    req.set(http::field::user_agent, BOOST_BEAST_VERSION_STRING);
    req.set(http::field::content_type, "application/json");
    std::string auth_header = "Bearer " + k8s_sa_token;
    req.set(http::field::authorization, auth_header);
    req.body() = body.dump();
    req.prepare_payload();

    http::write(stream, req);

    beast::flat_buffer buffer;
    http::response<http::string_body> res;
    http::read(stream, buffer, res);

    if (res.result() != http::status::ok && res.result() != http::status::created) {
      RAY_LOG(WARNING) << "Kubernetes API Post request returned HTTP status "
                       << res.result_int() << ". Response: " << res.body();
      if (res.result() == http::status::conflict) {
        return Status::AlreadyExists(absl::StrCat("Conflict: ", res.body()));
      }
      return Status::IOError(
          absl::StrCat("HTTP error ", res.result_int(), ": ", res.body()));
    }

    response_json = nlohmann::json::parse(res.body());

    beast::error_code ec;
    stream.shutdown(ec);
    if (ec == net::error::eof) {
      ec.assign(0, ec.category());
    }
    if (ec) {
      RAY_LOG(WARNING) << "SSL stream shutdown failed: " << ec.message();
    }
  } catch (const std::exception &e) {
    RAY_LOG(ERROR) << "Kubernetes API Post request failed: " << e.what();
    std::string err_msg(e.what());
    if (err_msg.find("Operation aborted") != std::string::npos ||
        err_msg.find("timeout") != std::string::npos) {
      return Status::TimedOut(
          absl::StrCat("Kubernetes API Post request timed out: ", e.what()));
    }
    return Status::IOError(
        absl::StrCat("Kubernetes API Post request failed: ", e.what()));
  }

  return Status::OK();
}

Status K8sApiGet(const std::string &path, nlohmann::json &response_json) {
  if (!k8s_client_initialized || k8s_host == nullptr || k8s_port == nullptr) {
    return Status::Invalid("Kubernetes client configuration is not initialized.");
  }

  static std::string k8s_sa_token = ReadFile(kK8sSaTokenPath);

  try {
    net::io_context ioc;
    ssl::context ctx(ssl::context::tlsv12_client);
    tcp::resolver resolver(ioc);
    ssl::stream<beast::tcp_stream> stream(ioc, ctx);

    RAY_RETURN_NOT_OK(EstablishSslConnection(ioc, ctx, stream, resolver));

    http::request<http::empty_body> req{http::verb::get, path, 11};
    req.set(http::field::host, k8s_host);
    req.set(http::field::user_agent, BOOST_BEAST_VERSION_STRING);
    std::string auth_header = "Bearer " + k8s_sa_token;
    req.set(http::field::authorization, auth_header);

    http::write(stream, req);

    beast::flat_buffer buffer;
    http::response<http::string_body> res;
    http::read(stream, buffer, res);

    if (res.result() != http::status::ok) {
      RAY_LOG(WARNING) << "Kubernetes API Get request returned HTTP status "
                       << res.result_int() << ". Response: " << res.body();
      if (res.result() == http::status::not_found) {
        return Status::NotFound(absl::StrCat("Resource not found: ", res.body()));
      }
      return Status::IOError(
          absl::StrCat("HTTP error ", res.result_int(), ": ", res.body()));
    }

    response_json = nlohmann::json::parse(res.body());

    auto date_hdr = res[http::field::date];
    if (!date_hdr.empty()) {
      response_json["__api_server_date__"] = std::string(date_hdr);
    }

    beast::error_code ec;
    stream.shutdown(ec);
    if (ec == net::error::eof) {
      ec.assign(0, ec.category());
    }
    if (ec) {
      RAY_LOG(WARNING) << "SSL stream shutdown failed: " << ec.message();
    }
  } catch (const std::exception &e) {
    RAY_LOG(ERROR) << "Kubernetes API Get request failed: " << e.what();
    std::string err_msg(e.what());
    if (err_msg.find("Operation aborted") != std::string::npos ||
        err_msg.find("timeout") != std::string::npos) {
      return Status::TimedOut(
          absl::StrCat("Kubernetes API Get request timed out: ", e.what()));
    }
    return Status::IOError(absl::StrCat("Kubernetes API Get request failed: ", e.what()));
  }

  return Status::OK();
}

Status K8sApiPut(const std::string &path,
                 const nlohmann::json &body,
                 nlohmann::json &response_json) {
  if (!k8s_client_initialized || k8s_host == nullptr || k8s_port == nullptr) {
    return Status::Invalid("Kubernetes client configuration is not initialized.");
  }

  static std::string k8s_sa_token = ReadFile(kK8sSaTokenPath);

  try {
    net::io_context ioc;
    ssl::context ctx(ssl::context::tlsv12_client);
    tcp::resolver resolver(ioc);
    ssl::stream<beast::tcp_stream> stream(ioc, ctx);

    RAY_RETURN_NOT_OK(EstablishSslConnection(ioc, ctx, stream, resolver));

    http::request<http::string_body> req{http::verb::put, path, 11};
    req.set(http::field::host, k8s_host);
    req.set(http::field::user_agent, BOOST_BEAST_VERSION_STRING);
    req.set(http::field::content_type, "application/json");
    std::string auth_header = "Bearer " + k8s_sa_token;
    req.set(http::field::authorization, auth_header);
    req.body() = body.dump();
    req.prepare_payload();

    http::write(stream, req);

    beast::flat_buffer buffer;
    http::response<http::string_body> res;
    http::read(stream, buffer, res);

    if (res.result() != http::status::ok && res.result() != http::status::created) {
      RAY_LOG(WARNING) << "Kubernetes API Put request returned HTTP status "
                       << res.result_int() << ". Response: " << res.body();
      if (res.result() == http::status::conflict) {
        return Status::AlreadyExists(absl::StrCat("Conflict: ", res.body()));
      }
      return Status::IOError(
          absl::StrCat("HTTP error ", res.result_int(), ": ", res.body()));
    }

    response_json = nlohmann::json::parse(res.body());

    beast::error_code ec;
    stream.shutdown(ec);
    if (ec == net::error::eof) {
      ec.assign(0, ec.category());
    }
    if (ec) {
      RAY_LOG(WARNING) << "SSL stream shutdown failed: " << ec.message();
    }
  } catch (const std::exception &e) {
    RAY_LOG(ERROR) << "Kubernetes API Put request failed: " << e.what();
    std::string err_msg(e.what());
    if (err_msg.find("Operation aborted") != std::string::npos ||
        err_msg.find("timeout") != std::string::npos) {
      return Status::TimedOut(
          absl::StrCat("Kubernetes API Put request timed out: ", e.what()));
    }
    return Status::IOError(absl::StrCat("Kubernetes API Put request failed: ", e.what()));
  }

  return Status::OK();
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
