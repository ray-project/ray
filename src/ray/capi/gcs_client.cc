// Copyright 2020-2023 The Ray Authors.
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

#ifndef _WIN32
#include <unistd.h>
#endif

#include <assert.h>
#include <ray/common/status.h>

#include <boost/asio.hpp>
#include <msgpack.hpp>

#include "ray/common/asio/instrumented_io_context.h"
#include "ray/gcs/gcs_client/gcs_client.h"

using namespace ray::gcs;
using namespace ray;
using namespace std;

static GcsClientOptions client_options;

class GcsClientComplex {
 public:
  GcsClientComplex(const GcsClientOptions &gcs_client_options);
  ~GcsClientComplex();
  bool Connect();
  void Disconnect();

  mutable absl::Mutex mutex_;
  bool is_connected_ GUARDED_BY(mutex_) = false;

  std::unique_ptr<std::thread> thread_io_service_;
  std::unique_ptr<instrumented_io_context> io_service_;
  std::unique_ptr<GcsClient> gcs_client_ GUARDED_BY(mutex_);
};

GcsClientComplex::GcsClientComplex(const GcsClientOptions &gcs_client_options) {
  gcs_client_ = std::make_unique<GcsClient>(gcs_client_options);
  io_service_ = std::make_unique<instrumented_io_context>();

  std::promise<bool> promise;
  thread_io_service_ = std::make_unique<std::thread>([this, &promise] {
    SetThreadName("global.accessor");
    std::unique_ptr<boost::asio::io_service::work> work(
        new boost::asio::io_service::work(*io_service_));
    promise.set_value(true);
    io_service_->run();
  });
  promise.get_future().get();
}

GcsClientComplex::~GcsClientComplex() { Disconnect(); }

bool GcsClientComplex::Connect() {
  absl::WriterMutexLock lock(&mutex_);
  if (!is_connected_) {
    is_connected_ = true;
    return gcs_client_->Connect(*io_service_).ok();
  }
  RAY_LOG(DEBUG) << "Duplicated connection for GlobalStateAccessor.";
  return true;
}

void GcsClientComplex::Disconnect() {
  absl::WriterMutexLock lock(&mutex_);
  RAY_LOG(DEBUG) << "Global state accessor disconnect";
  if (is_connected_) {
    io_service_->stop();
    thread_io_service_->join();
    gcs_client_->Disconnect();
    is_connected_ = false;
  }
}

extern "C" {

void GcsClient_ClientOptions_Update(const uint8_t *gcs_address, size_t gcs_address_len) {
  auto address = std::string((const char *)gcs_address, gcs_address_len);
  client_options = GcsClientOptions(address);
}

void *GcsClient_Create() {
  auto *gcs = new GcsClientComplex(client_options);
  if (gcs->Connect()) {
    return gcs;
  } else {
    delete gcs;
    return nullptr;
  }
}

void GcsClient_Destroy(void *gcs_client) {
  if (gcs_client != nullptr) {
    auto p = (GcsClientComplex *)gcs_client;
    p->Disconnect();
    delete p;
  }
}

int GcsClient_InternalKV_Get(void *gcs_client,
                             const uint8_t *ns,
                             size_t ns_len,
                             const uint8_t *key,
                             size_t key_len,
                             uint8_t *data,
                             size_t *data_len) {
  auto p = (GcsClientComplex *)gcs_client;
  auto ns_str = std::string((const char *)ns, ns_len);
  auto key_str = std::string((const char *)key, key_len);
  std::string value;
  absl::WriterMutexLock lock(&p->mutex_);
  auto status = p->gcs_client_->InternalKV().Get(ns_str, key_str, value);
  if (status.ok()) {
    if (*data_len < value.length()) {
      *data_len = 0;
      return -1;
    }
    *data_len = value.length();
    memcpy(data, value.c_str(), value.length());
    return 0;
  } else {
    *data_len = 0;
  }
  return -1;
}

bool GcsClient_InternalKV_Put(void *gcs_client,
                              const uint8_t *ns,
                              size_t ns_len,
                              const uint8_t *key,
                              size_t key_len,
                              const uint8_t *data,
                              size_t data_len) {
  auto p = (GcsClientComplex *)gcs_client;
  auto ns_str = std::string((const char *)ns, ns_len);
  auto key_str = std::string((const char *)key, key_len);
  auto data_str = std::string((const char *)data, data_len);
  bool added;
  absl::WriterMutexLock lock(&p->mutex_);
  auto status = p->gcs_client_->InternalKV().Put(ns_str, key_str, data_str, true, added);
  if (!status.ok()) {
    RAY_LOG(ERROR) << "Failed to put key " << key_str << " to GCS: " << status.ToString();
    return false;
  }
  return added;
}

bool GcsClient_InternalKV_Del(void *gcs_client,
                              const uint8_t *ns,
                              size_t ns_len,
                              const uint8_t *key,
                              size_t key_len) {
  auto p = (GcsClientComplex *)gcs_client;
  auto ns_str = std::string((const char *)ns, ns_len);
  auto key_str = std::string((const char *)key, key_len);
  absl::WriterMutexLock lock(&p->mutex_);
  auto status = p->gcs_client_->InternalKV().Del(ns_str, key_str, false);
  if (!status.ok()) {
    RAY_LOG(ERROR) << "Failed to delete key " << key_str
                   << " from GCS: " << status.ToString();
    return false;
  }
  return true;
}

bool GcsClient_InternalKV_Exists(void *gcs_client,
                                 const uint8_t *ns,
                                 size_t ns_len,
                                 const uint8_t *key,
                                 size_t key_len) {
  auto p = (GcsClientComplex *)gcs_client;
  auto ns_str = std::string((const char *)ns, ns_len);
  auto key_str = std::string((const char *)key, key_len);
  absl::WriterMutexLock lock(&p->mutex_);
  bool exists;
  auto status = p->gcs_client_->InternalKV().Exists(ns_str, key_str, exists);
  if (!status.ok()) {
    RAY_LOG(ERROR) << "Failed to check if key " << key_str
                   << " exists in GCS: " << status.ToString();
    return false;
  }
  return exists;
}
}
