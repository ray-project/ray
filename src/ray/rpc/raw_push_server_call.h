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

#pragma once

#include <grpcpp/generic/async_generic_service.h>
#include <grpcpp/grpcpp.h>

#include <functional>
#include <memory>
#include <string>
#include <utility>

#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/grpc_util.h"
#include "ray/common/status.h"
#include "ray/object_manager/push_bytes.h"
#include "ray/rpc/metrics.h"
#include "ray/rpc/rpc_callback_types.h"
#include "ray/rpc/server_call.h"
#include "src/ray/protobuf/object_manager.grpc.pb.h"

namespace ray {
namespace rpc {

/// Subclass of the generated ObjectManagerService::AsyncService that marks
/// the Push method as raw (ByteBuffer-based) instead of protobuf-based.
class RawObjectManagerAsyncService : public ObjectManagerService::AsyncService {
 public:
  RawObjectManagerAsyncService() {
    // Mark Push (method index 0) as raw so it delivers ByteBuffer
    // instead of deserialized PushRequest.
    MarkMethodRaw(0);
  }

  /// Request a raw Push call. Must be used instead of the typed RequestPush()
  /// when MarkMethodRaw(0) has been called.
  void RequestRawPush(grpc::ServerContext *ctx,
                      grpc::ByteBuffer *request,
                      grpc::ServerAsyncResponseWriter<grpc::ByteBuffer> *writer,
                      grpc::CompletionQueue *call_cq,
                      grpc::ServerCompletionQueue *notification_cq,
                      void *tag) {
    RequestAsyncUnary(0, ctx, request, writer, call_cq, notification_cq, tag);
  }
};

/// Handler interface for raw Push requests.
class RawPushHandler {
 public:
  virtual ~RawPushHandler() = default;

  /// Handle a raw Push request with pre-parsed header and data pointer.
  virtual void HandlePush(PushRequest header,
                          const uint8_t *data,
                          size_t data_len,
                          SendReplyCallback send_reply_callback) = 0;
};

/// ServerCall implementation for raw ByteBuffer-based Push RPCs.
class RawPushServerCall : public ServerCall {
 public:
  RawPushServerCall(const ServerCallFactory &factory,
                    RawPushHandler &handler,
                    instrumented_io_context &io_service,
                    std::string call_name,
                    bool record_metrics)
      : state_(ServerCallState::PENDING),
        factory_(factory),
        handler_(handler),
        response_writer_(&context_),
        io_service_(io_service),
        call_name_(std::move(call_name)),
        start_time_(0),
        record_metrics_(record_metrics) {
    if (record_metrics_) {
      grpc_server_req_new_counter_.Record(1.0, {{"Method", call_name_}});
    }
  }

  ServerCallState GetState() const override { return state_; }

  void HandleRequest() override {
    stats_handle_ = io_service_.stats()->RecordStart(call_name_);
    start_time_ = absl::GetCurrentTimeNanos();
    if (record_metrics_) {
      grpc_server_req_handling_counter_.Record(1.0, {{"Method", call_name_}});
    }
    if (!io_service_.stopped()) {
      io_service_.post([this] { HandleRequestImpl(); },
                       call_name_ + ".HandleRequestImpl");
    } else {
      SendReply(Status::Invalid("HandleServiceClosed"));
    }
  }

  void OnReplySent() override {
    if (record_metrics_) {
      grpc_server_req_finished_counter_.Record(1.0, {{"Method", call_name_}});
      grpc_server_req_succeeded_counter_.Record(1.0, {{"Method", call_name_}});
    }
    if (send_reply_success_callback_ && !io_service_.stopped()) {
      io_service_.post(
          [callback = std::move(send_reply_success_callback_)]() { callback(); },
          call_name_ + ".success_callback");
    }
    LogProcessTime();
  }

  void OnReplyFailed() override {
    if (record_metrics_) {
      grpc_server_req_finished_counter_.Record(1.0, {{"Method", call_name_}});
      grpc_server_req_failed_counter_.Record(1.0, {{"Method", call_name_}});
    }
    if (send_reply_failure_callback_ && !io_service_.stopped()) {
      io_service_.post(
          [callback = std::move(send_reply_failure_callback_)]() { callback(); },
          call_name_ + ".failure_callback");
    }
    LogProcessTime();
  }

  const ServerCallFactory &GetServerCallFactory() override { return factory_; }

 private:
  void HandleRequestImpl() {
    state_ = ServerCallState::PROCESSING;
    if (factory_.GetMaxActiveRPCs() == -1) {
      factory_.CreateCall();
    }

    // Deserialize the raw ByteBuffer into header + data pointer.
    DeserializedPush parsed;
    auto parse_status = DeserializePushFromByteBuffer(&request_buffer_, &parsed);
    if (!parse_status.ok()) {
      RAY_LOG(WARNING) << "Failed to parse raw Push request: " << parse_status;
      boost::asio::post(GetServerCallExecutor(),
                        [this] { SendReply(Status::IOError("Malformed push request")); });
      return;
    }

    handler_.HandlePush(
        std::move(parsed.header),
        parsed.data,
        parsed.data_len,
        [this](
            Status status, std::function<void()> success, std::function<void()> failure) {
          send_reply_success_callback_ = std::move(success);
          send_reply_failure_callback_ = std::move(failure);
          boost::asio::post(GetServerCallExecutor(),
                            [this, status]() { SendReply(status); });
        });
  }

  void LogProcessTime() {
    io_service_.stats()->RecordEnd(std::move(stats_handle_));
    auto end_time = absl::GetCurrentTimeNanos();
    if (record_metrics_) {
      grpc_server_req_process_time_ms_histogram_.Record(
          (end_time - start_time_) / 1000000.0, {{"Method", call_name_}});
    }
  }

  void SendReply(const Status &status) {
    if (io_service_.stopped()) {
      RAY_LOG_EVERY_N(WARNING, 100) << "Not sending reply because executor stopped.";
      return;
    }
    state_ = ServerCallState::SENDING_REPLY;
    // PushReply is empty, so send an empty ByteBuffer.
    grpc::ByteBuffer empty_reply;
    response_writer_.Finish(empty_reply, RayStatusToGrpcStatus(status), this);
  }

  ServerCallState state_;
  const ServerCallFactory &factory_;
  RawPushHandler &handler_;
  grpc::ServerContext context_;
  grpc::ByteBuffer request_buffer_;
  grpc::ServerAsyncResponseWriter<grpc::ByteBuffer> response_writer_;
  instrumented_io_context &io_service_;
  std::string call_name_;
  std::shared_ptr<StatsHandle> stats_handle_;
  int64_t start_time_;
  bool record_metrics_;
  std::function<void()> send_reply_success_callback_;
  std::function<void()> send_reply_failure_callback_;

  ray::stats::Histogram grpc_server_req_process_time_ms_histogram_{
      GetGrpcServerReqProcessTimeMsHistogramMetric()};
  ray::stats::Count grpc_server_req_new_counter_{GetGrpcServerReqNewCounterMetric()};
  ray::stats::Count grpc_server_req_handling_counter_{
      GetGrpcServerReqHandlingCounterMetric()};
  ray::stats::Count grpc_server_req_finished_counter_{
      GetGrpcServerReqFinishedCounterMetric()};
  ray::stats::Count grpc_server_req_succeeded_counter_{
      GetGrpcServerReqSucceededCounterMetric()};
  ray::stats::Count grpc_server_req_failed_counter_{
      GetGrpcServerReqFailedCounterMetric()};

  friend class RawPushServerCallFactory;
};

/// Factory that creates RawPushServerCall objects for the Push RPC.
class RawPushServerCallFactory : public ServerCallFactory {
 public:
  RawPushServerCallFactory(RawObjectManagerAsyncService &service,
                           RawPushHandler &handler,
                           const std::unique_ptr<grpc::ServerCompletionQueue> &cq,
                           instrumented_io_context &io_service,
                           std::string call_name,
                           bool record_metrics)
      : service_(service),
        handler_(handler),
        cq_(cq),
        io_service_(io_service),
        call_name_(std::move(call_name)),
        record_metrics_(record_metrics) {}

  void CreateCall() const override {
    auto *call =
        new RawPushServerCall(*this, handler_, io_service_, call_name_, record_metrics_);
    service_.RequestRawPush(&call->context_,
                            &call->request_buffer_,
                            &call->response_writer_,
                            cq_.get(),
                            cq_.get(),
                            call);
  }

  int64_t GetMaxActiveRPCs() const override { return -1; }

 private:
  RawObjectManagerAsyncService &service_;
  RawPushHandler &handler_;
  const std::unique_ptr<grpc::ServerCompletionQueue> &cq_;
  instrumented_io_context &io_service_;
  std::string call_name_;
  bool record_metrics_;
};

}  // namespace rpc
}  // namespace ray
