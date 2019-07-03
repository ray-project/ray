
#include "src/ray/rpc/raylet/raylet_client.h"

namespace ray {
namespace rpc {

RayletClient::RayletClient(const std::string &raylet_socket, const WorkerID &worker_id,
                           bool is_worker, const JobID &job_id,
                           const ::Language &language)
    : worker_id_(worker_id),
      is_worker_(is_worker),
      job_id_(job_id),
      language_(language),
      main_service_(),
      work_(main_service_),
      client_call_manager_(main_service_),
      heartbeat_timer_(main_service_) {
  std::shared_ptr<grpc::Channel> channel =
      grpc::CreateChannel("unix://" + raylet_socket, grpc::InsecureChannelCredentials());
  stub_ = RayletService::NewStub(channel);

  rpc_thread_ = std::thread([this]() { main_service_.run(); });
  RAY_LOG(INFO) << "[RayletClient] Connect to unix socket: "
                << "unix://" + raylet_socket
                << ", is worker: " << (is_worker_ ? "true" : "false")
                << ", worker id: " << worker_id;
  // Try to register client for 10 times.
  TryRegisterClient(10);
}

void RayletClient::TryRegisterClient(int times) {
  auto st = RegisterClient();
  if (!st.ok()) {
    if (times > 0) {
      heartbeat_timer_.expires_from_now(boost::posix_time::milliseconds(300));
      heartbeat_timer_.async_wait([this, times](const boost::system::error_code &error) {
        TryRegisterClient(times - 1);
      });
    } else {
      RAY_LOG(FATAL) << "Failed to register to raylet server, worker id: " << worker_id_;
    }
  } else {
    Heartbeat();
  }
}

RayletClient::~RayletClient() {
  main_service_.stop();
  rpc_thread_.join();
}

ray::Status RayletClient::Disconnect() {
  DisconnectClientRequest disconnect_client_request;
  disconnect_client_request.set_worker_id(worker_id_.Binary());
  disconnect_client_request.set_intentional(true);

  // This promise and future are only used to make this call synchronized.
  std::promise<Status> p;
  std::future<Status> f(p.get_future());
  auto callback = [&p](const Status &status, const DisconnectClientReply &reply) {
    if (!status.ok()) {
      RAY_LOG(ERROR) << "[RayletClient] Failed to disconnect from raylet, msg: "
                     << status.message();
    }
    p.set_value(status);
  };
  client_call_manager_
      .CreateCall<RayletService, DisconnectClientRequest, DisconnectClientReply>(
          *stub_, &RayletService::Stub::PrepareAsyncDisconnectClient,
          disconnect_client_request, callback);
  return f.get();
}

ray::Status RayletClient::SubmitTask(const std::vector<ObjectID> &execution_dependencies,
                                     const ray::raylet::TaskSpecification &task_spec) {
  SubmitTaskRequest submit_task_request;
  submit_task_request.set_task_spec(task_spec.SerializeAsString());
  IdVectorToProtobuf<ObjectID, SubmitTaskRequest>(
      execution_dependencies, submit_task_request,
      &SubmitTaskRequest::add_execution_dependencies);

  // This promise and future are only used to make this call synchronized.
  std::promise<Status> p;
  std::future<Status> f(p.get_future());
  auto callback = [&p](const Status &status, const SubmitTaskReply &reply) {
    if (!status.ok()) {
      RAY_LOG(ERROR) << "[RayletClient] Failed to submit task, msg: " << status.message();
    }
    p.set_value(status);
  };

  client_call_manager_.CreateCall<RayletService, SubmitTaskRequest, SubmitTaskReply>(
      *stub_, &RayletService::Stub::PrepareAsyncSubmitTask, submit_task_request,
      callback);
  return f.get();
}

ray::Status RayletClient::GetTask(
    std::unique_ptr<ray::raylet::TaskSpecification> *task_spec) {
  GetTaskRequest get_task_request;
  get_task_request.set_worker_id(worker_id_.Binary());

  // Callback to deal with reply.
  // This promise and future are only used to make this call synchronized.
  std::promise<Status> p;
  std::future<Status> f(p.get_future());
  auto callback = [this, task_spec, &p](const Status &status, const GetTaskReply &reply) {
    resource_ids_.clear();
    if (status.ok()) {
      for (size_t i = 0; i < reply.fractional_resource_ids().size(); ++i) {
        auto const &fractional_resource_ids = reply.fractional_resource_ids()[i];
        auto &acquired_resources = resource_ids_[fractional_resource_ids.resource_name()];

        size_t num_resource_ids = fractional_resource_ids.resource_ids().size();
        size_t num_resource_fractions =
            fractional_resource_ids.resource_fractions().size();
        RAY_CHECK(num_resource_ids == num_resource_fractions);
        RAY_CHECK(num_resource_ids > 0);
        for (size_t j = 0; j < num_resource_ids; ++j) {
          int64_t resource_id = fractional_resource_ids.resource_ids()[j];
          double resource_fraction = fractional_resource_ids.resource_fractions()[j];
          if (num_resource_ids > 1) {
            int64_t whole_fraction = resource_fraction;
            RAY_CHECK(whole_fraction == resource_fraction);
          }
          acquired_resources.emplace_back(resource_id, resource_fraction);
        }
      }
      task_spec->reset(new ray::raylet::TaskSpecification(reply.task_spec()));
    } else {
      *task_spec = nullptr;
      RAY_LOG(INFO) << "[RayletClient] Get task failed, msg: " << status.message();
    }
    p.set_value(status);
  };

  client_call_manager_.CreateCall<RayletService, GetTaskRequest, GetTaskReply>(
      *stub_, &RayletService::Stub::PrepareAsyncGetTask, get_task_request, callback);
  return f.get();
}

ray::Status RayletClient::FetchOrReconstruct(const std::vector<ObjectID> &object_ids,
                                             bool fetch_only,
                                             const TaskID &current_task_id) {
  FetchOrReconstructRequest fetch_or_reconstruct_request;
  fetch_or_reconstruct_request.set_fetch_only(fetch_only);
  fetch_or_reconstruct_request.set_task_id(current_task_id.Binary());
  fetch_or_reconstruct_request.set_worker_id(worker_id_.Binary());
  IdVectorToProtobuf<ObjectID, FetchOrReconstructRequest>(
      object_ids, fetch_or_reconstruct_request,
      &FetchOrReconstructRequest::add_object_ids);

  // Callback to deal with reply.
  // This promise and future are only used to make this call synchronized.
  std::promise<Status> p;
  std::future<Status> f(p.get_future());
  auto callback = [&p](const Status &status, const FetchOrReconstructReply &reply) {
    if (!status.ok()) {
      RAY_LOG(INFO) << "[RayletClient] Fetch or reconstruct failed, msg: "
                    << status.message();
    }
    p.set_value(status);
  };

  client_call_manager_
      .CreateCall<RayletService, FetchOrReconstructRequest, FetchOrReconstructReply>(
          *stub_, &RayletService::Stub::PrepareAsyncFetchOrReconstruct,
          fetch_or_reconstruct_request, callback);
  return f.get();
}

ray::Status RayletClient::NotifyUnblocked(const TaskID &current_task_id) {
  NotifyUnblockedRequest notify_unblocked_request;
  notify_unblocked_request.set_worker_id(worker_id_.Binary());
  notify_unblocked_request.set_task_id(current_task_id.Binary());

  // This promise and future are only used to make this call synchronized.
  std::promise<Status> p;
  std::future<Status> f(p.get_future());
  auto callback = [&p](const Status &status, const NotifyUnblockedReply &reply) {
    if (!status.ok()) {
      RAY_LOG(INFO) << "[RayletClient] Notify unblocked failed, msg: "
                    << status.message();
    }
    p.set_value(status);
  };

  client_call_manager_
      .CreateCall<RayletService, NotifyUnblockedRequest, NotifyUnblockedReply>(
          *stub_, &RayletService::Stub::PrepareAsyncNotifyUnblocked,
          notify_unblocked_request, callback);
  return f.get();
}

ray::Status RayletClient::Wait(const std::vector<ObjectID> &object_ids, int num_returns,
                               int64_t timeout_milliseconds, bool wait_local,
                               const TaskID &current_task_id, WaitResultPair *result) {
  WaitRequest wait_request;
  wait_request.set_worker_id(worker_id_.Binary());
  wait_request.set_timeout(timeout_milliseconds);
  wait_request.set_wait_local(wait_local);
  wait_request.set_task_id(current_task_id.Binary());
  wait_request.set_num_ready_objects(num_returns);
  IdVectorToProtobuf<ObjectID, WaitRequest>(object_ids, wait_request,
                                            &WaitRequest::add_object_ids);

  // This promise and future are only used to make this call synchronized.
  std::promise<Status> p;
  std::future<Status> f(p.get_future());
  auto callback = [result, &p](const Status &status, const WaitReply &reply) {
    if (status.ok()) {
      result->first = IdVectorFromProtobuf<ObjectID>(reply.found());
      result->second = IdVectorFromProtobuf<ObjectID>(reply.remaining());
    } else {
      RAY_LOG(INFO) << "[RayletClient] Wait failed, msg: " << status.message();
    }
    p.set_value(status);
  };

  client_call_manager_.CreateCall<RayletService, WaitRequest, WaitReply>(
      *stub_, &RayletService::Stub::PrepareAsyncWait, wait_request, callback);
  return f.get();
}

ray::Status RayletClient::PushError(const ray::JobID &job_id, const std::string &type,
                                    const std::string &error_message, double timestamp) {
  PushErrorRequest push_error_request;
  push_error_request.set_job_id(job_id.Binary());
  push_error_request.set_type(type);
  push_error_request.set_error_message(error_message);
  push_error_request.set_timestamp(timestamp);

  // This promise and future are only used to make this call synchronized.
  std::promise<Status> p;
  std::future<Status> f(p.get_future());
  auto callback = [&p](const Status &status, const PushErrorReply &reply) {
    if (!status.ok()) {
      RAY_LOG(INFO) << "[RayletClient] Push error failed, msg: " << status.message();
    }
    p.set_value(status);
  };

  client_call_manager_.CreateCall<RayletService, PushErrorRequest, PushErrorReply>(
      *stub_, &RayletService::Stub::PrepareAsyncPushError, push_error_request, callback);
  return f.get();
}

ray::Status RayletClient::PushProfileEvents(ProfileTableData *profile_events) {
  PushProfileEventsRequest push_profile_events_request;
  push_profile_events_request.set_allocated_profile_table_data(profile_events);

  // This promise and future are only used to make this call synchronized.
  std::promise<Status> p;
  std::future<Status> f(p.get_future());
  auto callback = [&p](const Status &status, const PushProfileEventsReply &reply) {
    if (!status.ok()) {
      RAY_LOG(INFO) << "[RayletClient] Push profile event failed, msg: "
                    << status.message();
    }
    p.set_value(status);
  };

  client_call_manager_
      .CreateCall<RayletService, PushProfileEventsRequest, PushProfileEventsReply>(
          *stub_, &RayletService::Stub::PrepareAsyncPushProfileEvents,
          push_profile_events_request, callback);
  return f.get();
}

ray::Status RayletClient::FreeObjects(const std::vector<ray::ObjectID> &object_ids,
                                      bool local_only, bool delete_creating_tasks) {
  FreeObjectsInObjectStoreRequest free_objects_request;
  free_objects_request.set_local_only(local_only);
  free_objects_request.set_delete_creating_tasks(delete_creating_tasks);
  IdVectorToProtobuf<ray::ObjectID, FreeObjectsInObjectStoreRequest>(
      object_ids, free_objects_request, &FreeObjectsInObjectStoreRequest::add_object_ids);

  // This promise and future are only used to make this call synchronized.
  std::promise<Status> p;
  std::future<Status> f(p.get_future());
  auto callback = [&p](const Status &status, const FreeObjectsInObjectStoreReply &reply) {
    if (!status.ok()) {
      RAY_LOG(INFO) << "[RayletClient] Free objects failed, msg: " << status.message();
    }
    p.set_value(status);
  };

  client_call_manager_.CreateCall<RayletService, FreeObjectsInObjectStoreRequest,
                                  FreeObjectsInObjectStoreReply>(
      *stub_, &RayletService::Stub::PrepareAsyncFreeObjectsInObjectStore,
      free_objects_request, callback);
  return f.get();
}

ray::Status RayletClient::PrepareActorCheckpoint(const ActorID &actor_id,
                                                 ActorCheckpointID &checkpoint_id) {
  PrepareActorCheckpointRequest prepare_actor_checkpoint_request;
  prepare_actor_checkpoint_request.set_actor_id(actor_id.Binary());
  prepare_actor_checkpoint_request.set_worker_id(worker_id_.Binary());

  // This promise and future are only used to make this call synchronized.
  std::promise<Status> p;
  std::future<Status> f(p.get_future());
  auto callback = [&checkpoint_id, &p](const Status &status,
                                       const PrepareActorCheckpointReply &reply) {
    if (status.ok()) {
      checkpoint_id = ActorCheckpointID::FromBinary(reply.checkpoint_id());
    } else {
      RAY_LOG(INFO) << "[RayletClient] Prepare actor checkpoint failed, msg: "
                    << status.message();
    }
    p.set_value(status);
  };

  client_call_manager_.CreateCall<RayletService, PrepareActorCheckpointRequest,
                                  PrepareActorCheckpointReply>(
      *stub_, &RayletService::Stub::PrepareAsyncPrepareActorCheckpoint,
      prepare_actor_checkpoint_request, callback);
  return f.get();
}

ray::Status RayletClient::NotifyActorResumedFromCheckpoint(
    const ActorID &actor_id, const ActorCheckpointID &checkpoint_id) {
  NotifyActorResumedFromCheckpointRequest notify_actor_resumed_from_checkpoint_request;
  notify_actor_resumed_from_checkpoint_request.set_actor_id(actor_id.Binary());
  notify_actor_resumed_from_checkpoint_request.set_checkpoint_id(checkpoint_id.Binary());

  // This promise and future are only used to make this call synchronized.
  std::promise<Status> p;
  std::future<Status> f(p.get_future());
  auto callback = [&p](const Status &status,
                       const NotifyActorResumedFromCheckpointReply &reply) {
    if (!status.ok()) {
      RAY_LOG(INFO) << "[RayletClient] NotifyActorResumedFromCheckpoint failed, msg: "
                    << status.message();
    }
    p.set_value(status);
  };

  client_call_manager_.CreateCall<RayletService, NotifyActorResumedFromCheckpointRequest,
                                  NotifyActorResumedFromCheckpointReply>(
      *stub_, &RayletService::Stub::PrepareAsyncNotifyActorResumedFromCheckpoint,
      notify_actor_resumed_from_checkpoint_request, callback);
  return f.get();
}

ray::Status RayletClient::SetResource(const std::string &resource_name,
                                      const double capacity,
                                      const ray::ClientID &client_id) {
  SetResourceRequest set_resource_request;
  set_resource_request.set_resource_name(resource_name);
  set_resource_request.set_capacity(capacity);
  set_resource_request.set_client_id(client_id.Binary());

  // This promise and future are only used to make this call synchronized.
  std::promise<Status> p;
  std::future<Status> f(p.get_future());
  auto callback = [&p](const Status &status, const SetResourceReply &reply) {
    if (!status.ok()) {
      RAY_LOG(INFO) << "[RayletClient] SetResource failed, msg: " << status.message();
    }
    p.set_value(status);
  };

  client_call_manager_.CreateCall<RayletService, SetResourceRequest, SetResourceReply>(
      *stub_, &RayletService::Stub::PrepareAsyncSetResource, set_resource_request,
      callback);
  return f.get();
}

ray::Status RayletClient::RegisterClient() {
  // Send register client request to raylet server.
  RegisterClientRequest register_client_request;
  register_client_request.set_is_worker(is_worker_);
  register_client_request.set_worker_id(worker_id_.Binary());
  register_client_request.set_worker_pid(getpid());
  register_client_request.set_job_id(job_id_.Binary());
  register_client_request.set_language(static_cast<int32_t>(language_));

  // This promise and future are only used to make this call synchronized.
  std::promise<Status> p;
  std::future<Status> f(p.get_future());
  auto callback = [&p](const Status &status, const RegisterClientReply &reply) {
    if (!status.ok()) {
      RAY_LOG(INFO) << "[RayletClient] Register client failed, msg: " << status.message();
    }
    p.set_value(status);
  };
  client_call_manager_
      .CreateCall<RayletService, RegisterClientRequest, RegisterClientReply>(
          *stub_, &RayletService::Stub::PrepareAsyncRegisterClient,
          register_client_request, callback);
  return f.get();
}

void RayletClient::Heartbeat() {
  // Send register client request to raylet server.
  HeartbeatRequest heartbeat_request;
  heartbeat_request.set_is_worker(is_worker_);
  heartbeat_request.set_worker_id(worker_id_.Binary());

  auto callback = [](const Status &status, const HeartbeatReply &reply) {
    if (!status.ok()) {
      RAY_LOG(INFO) << "[RayletClient] Heartbeat failed, msg: " << status.message();
    }
  };
  client_call_manager_.CreateCall<RayletService, HeartbeatRequest, HeartbeatReply>(
      *stub_, &RayletService::Stub::PrepareAsyncHeartbeat, heartbeat_request, callback);

  heartbeat_timer_.expires_from_now(boost::posix_time::milliseconds(300));
  heartbeat_timer_.async_wait([this](const boost::system::error_code &error) {
    RAY_CHECK(!error);
    Heartbeat();
  });
}

}  // namespace rpc
}  // namespace ray
