// Copyright 2021 The Ray Authors.
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

namespace ray {

class MockRayletClientInterface : public RayletClientInterface {
 public:
  MOCK_METHOD(std::shared_ptr<grpc::Channel>, GetChannel, (), (const));
  MOCK_METHOD(void,
              ReportWorkerBacklog,
              (const WorkerID &worker_id,
               const std::vector<rpc::WorkerBacklogReport> &backlog_reports),
              (override));
  MOCK_METHOD(
      void,
      RequestWorkerLease,
      (const rpc::LeaseSpec &lease_spec,
       bool grant_or_reject,
       const ray::rpc::ClientCallback<ray::rpc::RequestWorkerLeaseReply> &callback,
       const int64_t backlog_size,
       const bool is_selected_based_on_locality),
      (override));
  MOCK_METHOD(void,
              ReturnWorkerLease,
              (int worker_port,
               const LeaseID &lease_id,
               bool disconnect_worker,
               const std::string &disconnect_worker_error_detail,
               bool worker_exiting),
              (override));
  MOCK_METHOD(void,
              GetWorkerFailureCause,
              (const LeaseID &lease_id,
               const rpc::ClientCallback<rpc::GetWorkerFailureCauseReply> &callback),
              (override));
  MOCK_METHOD(void,
              PrestartWorkers,
              (const rpc::PrestartWorkersRequest &request,
               const rpc::ClientCallback<ray::rpc::PrestartWorkersReply> &callback),
              (override));
  MOCK_METHOD(void,
              ReleaseUnusedActorWorkers,
              (const std::vector<WorkerID> &workers_in_use,
               const rpc::ClientCallback<rpc::ReleaseUnusedActorWorkersReply> &callback),
              (override));
  MOCK_METHOD(void,
              CancelWorkerLease,
              (const LeaseID &lease_id,
               const rpc::ClientCallback<rpc::CancelWorkerLeaseReply> &callback),
              (override));
  MOCK_METHOD(
      void,
      PrepareBundleResources,
      (const std::vector<std::shared_ptr<const BundleSpecification>> &bundle_specs,
       const ray::rpc::ClientCallback<ray::rpc::PrepareBundleResourcesReply> &callback),
      (override));
  MOCK_METHOD(
      void,
      CommitBundleResources,
      (const std::vector<std::shared_ptr<const BundleSpecification>> &bundle_specs,
       const ray::rpc::ClientCallback<ray::rpc::CommitBundleResourcesReply> &callback),
      (override));
  MOCK_METHOD(
      void,
      CancelResourceReserve,
      (const BundleSpecification &bundle_spec,
       const ray::rpc::ClientCallback<ray::rpc::CancelResourceReserveReply> &callback),
      (override));
  MOCK_METHOD(void,
              ReleaseUnusedBundles,
              (const std::vector<rpc::Bundle> &bundles_in_use,
               const rpc::ClientCallback<rpc::ReleaseUnusedBundlesReply> &callback),
              (override));
  MOCK_METHOD(void,
              PinObjectIDs,
              (const rpc::Address &caller_address,
               const std::vector<ObjectID> &object_ids,
               const ObjectID &generator_id,
               const ray::rpc::ClientCallback<ray::rpc::PinObjectIDsReply> &callback),
              (override));
  MOCK_METHOD(void,
              GetResourceLoad,
              (const rpc::ClientCallback<rpc::GetResourceLoadReply> &callback),
              (override));
  MOCK_METHOD(void,
              RegisterMutableObjectReader,
              (const ObjectID &object_id,
               int64_t num_readers,
               const ObjectID &local_reader_object_id,
               const rpc::ClientCallback<ray::rpc::RegisterMutableObjectReply> &callback),
              (override));
  MOCK_METHOD(void,
              PushMutableObject,
              (const ObjectID &object_id,
               uint64_t data_size,
               uint64_t metadata_size,
               void *data,
               void *metadata,
               const rpc::ClientCallback<ray::rpc::PushMutableObjectReply> &callback),
              (override));
  MOCK_METHOD(void,
              GetSystemConfig,
              (const rpc::ClientCallback<rpc::GetSystemConfigReply> &callback),
              (override));
  MOCK_METHOD(void,
              NotifyGCSRestart,
              (const rpc::ClientCallback<rpc::NotifyGCSRestartReply> &callback),
              (override));
  MOCK_METHOD(void,
              ShutdownRaylet,
              (const NodeID &node_id,
               bool graceful,
               const rpc::ClientCallback<rpc::ShutdownRayletReply> &callback),
              (override));
  MOCK_METHOD(void,
              DrainRaylet,
              (const rpc::autoscaler::DrainNodeReason &reason,
               const std::string &reason_message,
               int64_t draining_deadline_timestamp_ms,
               const rpc::ClientCallback<rpc::DrainRayletReply> &callback),
              (override));
  MOCK_METHOD(
      void,
      CancelLeasesWithResourceShapes,
      ((const std::vector<google::protobuf::Map<std::string, double>>)&resource_shapes,
       const rpc::ClientCallback<rpc::CancelLeasesWithResourceShapesReply> &callback),
      (override));
  MOCK_METHOD(void,
              IsLocalWorkerDead,
              (const WorkerID &worker_id,
               const rpc::ClientCallback<rpc::IsLocalWorkerDeadReply> &callback),
              (override));
  MOCK_METHOD(void,
              GetNodeStats,
              (const rpc::GetNodeStatsRequest &request,
               const rpc::ClientCallback<rpc::GetNodeStatsReply> &callback),
              (override));
  MOCK_METHOD(void,
              KillLocalActor,
              (const rpc::KillLocalActorRequest &request,
               const rpc::ClientCallback<rpc::KillLocalActorReply> &callback),
              (override));
  MOCK_METHOD(void,
              GlobalGC,
              (const rpc::ClientCallback<rpc::GlobalGCReply> &callback),
              (override));
  MOCK_METHOD(int64_t, GetPinsInFlight, (), (const, override));
};

}  // namespace ray
