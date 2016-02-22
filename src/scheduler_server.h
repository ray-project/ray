#ifndef ORCHESTRA_SCHEDULER_SERVER_H
#define ORCHESTRA_SCHEDULER_SERVER_H

#include <iostream>
#include <memory>
#include <string>
#include <mutex>

#include "scheduler.h"


class SchedulerServerServiceImpl final : public SchedulerServer::Service {
  ObjTable objtable_;
  std::unique_ptr<Scheduler> scheduler_;
public:
  SchedulerServerServiceImpl() : scheduler_(new Scheduler()) {
  }
  Status RemoteCall(ServerContext* context, const RemoteCallRequest* request, RemoteCallReply* reply) override;
  Status PushObj(ServerContext* context, const PushObjRequest* request, PushObjReply* reply) override;
  Status PullObj(ServerContext* context, const PullObjRequest* request, AckReply* reply) override {
    return Status::OK;
  }
  Status RegisterWorker(ServerContext* context, const RegisterWorkerRequest* request, RegisterWorkerReply* reply) override {
    WorkerId workerid = scheduler_->register_worker(request->worker_address(), request->objstore_address());
    reply->set_workerid(workerid);
    return Status::OK;
  }
  Status RegisterObjStore(ServerContext* context, const RegisterObjStoreRequest* request, RegisterObjStoreReply* reply) override {
    try {
      reply->set_objstoreid(scheduler_->register_objstore(request->address()));
    } catch (...) {
      std::cout << "caught exception" << std::endl;
    }
    return Status::OK;
  }
  Status RegisterFunction(ServerContext* context, const RegisterFunctionRequest* request, AckReply* reply) override {
    scheduler_->register_function(request->fnname(), request->workerid(), request->num_return_vals());
    return Status::OK;
  }
  Status GetDebugInfo(ServerContext* context, const GetDebugInfoRequest* request, GetDebugInfoReply* reply) override {
    return Status::OK;
  }
};

#endif
