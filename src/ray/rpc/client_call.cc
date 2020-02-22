#include "ray/rpc/client_call.h"

namespace ray {
namespace rpc {

const std::shared_ptr<ClientCall> &ClientCallTag::GetCall() const { return call_; }

void ClientCallManager::PollEventsFromCompletionQueue(int index) {
  void *got_tag;
  bool ok = false;
  // Keep reading events from the `CompletionQueue` until it's shutdown.
  // NOTE(edoakes): we use AsyncNext here because for some unknown reason,
  // synchronous cq_.Next blocks indefinitely in the case that the process
  // received a SIGTERM.
  while (true) {
    auto deadline = gpr_time_add(gpr_now(GPR_CLOCK_REALTIME),
                                 gpr_time_from_millis(250, GPR_TIMESPAN));
    auto status = cqs_[index].AsyncNext(&got_tag, &ok, deadline);
    if (status == grpc::CompletionQueue::SHUTDOWN) {
      break;
    } else if (status == grpc::CompletionQueue::TIMEOUT && shutdown_) {
      // If we timed out and shutdown, then exit immediately. This should not
      // be needed, but gRPC seems to not return SHUTDOWN correctly in these
      // cases (e.g., test_wait will hang on shutdown without this check).
      break;
    } else if (status != grpc::CompletionQueue::TIMEOUT) {
      auto tag = reinterpret_cast<ClientCallTag *>(got_tag);
      tag->GetCall()->SetReturnStatus();
      if (ok && !main_service_.stopped() && !shutdown_) {
        // Post the callback to the main event loop.
        main_service_.post([tag]() {
          tag->GetCall()->OnReplyReceived();
          // The call is finished, and we can delete this tag now.
          delete tag;
        });
      } else {
        delete tag;
      }
    }
  }
}

ClientCall::~ClientCall() = default;

ClientCallManager::ClientCallManager(boost::asio::io_service &main_service,
                                     int num_threads)
    : main_service_(main_service), num_threads_(num_threads), shutdown_(false) {
  rr_index_ = rand() % num_threads_;  // Start the polling threads.
  cqs_.reserve(num_threads_);
  for (int i = 0; i < num_threads_; i++) {
    cqs_.emplace_back();
    polling_threads_.emplace_back(&ClientCallManager::PollEventsFromCompletionQueue, this,
                                  i);
  }
}

ClientCallManager::~ClientCallManager() {
  shutdown_ = true;
  for (auto &cq : cqs_) {
    cq.Shutdown();
  }
  for (auto &polling_thread : polling_threads_) {
    polling_thread.join();
  }
}

}  // namespace rpc
}  // namespace ray
