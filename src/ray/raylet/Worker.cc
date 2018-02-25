#ifndef WORKER_CC
#define WORKER_CC

#include "Worker.h"

#include <boost/bind.hpp>

#include "common.h"
#include "format/nm_generated.h"
#include "node_manager.h"

using namespace std;
namespace ray {

/// A constructor responsible for initializing the state of a worker.
Worker::Worker(pid_t pid, shared_ptr<ClientConnection> connection) {
  pid_ = pid;
  connection_ = connection;
}

pid_t Worker::Pid() const {
  return pid_;
}

const shared_ptr<ClientConnection> Worker::Connection() const {
  return connection_;
}

} // end namespace ray

#endif
