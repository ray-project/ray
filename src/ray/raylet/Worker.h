#ifndef WORKER_H
#define WORKER_H
using namespace std;
namespace ray {
/// Worker class encapsulates the implementation details of a worker. A worker
/// is the execution container around a unit of Ray work, such as a task or an
/// actor. Ray units of work execute in the context of a Worker.
class Worker {
public:
  /// A constructor that initializes a worker object.
  Worker();
  /// A destructor responsible for freeing all worker state.
  ~Worker() {}
private:
  /// Connection state of a worker.
  /// TODO(swang): provide implementation details for ClientConnection
  int ClientConnection conn_;

};

} // end namespace ray

#endif
