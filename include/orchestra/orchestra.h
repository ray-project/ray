#ifndef ORCHESTRA_INCLUDE_ORCHESTRA_H
#define ORCHESTRA_INCLUDE_ORCHESTRA_H

#include <vector>
#include <unordered_map>

typedef size_t ObjRef;
typedef size_t WorkerId;
typedef size_t ObjStoreId;

class FnInfo {
  size_t num_return_vals_;
  std::vector<WorkerId> workers_; // `workers_` is a sorted vector
public:
  void set_num_return_vals(size_t num) {
    num_return_vals_ = num;
  }
  size_t num_return_vals() const {
    return num_return_vals_;
  }
  void add_worker(WorkerId workerid) {
    // insert `workerid` into `workers_` so that `workers_` stays sorted
    workers_.insert(std::lower_bound(workers_.begin(), workers_.end(), workerid), workerid);
  }
  size_t num_workers() const {
    return workers_.size();
  }
  const std::vector<WorkerId>& workers() const {
    return workers_;
  }
};

typedef std::vector<std::vector<ObjStoreId> > ObjTable;
typedef std::unordered_map<std::string, FnInfo> FnTable;

#define ORCH_VERBOSE -1
#define ORCH_INFO 0
#define ORCH_DEBUG 1
#define ORCH_FATAL 2

#define ORCH_LOG(LEVEL, MESSAGE) \
  if (LEVEL == ORCH_VERBOSE) { \
    \
  } else if (LEVEL == ORCH_FATAL) { \
    std::cerr << "fatal error occured: " << MESSAGE << std::endl; \
    std::exit(1); \
  } else { \
    std::cout << MESSAGE << std::endl; \
  }

class objstore_not_registered_error : public std::runtime_error
{
public:
  objstore_not_registered_error(const std::string& msg) : std::runtime_error(msg) {}
};

struct slice {
  uint8_t* data;
  size_t len;
};

#endif
