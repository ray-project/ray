#ifndef HALO_INCLUDE_HALO_H
#define HALO_INCLUDE_HALO_H

#include <vector>
#include <unordered_map>

typedef size_t ObjRef;
typedef size_t WorkerId;
typedef size_t ObjStoreId;
typedef size_t OperationId;

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

#define HALO_VERBOSE -1
#define HALO_INFO 0
#define HALO_DEBUG 1
#define HALO_FATAL 2
#define HALO_REFCOUNT HALO_VERBOSE
#define HALO_ALIAS HALO_VERBOSE

#define HALO_LOG(LEVEL, MESSAGE) \
  if (LEVEL == HALO_VERBOSE) { \
    \
  } else if (LEVEL == HALO_FATAL) { \
    std::cerr << "fatal error occured: " << MESSAGE << std::endl; \
    std::exit(1); \
  } else if (LEVEL == HALO_DEBUG) { \
    \
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
