#ifndef RAY_INCLUDE_RAY_H
#define RAY_INCLUDE_RAY_H

#include <vector>
#include <unordered_map>
#include <algorithm>
#include "logging.h"

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
