#ifndef ORCHESTRA_INCLUDE_ORCHESTRA_H
#define ORCHESTRA_INCLUDE_ORCHESTRA_H

#include <vector>
#include <unordered_map>

typedef size_t ObjRef;
typedef size_t WorkerId;
typedef size_t ObjStoreId;

class FnInfo {
  size_t num_return_vals_;
  std::vector<WorkerId> workers_;
public:
  void set_num_return_vals(size_t num) {
    num_return_vals_ = num;
  }
  size_t num_return_vals() const {
    return num_return_vals_;
  }
  void add_worker(WorkerId workerid) {
    workers_.push_back(workerid);
  }
  size_t num_workers() const {
    return workers_.size();
  }
  ObjRef worker(size_t i) const {
    return workers_[i];
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
  char* data;
  size_t len;
};

#endif
