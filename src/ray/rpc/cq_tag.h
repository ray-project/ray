#ifndef RAY_RPC_CQ_TAG_H
#define RAY_RPC_CQ_TAG_H

#include <memory>

namespace ray {
namespace rpc {

template <class Call>
class CqTag {
 public:
  explicit CqTag(std::shared_ptr<Call> call) : call_(std::move(call)) {}

  const std::shared_ptr<Call> &GetCall() const { return call_; }

 private:
  std::shared_ptr<Call> call_;
};

}  // namespace rpc
}  // namespace ray
#endif  // RAY_RPC_CQ_TAG_H
