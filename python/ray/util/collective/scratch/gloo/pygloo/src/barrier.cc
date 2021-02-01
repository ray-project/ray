#include <collective.h>
#include <gloo/barrier.h>

namespace pygloo {

void barrier(const std::shared_ptr<gloo::Context> &context, uint32_t tag) {
  gloo::BarrierOptions opts_(context);

  opts_.setTag(tag);

  gloo::barrier(opts_);
}
} // namespace pygloo