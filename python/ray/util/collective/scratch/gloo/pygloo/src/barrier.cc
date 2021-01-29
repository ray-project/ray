#include <collective.h>
#include <gloo/Barrier.h>


namespace pygloo {

void barrier(const std::shared_ptr<gloo::Context> &context) {
  gloo::BarrierOptions opts_(context);

  opts_.setTag(0);

  gloo::barrier(opts_);
}

} // pygloo