#include <collective.h>
#include <gloo/types.h>

namespace pygloo {

template <typename T>
void recv(const std::shared_ptr<gloo::Context> &context, intptr_t recvbuf,
          size_t size, int peer, uint32_t tag) {
  if (context->rank == peer)
    throw std::runtime_error(
        "peer equals to current rank. Please specify other peer values.");

  auto outputBuffer = context->createUnboundBuffer(
      reinterpret_cast<T *>(recvbuf), size * sizeof(T));

  constexpr uint8_t kSendRecvSlotPrefix = 0x09;
  gloo::Slot slot = gloo::Slot::build(kSendRecvSlotPrefix, tag);

  outputBuffer->recv(peer, slot);
  outputBuffer->waitRecv(context->getTimeout());
}

void recv_wrapper(const std::shared_ptr<gloo::Context> &context,
                  intptr_t recvbuf, size_t size, glooDataType_t datatype,
                  int peer, uint32_t tag) {
  switch (datatype) {
  case glooDataType_t::glooInt8:
    recv<int8_t>(context, recvbuf, size, peer, tag);
    break;
  case glooDataType_t::glooUint8:
    recv<uint8_t>(context, recvbuf, size, peer, tag);
    break;
  case glooDataType_t::glooInt32:
    recv<int32_t>(context, recvbuf, size, peer, tag);
    break;
  case glooDataType_t::glooUint32:
    recv<uint32_t>(context, recvbuf, size, peer, tag);
    break;
  case glooDataType_t::glooInt64:
    recv<int64_t>(context, recvbuf, size, peer, tag);
    break;
  case glooDataType_t::glooUint64:
    recv<uint64_t>(context, recvbuf, size, peer, tag);
    break;
  case glooDataType_t::glooFloat16:
    recv<gloo::float16>(context, recvbuf, size, peer, tag);
    break;
  case glooDataType_t::glooFloat32:
    recv<float_t>(context, recvbuf, size, peer, tag);
    break;
  case glooDataType_t::glooFloat64:
    recv<double_t>(context, recvbuf, size, peer, tag);
    break;
  default:
    throw std::runtime_error("Unhandled dataType");
  }
}
} // namespace pygloo