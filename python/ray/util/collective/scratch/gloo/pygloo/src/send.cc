#include <collective.h>
#include <gloo/types.h>
#include <iostream>
namespace pygloo {

template <typename T>
void send(const std::shared_ptr<gloo::Context> &context, intptr_t sendbuf,
          size_t size, int peer, uint32_t tag) {
  if (context->rank == peer)
    throw std::runtime_error(
        "peer equals to current rank. Please specify other peer values.");

  auto inputBuffer = context->createUnboundBuffer(
      reinterpret_cast<T *>(sendbuf), size * sizeof(T));

  constexpr uint8_t kSendRecvSlotPrefix = 0x09;
  gloo::Slot slot = gloo::Slot::build(kSendRecvSlotPrefix, tag);

  inputBuffer->send(peer, slot);
  inputBuffer->waitSend(context->getTimeout());
}

void send_wrapper(const std::shared_ptr<gloo::Context> &context,
                  intptr_t sendbuf, size_t size, glooDataType_t datatype,
                  int peer, uint32_t tag) {
  switch (datatype) {
  case glooDataType_t::glooInt8:
    send<int8_t>(context, sendbuf, size, peer, tag);
    break;
  case glooDataType_t::glooUint8:
    send<uint8_t>(context, sendbuf, size, peer, tag);
    break;
  case glooDataType_t::glooInt32:
    send<int32_t>(context, sendbuf, size, peer, tag);
    break;
  case glooDataType_t::glooUint32:
    send<uint32_t>(context, sendbuf, size, peer, tag);
    break;
  case glooDataType_t::glooInt64:
    send<int64_t>(context, sendbuf, size, peer, tag);
    break;
  case glooDataType_t::glooUint64:
    send<uint64_t>(context, sendbuf, size, peer, tag);
    break;
  case glooDataType_t::glooFloat16:
    send<gloo::float16>(context, sendbuf, size, peer, tag);
    break;
  case glooDataType_t::glooFloat32:
    send<float_t>(context, sendbuf, size, peer, tag);
    break;
  case glooDataType_t::glooFloat64:
    send<double_t>(context, sendbuf, size, peer, tag);
    break;
  default:
    throw std::runtime_error("Unhandled dataType");
  }
}
} // namespace pygloo