#include <collective.h>
// #include <gloo/send.h>

namespace pygloo {

template <typename T>
void recv(const std::shared_ptr<gloo::Context> &context, intptr_t recvbuf,
            size_t size, int peer) {

  T *output_ptr = reinterpret_cast<T *>(recvbuf);

  auto outputBuffer = context->createUnboundBuffer(output_ptr, size * sizeof(T));

  // outputBuffer->recv(peer, 0, 0, size * sizeof(T));
  outputBuffer->recv(peer, peer);
  outputBuffer->waitRecv(context->getTimeout());
}

void recv_wrapper(const std::shared_ptr<gloo::Context> &context,
                    intptr_t recvbuf, size_t size,
                    glooDataType_t datatype, int peer) {
  switch (datatype) {
  case glooDataType_t::glooInt8:
    recv<int8_t>(context, recvbuf, size, peer);
    break;
  case glooDataType_t::glooUint8:
    recv<uint8_t>(context, recvbuf, size, peer);
    break;
  case glooDataType_t::glooInt32:
    recv<int32_t>(context, recvbuf, size, peer);
    break;
  case glooDataType_t::glooUint32:
    recv<uint32_t>(context, recvbuf, size, peer);
    break;
  case glooDataType_t::glooInt64:
    recv<int64_t>(context, recvbuf, size, peer);
    break;
  case glooDataType_t::glooUint64:
    recv<uint64_t>(context, recvbuf, size, peer);
    break;
  case glooDataType_t::glooFloat16:
    recv<gloo::float16>(context, recvbuf, size, peer);
    break;
  case glooDataType_t::glooFloat32:
    recv<float_t>(context, recvbuf, size, peer);
    break;
  case glooDataType_t::glooFloat64:
    recv<double_t>(context, recvbuf, size, peer);
    break;
  default:
    throw std::runtime_error("Unhandled dataType");
  }
}

} // pygloo