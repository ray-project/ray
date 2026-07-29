package io.ray.serve.util;

import com.google.protobuf.InvalidProtocolBufferException;

@FunctionalInterface
public interface ProtobufBytesParser<T> {

  T parse(byte[] bytes) throws InvalidProtocolBufferException;
}
