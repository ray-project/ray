package org.ray.spi.impl;

import com.google.flatbuffers.FlatBufferBuilder;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

class NativeRedisClient {
  NativeRedisClient(String redisAddress) {

  }
  native void Connect(String redisAddress);
  native void execute_command();
}