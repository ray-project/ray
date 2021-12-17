package io.ray.runtime.serializer;

import com.google.common.base.Preconditions;
import com.google.protobuf.AbstractMessage;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.InvalidProtocolBufferException;
import io.ray.runtime.RayNativeRuntime;
import io.ray.runtime.generated.RuntimeEnvCommon;
import io.ray.runtime.generated.Serialization;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.ExtensionValue;
import org.msgpack.value.Value;

import java.lang.reflect.Method;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProtobufSerializer {

  private static final Logger LOGGER = LoggerFactory.getLogger(ProtobufSerializer.class);

  public static boolean isProtobufObject(Object obj) {
    try {
      Class cls = obj.getClass();
      while (cls != null || !cls.getName().equals("java.lang.Object")) {
        if (cls.getName().equals("com.google.protobuf.AbstractMessage")) {
          return true;
        }
        cls = cls.getSuperclass();
      }
      return false;
    } catch (Exception e) {
      return false;
    }
  }

  public static byte[] encode(Object obj) {
    if (!isProtobufObject(obj)) {
      throw new RuntimeException("Please make sure to check object is protobuf ahead of time");
    }

    AbstractMessage protoMessage = (AbstractMessage) obj;
    Descriptors.Descriptor descriptor = protoMessage.getDescriptorForType();
    Descriptors.FileDescriptor fileDescriptor = descriptor.getFile();
    Serialization.ProtobufObject protoWrapper = (
      Serialization.ProtobufObject.newBuilder()
        .setSerializedData(protoMessage.toByteString())
        .setName(descriptor.getName())
        .setDescriptorName(fileDescriptor.getName())
        .setDescriptorPackage(fileDescriptor.getPackage())
        .setDescriptorSerializePb(fileDescriptor.toProto().toByteString())
    ).build();

    LOGGER.info(protoWrapper.toString());
    return protoWrapper.toByteArray();
  }

  private static final int MESSAGE_PACK_OFFSET = 9;

  public static <T> T decode(byte[] bs, Class<?> type) {
    try {
      LOGGER.info("Java decode protobuf messagepack");
//      LOGGER.info(Arrays.toString(bs));

      // Read MessagePack bytes length.
      MessageUnpacker headerUnpacker = MessagePack.newDefaultUnpacker(bs, 0, MESSAGE_PACK_OFFSET);
      long msgpackBytesLength = headerUnpacker.unpackLong();
      headerUnpacker.close();
      // Check MessagePack bytes length is valid.
      Preconditions.checkState(MESSAGE_PACK_OFFSET + msgpackBytesLength <= bs.length);
      // Deserialize MessagePack bytes from MESSAGE_PACK_OFFSET.
      MessageUnpacker unpacker =
        MessagePack.newDefaultUnpacker(bs, MESSAGE_PACK_OFFSET, (int) msgpackBytesLength);
      Value v = unpacker.unpackValue();
      if (type == null) {
        type = Object.class;
      }

      LOGGER.info("ValueType");
      LOGGER.info(v.getValueType().toString());

      Serialization.ProtobufObject protoWrapper = Serialization.ProtobufObject.parseFrom(v.asBinaryValue().asByteArray());
      LOGGER.info("Got protoWrapper " + protoWrapper);
      Method method = type.getDeclaredMethod("parseFrom", byte[].class);
      Object parsedObject = method.invoke(null, protoWrapper.getSerializedData().toByteArray());
      LOGGER.info("Parsed user protobuf");
      return (T) parsedObject;
    } catch (Exception e) {
      LOGGER.info(e.toString());
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  public static class TestActor {
    public String returnWorkingDir(RuntimeEnvCommon.RuntimeEnv envProto) {
      return envProto.getWorkingDir();
    }
  }
}
