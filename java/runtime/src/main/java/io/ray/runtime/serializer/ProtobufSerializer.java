package io.ray.runtime.serializer;

import com.google.common.base.Preconditions;
import com.google.protobuf.AbstractMessage;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.InvalidProtocolBufferException;
import io.ray.runtime.generated.Serialization;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.ExtensionValue;
import org.msgpack.value.Value;

import java.lang.reflect.Method;

public class ProtobufSerializer {
  public static boolean isProtobufObject(Object obj) {
    Class cls = obj.getClass();
    while (cls != null || !cls.getName().equals("java.lang.Object")) {
      if (cls.getName().equals("com.google.protobuf.AbstractMessage")) {
        return true;
      }
      cls = cls.getSuperclass();
    }
    return false;
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

    System.out.println(protoWrapper);
    return protoWrapper.toByteArray();
  }

  private static final int MESSAGE_PACK_OFFSET = 9;

  public static <T> T decode(byte[] bs, Class<?> type) {
    try {
      Serialization.ProtobufObject protoWrapper = Serialization.ProtobufObject.parseFrom(bs);
      Method method = type.getDeclaredMethod("parseFrom", byte[].class);
      Object parsedObject = method.invoke(null, protoWrapper.getSerializedData().toByteArray());
      return (T) parsedObject;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
