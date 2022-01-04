package io.ray.runtime.serializer;

import com.google.common.base.Preconditions;
import com.google.protobuf.AbstractMessage;
import com.google.protobuf.Descriptors;
import io.ray.runtime.generated.Serialization;
import java.lang.reflect.Method;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProtobufSerializer {

  private static final Logger LOGGER = LoggerFactory.getLogger(ProtobufSerializer.class);

  public static byte[] encode(Object obj) {
    Preconditions.checkState(obj instanceof AbstractMessage);

    AbstractMessage protoMessage = (AbstractMessage) obj;
    Descriptors.Descriptor descriptor = protoMessage.getDescriptorForType();
    Descriptors.FileDescriptor fileDescriptor = descriptor.getFile();
    Serialization.ProtobufObject protoWrapper =
        (Serialization.ProtobufObject.newBuilder()
                .setSerializedData(protoMessage.toByteString())
                .setName(descriptor.getName())
                .setDescriptorName(fileDescriptor.getName())
                .setDescriptorPackage(fileDescriptor.getPackage())
                .setDescriptorSerializePb(fileDescriptor.toProto().toByteString()))
            .build();

    return protoWrapper.toByteArray();
  }

  public static <T> T decode(byte[] bs, Class<?> type) {
    try {
      Serialization.ProtobufObject protoWrapper = Serialization.ProtobufObject.parseFrom(bs);
      Method method = type.getDeclaredMethod("parseFrom", byte[].class);
      Object parsedObject = method.invoke(null, protoWrapper.getSerializedData().toByteArray());
      return (T) parsedObject;
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }
}
