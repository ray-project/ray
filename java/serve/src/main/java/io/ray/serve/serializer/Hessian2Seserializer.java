package io.ray.serve.serializer;

import com.caucho.hessian.io.Hessian2Input;
import com.caucho.hessian.io.Hessian2Output;
import com.caucho.hessian.io.SerializerFactory;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class Hessian2Seserializer {

  private static SerializerFactory DEFAULT_FACTORY = new SerializerFactory();

  public static byte[] encode(Object data) throws IOException {
    return encode(data, DEFAULT_FACTORY);
  }

  public static byte[] encode(Object data, SerializerFactory serializerFactory) throws IOException {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    Hessian2Output hessian2Output = new Hessian2Output(byteArrayOutputStream);
    hessian2Output.setSerializerFactory(serializerFactory);
    hessian2Output.writeObject(data);
    hessian2Output.flush();
    return byteArrayOutputStream.toByteArray();
  }

  public static Object decode(byte[] data) throws IOException {
    return decode(data, DEFAULT_FACTORY);
  }

  public static Object decode(byte[] data, SerializerFactory serializerFactory) throws IOException {
    ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(data);
    Hessian2Input hessian2Input = new Hessian2Input(byteArrayInputStream);
    hessian2Input.setSerializerFactory(serializerFactory);
    return hessian2Input.readObject();
  }
}
