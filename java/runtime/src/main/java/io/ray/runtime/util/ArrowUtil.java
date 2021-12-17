package io.ray.runtime.util;

import io.ray.runtime.exception.RayException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;

public class ArrowUtil {
  public static final RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE);

  public static VectorSchemaRoot deserialize(byte[] data) {
    try (ArrowStreamReader reader =
        new ArrowStreamReader(new ByteArrayInputStream(data), rootAllocator)) {
      reader.loadNextBatch();
      return reader.getVectorSchemaRoot();
    } catch (Exception e) {
      throw new RayException("Failed to deserialize Arrow data", e.getCause());
    }
  }

  public static byte[] serialize(VectorSchemaRoot root) {
    try {
      ByteArrayOutputStream sink = new ByteArrayOutputStream();
      ArrowStreamWriter writer = new ArrowStreamWriter(root, null, sink);
      writer.start();
      writer.writeBatch();
      writer.end();
      writer.close();
      return sink.toByteArray();
    } catch (Exception e) {
      throw new RayException("Failed to serialize Arrow data", e.getCause());
    }
  }
}
