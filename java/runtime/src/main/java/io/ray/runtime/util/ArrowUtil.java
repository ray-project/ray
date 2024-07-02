package io.ray.runtime.util;

import io.ray.api.exception.RayException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.channels.Channels;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.MessageChannelReader;
import org.apache.arrow.vector.ipc.message.MessageResult;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Schema;

/** Helper method for serialize and deserialize arrow data. */
public class ArrowUtil {
  public static final RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE);

  /**
   * Deserialize data in byte array to arrow data format.
   *
   * @return The vector schema root of arrow.
   */
  public static VectorSchemaRoot deserialize(byte[] data) {
    try (MessageChannelReader reader =
        new MessageChannelReader(
            new ReadChannel(Channels.newChannel(new ByteArrayInputStream(data))), rootAllocator)) {
      MessageResult result = reader.readNext();
      Schema schema = MessageSerializer.deserializeSchema(result.getMessage());
      VectorSchemaRoot root = VectorSchemaRoot.create(schema, rootAllocator);
      VectorLoader loader = new VectorLoader(root);
      result = reader.readNext();
      ArrowRecordBatch batch =
          MessageSerializer.deserializeRecordBatch(result.getMessage(), result.getBodyBuffer());
      loader.load(batch);
      return root;
    } catch (Exception e) {
      throw new RayException("Failed to deserialize Arrow data", e.getCause());
    }
  }

  /**
   * Serialize data from arrow data format to byte array.
   *
   * @return The byte array of data.
   */
  public static byte[] serialize(VectorSchemaRoot root) {
    try (ByteArrayOutputStream sink = new ByteArrayOutputStream();
        ArrowStreamWriter writer = new ArrowStreamWriter(root, null, sink)) {
      writer.start();
      writer.writeBatch();
      writer.end();
      return sink.toByteArray();
    } catch (Exception e) {
      throw new RayException("Failed to serialize Arrow data", e.getCause());
    }
  }
}
