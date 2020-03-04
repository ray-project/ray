package org.ray.streaming.serialization;

import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.List;
import org.ray.streaming.message.KeyRecord;
import org.ray.streaming.message.Record;

public class StreamElementSerializer {
  private static final byte RECORD_TYPE_ID = 0;
  private static final byte KEY_RECORD_TYPE_ID = 1;

  private MsgPackSerializer msgPackSerializer = new MsgPackSerializer();

  public byte[] serialize(Record record) {
    Preconditions.checkArgument(record.getValue().getClass() == byte[].class,
      "Only bytes is supported");
    byte[] value = (byte[]) record.getValue();
    Class<? extends Record> clz = record.getClass();
    if (clz == Record.class) {
      return msgPackSerializer.serialize(Arrays.asList(
        RECORD_TYPE_ID, record.getStream(), value));
    } else if (clz == KeyRecord.class) {
      KeyRecord keyRecord = (KeyRecord) record;
      Object key = keyRecord.getKey();
      Preconditions.checkArgument(key.getClass() == byte[].class,
        "Only bytes is supported");
      return msgPackSerializer.serialize(Arrays.asList(
        KEY_RECORD_TYPE_ID, keyRecord.getStream(), key, value));
    } else {
      throw new UnsupportedOperationException(
        String.format("Serialize %s is unsupported.", record));
    }
  }

  @SuppressWarnings("unchecked")
  public Record deserialize(byte[] bytes) {
    List list = (List) msgPackSerializer.deserialize(bytes);
    Byte typeId = (Byte) list.get(0);
    switch (typeId) {
      case RECORD_TYPE_ID: {
        String stream = (String) list.get(1);
        Object value = list.get(2);
        Record record = new Record(value);
        record.setStream(stream);
        return record;
      }
      case KEY_RECORD_TYPE_ID: {
        String stream = (String) list.get(1);
        Object key = list.get(2);
        Object value = list.get(3);
        KeyRecord keyRecord = new KeyRecord(key, value);
        keyRecord.setStream(stream);
        return keyRecord;
      }
      default:
        throw new UnsupportedOperationException("Unsupported type " + typeId);

    }
  }

}
