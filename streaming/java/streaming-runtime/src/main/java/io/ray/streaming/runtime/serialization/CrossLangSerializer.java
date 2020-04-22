package io.ray.streaming.runtime.serialization;

import io.ray.streaming.message.KeyRecord;
import io.ray.streaming.message.Record;
import java.util.Arrays;
import java.util.List;

/**
 * A serializer for cross-lang serialization between java/python.
 * TODO implements a more sophisticated serialization framework
 */
public class CrossLangSerializer implements Serializer {
  private static final byte RECORD_TYPE_ID = 0;
  private static final byte KEY_RECORD_TYPE_ID = 1;

  private MsgPackSerializer msgPackSerializer = new MsgPackSerializer();

  public byte[] serialize(Object object) {
    Record record = (Record) object;
    Object value = record.getValue();
    Class<? extends Record> clz = record.getClass();
    if (clz == Record.class) {
      return msgPackSerializer.serialize(Arrays.asList(
          RECORD_TYPE_ID, record.getStream(), value));
    } else if (clz == KeyRecord.class) {
      KeyRecord keyRecord = (KeyRecord) record;
      Object key = keyRecord.getKey();
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
