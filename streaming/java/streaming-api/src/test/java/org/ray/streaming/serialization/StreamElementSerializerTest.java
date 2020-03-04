package org.ray.streaming.serialization;

import static org.testng.Assert.*;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.ray.streaming.message.KeyRecord;
import org.ray.streaming.message.Record;
import org.testng.annotations.Test;

public class StreamElementSerializerTest {

  @Test
  @SuppressWarnings("unchecked")
  public void testSerialize() {
    StreamElementSerializer serializer = new StreamElementSerializer();
    Record record = new Record(new byte[2]);
    record.setStream("stream1");
    assertTrue(EqualsBuilder.reflectionEquals(record,
      serializer.deserialize(serializer.serialize(record))));
    KeyRecord keyRecord = new KeyRecord(new byte [2], new byte[2]);
    keyRecord.setStream("stream2");
    assertTrue(EqualsBuilder.reflectionEquals(keyRecord,
      serializer.deserialize(serializer.serialize(keyRecord))));
  }
}