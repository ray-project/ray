package org.ray.streaming.runtime.serialization;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


import org.apache.commons.lang3.builder.EqualsBuilder;
import org.ray.streaming.message.KeyRecord;
import org.ray.streaming.message.Record;
import org.testng.annotations.Test;

public class CrossLangSerializerTest {

  @Test
  @SuppressWarnings("unchecked")
  public void testSerialize() {
    CrossLangSerializer serializer = new CrossLangSerializer();
    Record record = new Record("value");
    record.setStream("stream1");
    assertTrue(EqualsBuilder.reflectionEquals(record,
      serializer.deserialize(serializer.serialize(record))));
    KeyRecord keyRecord = new KeyRecord("key", "value");
    keyRecord.setStream("stream2");
    assertEquals(keyRecord,
      serializer.deserialize(serializer.serialize(keyRecord)));
  }
}