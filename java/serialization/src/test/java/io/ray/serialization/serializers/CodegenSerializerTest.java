package io.ray.serialization.serializers;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import io.ray.serialization.Fury;
import io.ray.serialization.bean.Cyclic;
import io.ray.serialization.util.MemoryBuffer;
import io.ray.serialization.util.MemoryUtils;
import org.testng.annotations.Test;

public class CodegenSerializerTest {

  @Test
  public void testSupport() {
    Fury fury = Fury.builder().withReferenceTracking(true).build();
    assertTrue(CodegenSerializer.support(fury, Cyclic.class));
  }

  @Test
  public void testSerializeCircularReference() {
    Cyclic cyclic = Cyclic.create(true);
    Fury fury = Fury.builder().withReferenceTracking(true).build();
    MemoryBuffer buffer = MemoryUtils.buffer(32);

    fury.serialize(buffer, cyclic);
    fury.deserialize(buffer);

    Serializer<Cyclic> beanSerializer = fury.getClassResolver().getTypedSerializer(Cyclic.class);
    fury.getReferenceResolver().writeReferenceOrNull(buffer, cyclic);
    beanSerializer.write(fury, buffer, cyclic);
    fury.getReferenceResolver().readReferenceOrNull(buffer);
    fury.getReferenceResolver().preserveReferenceId();
    Cyclic cyclic1 = beanSerializer.read(fury, buffer, Cyclic.class);
    fury.reset();
    assertEquals(cyclic1, cyclic);
  }
}
