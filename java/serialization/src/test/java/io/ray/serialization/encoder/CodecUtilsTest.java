package io.ray.serialization.encoder;

import static org.testng.Assert.assertEquals;

import io.ray.serialization.Fury;
import io.ray.serialization.bean.BeanA;
import io.ray.serialization.util.MemoryBuffer;
import io.ray.serialization.util.MemoryUtils;
import org.testng.annotations.Test;

public class CodecUtilsTest {

  @SuppressWarnings("unchecked")
  @Test
  public void loadOrGenSeqCodecClass() throws Exception {
    Fury fury = Fury.builder().withReferenceTracking(false).build();
    Class<?> seqCodecClass = CodecUtils.loadOrGenSeqCodecClass(BeanA.class, fury);
    Generated.GeneratedSerializer seqCodec =
        seqCodecClass
            .asSubclass(Generated.GeneratedSerializer.class)
            .getConstructor(Fury.class, Class.class)
            .newInstance(fury, BeanA.class);
    MemoryBuffer buffer = MemoryUtils.buffer(32);
    BeanA beanA = BeanA.createBeanA(2);
    seqCodec.write(fury, buffer, beanA);
    byte[] bytes = buffer.getBytes(0, buffer.writerIndex());
    Object obj = seqCodec.read(fury, MemoryUtils.wrap(bytes), BeanA.class);
    assertEquals(obj, beanA);
  }
}
