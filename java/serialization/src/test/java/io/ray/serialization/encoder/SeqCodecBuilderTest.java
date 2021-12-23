package io.ray.serialization.encoder;

import io.ray.serialization.Fury;
import io.ray.serialization.bean.BeanA;
import io.ray.serialization.bean.BeanB;
import io.ray.serialization.bean.Foo;
import io.ray.serialization.util.Descriptor;
import io.ray.serialization.util.Tuple2;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.testng.Assert;
import org.testng.annotations.Test;

public class SeqCodecBuilderTest {

  @Test
  public void genCode() {
    Fury fury = Fury.builder().withReferenceTracking(false).build();
    new SeqCodecBuilder(Foo.class, fury).genCode();
    // System.out.println(code);
    new SeqCodecBuilder(BeanA.class, fury).genCode();
    new SeqCodecBuilder(BeanB.class, fury).genCode();
  }

  public static class A {
    Integer f1;
    Integer f2;
    Long f3;
    int f4;
    int f5;
  }

  @Test
  public void testBuildDescriptors() {
    Tuple2<List<Descriptor>, List<Descriptor>> descriptors =
        SeqCodecBuilder.buildDescriptors(A.class);
    List<Class<?>> primitives =
        descriptors.f0.stream()
            .map(d -> d.getTypeToken().getRawType())
            .collect(Collectors.toList());
    List<Class<?>> noPrimitives =
        descriptors.f1.stream()
            .map(d -> d.getTypeToken().getRawType())
            .collect(Collectors.toList());
    Assert.assertEquals(Arrays.asList(Long.class, Integer.class, Integer.class), noPrimitives);
    Assert.assertEquals(Arrays.asList(int.class, int.class), primitives);
  }
}
