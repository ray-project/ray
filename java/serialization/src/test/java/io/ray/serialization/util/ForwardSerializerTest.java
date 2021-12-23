package io.ray.serialization.util;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertTrue;

import com.google.common.hash.Hashing;
import io.ray.serialization.Fury;
import io.ray.serialization.bean.BeanA;
import io.ray.serialization.codegen.CompileUnit;
import io.ray.serialization.codegen.JaninoUtils;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ForwardSerializerTest {
  public static void main(String[] args) {
    int i = new Random().nextInt(Integer.MAX_VALUE);
    System.out.println(i);
    System.out.println(Hashing.murmur3_32().hashInt(i).asInt());
    System.out.println(Integer.toHexString(i));
    System.out.println(Integer.toHexString(Hashing.murmur3_32().hashInt(i).asInt()));
  }

  private ForwardSerializer createSerializer() {
    return new ForwardSerializer(
        new ForwardSerializer.DefaultFuryProxy() {
          @Override
          protected Fury newFurySerializer(ClassLoader loader) {
            Fury fury = super.newFurySerializer(loader);
            // We can register custom serializers here.
            System.out.printf("Created serializer %s, start to do init staff.\n", fury);
            return fury;
          }
        });
  }

  @Test
  public void testSerialize() {
    BeanA beanA = BeanA.createBeanA(3);
    ForwardSerializer fury = createSerializer();
    Assert.assertEquals(fury.deserialize(fury.serialize(beanA)), beanA);
  }

  private volatile boolean hasException;

  @Test
  public void testConcurrent() throws InterruptedException {
    BeanA beanA = BeanA.createBeanA(3);
    ForwardSerializer serializer = createSerializer();
    serializer.register(BeanA.class);
    Assert.assertEquals(serializer.deserialize(serializer.serialize(beanA)), beanA);
    ExecutorService executorService = Executors.newFixedThreadPool(12);
    for (int i = 0; i < 1000; i++) {
      executorService.execute(
          () -> {
            for (int j = 0; j < 10; j++) {
              try {
                assertEquals(serializer.deserialize(serializer.serialize(beanA)), beanA);
              } catch (Exception e) {
                hasException = true;
                e.printStackTrace();
                throw e;
              }
            }
          });
    }
    executorService.shutdown();
    assertTrue(executorService.awaitTermination(30, TimeUnit.SECONDS));
    assertFalse(hasException);
  }

  @Test
  public void testClassLoader() throws Exception {
    ForwardSerializer serializer = createSerializer();
    CompileUnit unit =
        new CompileUnit(
            "demo.pkg1",
            "A",
            (""
                + "package demo.pkg1;\n"
                + "public class A {\n"
                + "  public String f1 = \"str1\";\n"
                + "  public String f2 = \"str2\";\n"
                + "}"));
    ClassLoader loader = null;
    for (int i = 0; i < 5; i++) {
      ClassLoader newLoader = JaninoUtils.compile(getClass().getClassLoader(), unit);
      assertNotSame(loader, newLoader);
      assertNotEquals(loader, newLoader);
      loader = newLoader;
      Class<?> clz = loader.loadClass("demo.pkg1.A");
      Object a = clz.newInstance();
      serializer.setClassLoader(loader);
      serializer.deserialize(serializer.serialize(a));
    }
  }
}
