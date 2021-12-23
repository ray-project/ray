package io.ray.serialization.util;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.google.common.hash.Hashing;
import io.ray.serialization.Fury;
import io.ray.serialization.bean.BeanA;
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

}
