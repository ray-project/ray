package org.ray.api.test;

import org.apache.commons.lang3.SerializationUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.ray.util.RemoteFunction;

@RunWith(MyRunner.class)
public class RemoteLambdaTest {

  public static <T> String RemoteToString(T o) {
    return o.toString();
  }

  @Test
  public void test() {

    RemoteFunction<String, String> f0 = RemoteLambdaTest::RemoteToString;
    byte[] bytes = SerializationUtils.serialize(f0);
    //System.out.println(new String(bytes));
    //Object m = SerializationUtils.deserialize(bytes);

    RemoteFunction<Integer, Integer> f = x ->
    {
      System.out.println("remote function " + x);
      return x + 1;
    };

    RemoteFunction<Integer, Integer> f2 = SerializationUtils.clone(f);
    Assert.assertEquals(101, (int) f2.apply(100));

    Integer y = 100;
    RemoteFunction<Integer, Integer> f3 = x ->
    {
      System.out.println("remote function " + x);
      return x + y + 1;
    };
    RemoteFunction<Integer, Integer> f4 = SerializationUtils.clone(f3);
    Assert.assertEquals(201, (int) f4.apply(100));
    Assert.assertEquals(201, (int) f4.apply(100));
  }
}
