package org.ray.api.test;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.ray.api.Ray;
import org.ray.api.RayObject;
import org.ray.api.WaitResult;
import org.ray.util.logger.RayLog;

/**
 * Integration test for Ray.*
 */
@RunWith(MyRunner.class)
public class RayMethodsTest {

  @Test
  public void test() {
    RayObject<Integer> i1Id = Ray.put(1);
    RayObject<Double> f1Id = Ray.put(3.14);
    RayObject<String> s1Id = Ray.put(String.valueOf("Hello "));
    RayObject<String> s2Id = Ray.put(String.valueOf("World!"));
    RayObject<Object> n1Id = Ray.put(null);

    WaitResult<String> res = Ray.wait(ImmutableList.of(s1Id, s2Id), 2);

    List<String> ss = res.getReadyOnes().stream().map(RayObject::get).collect(Collectors.toList());
    int i1 = i1Id.get();
    double f1 = f1Id.get();
    Object n1 = n1Id.get();

    RayLog.rapp.info("Strings: " + ss.get(0) + ss.get(1) + " int: " + i1 + " double: " + f1
        + " null: " + n1);
    Assert.assertEquals("Hello World!", ss.get(0) + ss.get(1));
    Assert.assertEquals(1, i1);
    Assert.assertEquals(3.14, f1, Double.MIN_NORMAL);
    Assert.assertNull(n1);

    // metadata test
    RayObject<Integer> vid = Ray.put(643, "test metadata");
    Integer v = vid.get();
    String m = vid.getMeta();

    Assert.assertEquals(643L, v.longValue());
    Assert.assertEquals("test metadata", m);
  }
}
