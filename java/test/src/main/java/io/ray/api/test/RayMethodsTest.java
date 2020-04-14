package io.ray.api.test;

import com.google.common.collect.ImmutableList;
import io.ray.api.Ray;
import io.ray.api.RayObject;
import io.ray.api.WaitResult;
import java.util.List;
import java.util.stream.Collectors;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Integration test for Ray.*
 */
public class RayMethodsTest extends BaseTest {

  @Test
  public void test() {
    RayObject<Integer> i1Id = Ray.put(1);
    RayObject<Double> f1Id = Ray.put(3.14);
    RayObject<String> s1Id = Ray.put(String.valueOf("Hello "));
    RayObject<String> s2Id = Ray.put(String.valueOf("World!"));
    RayObject<Object> n1Id = Ray.put(null);

    WaitResult<String> res = Ray.wait(ImmutableList.of(s1Id, s2Id), 2, 1000);

    List<String> ss = res.getReady().stream().map(RayObject::get).collect(Collectors.toList());
    int i1 = i1Id.get();
    double f1 = f1Id.get();
    Object n1 = n1Id.get();

    Assert.assertEquals("Hello World!", ss.get(0) + ss.get(1));
    Assert.assertEquals(1, i1);
    Assert.assertEquals(3.14, f1, Double.MIN_NORMAL);
    Assert.assertNull(n1);

  }
}
