package org.ray.api.test;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.ray.api.Ray;
import org.ray.api.RayObject;
import org.ray.api.id.UniqueId;

/**
 * Test putting and getting objects.
 */
@RunWith(MyRunner.class)
public class ObjectStoreTest {

  @Test
  public void testPutAndGet() {
    RayObject<Integer> obj = Ray.put(1);
    Assert.assertEquals(1, (int) obj.get());
  }

  @Test
  public void testGetMultipleObjects() {
    List<Integer> ints = ImmutableList.of(1, 2, 3, 4, 5);
    List<UniqueId> ids = ints.stream().map(obj -> Ray.put(obj).getId())
        .collect(Collectors.toList());
    Assert.assertEquals(ints, Ray.get(ids));
  }
}
