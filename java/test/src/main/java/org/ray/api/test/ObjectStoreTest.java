package org.ray.api.test;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.stream.Collectors;
import org.ray.api.Ray;
import org.ray.api.RayObject;
import org.ray.api.id.ObjectId;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Test putting and getting objects.
 */
public class ObjectStoreTest extends BaseTest {

  @Test
  public void testPutAndGet() {
    RayObject<Integer> obj = Ray.put(1);
    Assert.assertEquals(1, (int) obj.get());
  }

  @Test
  public void testGetMultipleObjects() {
    List<Integer> ints = ImmutableList.of(1, 2, 3, 4, 5);
    List<ObjectId> ids = ints.stream().map(obj -> Ray.put(obj).getId())
        .collect(Collectors.toList());
    Assert.assertEquals(ints, Ray.get(ids));
  }
}
