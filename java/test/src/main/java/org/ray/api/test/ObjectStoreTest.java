package org.ray.api.test;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.stream.Collectors;
import org.ray.api.Ray;
import org.ray.api.RayObject;
import org.ray.api.id.ObjectId;
import org.ray.runtime.AbstractRayRuntime;
import org.ray.runtime.ObjectInterface;
import org.ray.runtime.RayObjectImpl;
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

  @Test
  public void testPutDuplicateObjectId() {
    ObjectId objectId = ObjectId.randomId();
    RayObject<Integer> obj = new RayObjectImpl<>(objectId);
    ObjectInterface objectInterface =
        ((AbstractRayRuntime) Ray.internal()).getWorker().getObjectInterface();
    objectInterface.put(objectId, 1);
    Assert.assertEquals(1, (int) obj.get());
    // the second put should fail but be ignored.
    objectInterface.put(objectId, 2);
    Assert.assertEquals(1, (int) obj.get());
  }
}
