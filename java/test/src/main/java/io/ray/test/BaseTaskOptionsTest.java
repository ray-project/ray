package io.ray.test;

import com.google.common.collect.ImmutableMap;
import io.ray.api.options.BaseTaskOptions;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;

public class BaseTaskOptionsTest {

  public static class MockActorCreationOptions extends BaseTaskOptions {
    public MockActorCreationOptions(Map<String, Double> resources) {
      super(resources);
    }
  }

  @Test
  public void testLegalResources() {
    Map<String, Double> resources =
        ImmutableMap.of("CPU", 0.5, "GPU", 3.0, "memory", 1024.0, "A", 4294967296.0);
    new MockActorCreationOptions(resources);
  }

  @Test(expectedExceptions = {IllegalArgumentException.class})
  public void testIllegalResourcesWithNullValue() {
    Map<String, Double> resources = new HashMap<>();
    resources.put("CPU", null);
    new MockActorCreationOptions(resources);
  }

  @Test
  public void testIllegalResourcesWithZeroValue() {
    Map<String, Double> resources = ImmutableMap.of("CPU", 0.0);
    new MockActorCreationOptions(resources);
  }

  @Test(expectedExceptions = {IllegalArgumentException.class})
  public void testIllegalResourcesWithNegativeValue() {
    Map<String, Double> resources = ImmutableMap.of("CPU", -1.0);
    new MockActorCreationOptions(resources);
  }

  @Test(expectedExceptions = {IllegalArgumentException.class})
  public void testIllegalResourcesWithNonIntegerValue() {
    Map<String, Double> resources = ImmutableMap.of("CPU", 3.5);
    new MockActorCreationOptions(resources);
  }
}
