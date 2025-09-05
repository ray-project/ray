package io.ray.test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

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
    double precision = 0.001;
    Map<String, Double> inputResources =
        ImmutableMap.of("CPU", 0.5, "GPU", 3.0, "memory", 1024.0, "A", 4294967296.0);
    Map<String, Double> resources = new MockActorCreationOptions(inputResources).getResources();

    assertEquals(resources.get("CPU"), 0.5, precision);
    assertEquals(resources.get("GPU"), 3.0, precision);
    assertEquals(resources.get("memory"), 1024.0, precision);
    assertEquals(resources.get("A"), 4294967296.0, precision);
  }

  @Test
  public void testResourcesFiltering() {
    Map<String, Double> inputResources = ImmutableMap.of("CPU", 0.0, "GPU", 0.0);
    Map<String, Double> resources = new MockActorCreationOptions(inputResources).getResources();

    assertTrue(resources.isEmpty());
  }

  @Test
  public void testEmptyResourceMap() {
    Map<String, Double> resources = new HashMap<>();
    MockActorCreationOptions options = new MockActorCreationOptions(resources);
    assertTrue(options.getResources().isEmpty());
  }

  @Test(expectedExceptions = {IllegalArgumentException.class})
  public void testNullResourceMap() {
    new MockActorCreationOptions(null);
  }

  @Test(expectedExceptions = {IllegalArgumentException.class})
  public void testNullResourceKey() {
    Map<String, Double> resources = new HashMap<>();
    resources.put(null, 1.0);
    new MockActorCreationOptions(resources);
  }

  @Test(expectedExceptions = {UnsupportedOperationException.class})
  public void testResourcesImmutability() {
    Map<String, Double> inputResources = new HashMap<>();
    inputResources.put("CPU", 2.0);

    MockActorCreationOptions options = new MockActorCreationOptions(inputResources);
    Map<String, Double> resources = options.getResources();
    resources.put("GPU", 1.0);
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
