package io.ray.api.type;

import static org.testng.Assert.assertEquals;

import java.util.Map;
import org.testng.annotations.Test;

public class TypeTest {

  @Test
  public void testCapture() {
    assertEquals(new Type<Map<String, Integer>>(){}.capture().getTypeName(),
        "java.util.Map<java.lang.String, java.lang.Integer>");
  }
}