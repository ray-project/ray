package io.ray.serialization.util;

import static org.testng.Assert.assertEquals;

import com.google.common.reflect.TypeToken;
import org.testng.annotations.Test;

public class TypeUtilsTest {

  @Test
  public void getArrayTypeTest() {
    assertEquals("int[][][][][]", TypeUtils.getArrayType(int[][][][][].class));
    assertEquals("java.lang.Object[][][][][]", TypeUtils.getArrayType(Object[][][][][].class));
    assertEquals("int[][][][][]", TypeUtils.getArrayType(TypeToken.of(int[][][][][].class)));
    assertEquals(
        "java.lang.Object[][][][][]", TypeUtils.getArrayType(TypeToken.of(Object[][][][][].class)));
  }
}
