package io.ray.serialization.util;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

import com.google.common.reflect.TypeToken;
import io.ray.serialization.bean.BeanA;
import java.util.List;
import java.util.Map;
import org.testng.annotations.Test;

public class ReflectionUtilsTest {

  @Test
  public void getReturnType() {
    assertEquals(
        ReflectionUtils.getReturnType(ReflectionUtils.class, "getReturnType"), Class.class);
    assertFalse(ReflectionUtils.hasException(List.class, "getClass"));
  }

  @Test
  public void getTypeArguments() {
    TypeToken<Tuple2<String, Map<String, Integer>>> typeToken =
        new TypeToken<Tuple2<String, Map<String, Integer>>>() {};
    assertEquals(ReflectionUtils.getTypeArguments(typeToken).size(), 2);
  }

  @Test
  public void getAllTypeArguments() {
    TypeToken<Tuple2<String, Map<String, BeanA>>> typeToken =
        new TypeToken<Tuple2<String, Map<String, BeanA>>>() {};
    List<TypeToken<?>> allTypeArguments = ReflectionUtils.getAllTypeArguments(typeToken);
    assertEquals(allTypeArguments.size(), 3);
    assertEquals(allTypeArguments.get(2).getRawType(), BeanA.class);
  }

  class A {
    String f1;
  }

  @Test
  public void testGetClassNameWithoutPackage() {
    assertEquals(
        ReflectionUtils.getClassNameWithoutPackage(A.class),
        ReflectionUtilsTest.class.getSimpleName() + "$" + A.class.getSimpleName());
  }
}
