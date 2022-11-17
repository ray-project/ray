package io.ray.serve.util;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ReflectUtilTest {

  static class ReflectExample {
    public ReflectExample() {}

    public ReflectExample(Integer a) {}

    public ReflectExample(String a) {}

    public void test(String a) {}
  }

  @SuppressWarnings("unchecked")
  @Test
  public void getConstructorTest() throws NoSuchMethodException {

    Constructor<ReflectExample> constructor = ReflectUtil.getConstructor(ReflectExample.class);
    Assert.assertNotNull(constructor);

    constructor = ReflectUtil.getConstructor(ReflectExample.class);
    Assert.assertNotNull(constructor);

    constructor = ReflectUtil.getConstructor(ReflectExample.class, 2);
    Assert.assertNotNull(constructor);

    constructor = ReflectUtil.getConstructor(ReflectExample.class, "");
    Assert.assertNotNull(constructor);

    Assert.assertThrows(
        NoSuchMethodException.class,
        () -> ReflectUtil.getConstructor(ReflectExample.class, new HashMap<>()));
  }

  @Test
  public void getMethodTest() throws NoSuchMethodException {

    Method method = ReflectUtil.getMethod(ReflectExample.class, "test", "");
    Assert.assertNotNull(method);

    Assert.assertThrows(
        NoSuchMethodException.class,
        () -> ReflectUtil.getMethod(ReflectExample.class, "test", new HashMap<>()));
  }

  @Test
  public void getMethodStringsTest() {
    List<String> methodList = ReflectUtil.getMethodStrings(ReflectExample.class);
    String result = null;
    for (String method : methodList) {
      if (StringUtils.contains(method, "test")) {
        result = method;
      }
    }
    Assert.assertNotNull(result, "there should be test method");
  }
}
