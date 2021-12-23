package io.ray.serialization.util;

import com.google.common.reflect.TypeToken;
import io.ray.serialization.bean.BeanA;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.testng.annotations.Test;

public class DescriptorTest {

  @Test
  public void getDescriptorsTest() throws IntrospectionException {
    Class<?> clz = BeanA.class;
    TypeToken<?> typeToken = TypeToken.of(clz);
    // sort to fix field order
    List<?> descriptors =
        Arrays.stream(Introspector.getBeanInfo(clz).getPropertyDescriptors())
            .filter(d -> !d.getName().equals("class"))
            .filter(d -> !d.getName().equals("declaringClass"))
            .filter(d -> d.getReadMethod() != null && d.getWriteMethod() != null)
            .map(
                p -> {
                  TypeToken<?> returnType = typeToken.method(p.getReadMethod()).getReturnType();
                  return Arrays.<Object>asList(
                      p.getName(),
                      returnType,
                      p.getReadMethod().getName(),
                      p.getWriteMethod().getName());
                })
            .collect(Collectors.toList());

    Descriptor.getDescriptors(clz);
  }
}
