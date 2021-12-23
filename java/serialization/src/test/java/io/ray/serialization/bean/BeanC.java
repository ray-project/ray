package io.ray.serialization.bean;

import java.util.Arrays;
import lombok.Data;

@Data
public class BeanC {
  private int f1;
  private double f2;
  private Object f3;
  private Object[] f4;

  public static BeanC create() {
    BeanC obj = new BeanC();
    obj.f1 = 1;
    obj.f2 = 1.0d;
    obj.f3 = Arrays.asList("str", "str");
    obj.f4 = new Object[] {"str", "str"};
    return obj;
  }
}
