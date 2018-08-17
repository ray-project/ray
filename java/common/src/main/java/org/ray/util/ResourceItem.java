package org.ray.util;

public @interface ResourceItem {
  public String name() default "";
  public double value() default 0;

}
