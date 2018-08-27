package org.ray.api.annotation;

public @interface ResourceItem {

  String name() default "";

  double value() default 0;

}
