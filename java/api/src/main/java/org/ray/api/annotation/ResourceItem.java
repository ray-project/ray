package org.ray.api.annotation;


import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Represents a custom resource, including its name and quantity.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.ANNOTATION_TYPE)
public @interface ResourceItem {

  /**
   * Name of this resource, must not be null or empty.
   */
  String name();

  /**
   * Quantity of this resource.
   */
  double value() default 0;

}
