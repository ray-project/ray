package org.ray.api.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Defines a remote function (when used on a method),
 * or an actor (when used on a class).
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.TYPE})
public @interface RayRemote {

  /**
   * Defines the quantity of various custom resources to reserve
   * for this task or for the lifetime of the actor.
   * @return an array of custom resource items.
   */
  ResourceItem[] resources() default {};
}
