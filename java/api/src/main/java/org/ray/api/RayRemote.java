package org.ray.api;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * a ray remote function or class (as an actor).
 */
@Target({ElementType.METHOD, ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface RayRemote {

  /**
   *
   * @return whether to use external I/O pool to execute the function.
   */
  boolean externalIo() default false;

  /**
   * This is used for default resources.
   * @return The resources of the method or actor need.
   */
  ResourceItem[] resources() default {@ResourceItem()};
}
