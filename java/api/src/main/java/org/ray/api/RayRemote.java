package org.ray.api;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.ray.util.ResourceItem;

/**
 * a ray remote function or class (as an actor).
 */
@Target({ElementType.METHOD, ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface RayRemote {
  /**
   * This is used for default resources.
   * @return The resources of the method or actor need.
   */
  ResourceItem[] resources() default {@ResourceItem()};
}
