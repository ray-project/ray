package io.ray.api.concurrencygroup.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.TYPE)
@Inherited
@Repeatable(DefConcurrencyGroups.class)
@Retention(RetentionPolicy.RUNTIME)
public @interface DefConcurrencyGroup {

  public String name() default "";

  public int maxConcurrency() default 1;
}
