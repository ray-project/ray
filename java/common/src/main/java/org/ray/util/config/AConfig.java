package org.ray.util.config;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotate a field as a ray configuration item.
 */
@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface AConfig {

  /**
   * comments for this configuration field
   */
  String comment();

  /**
   * when the config is an array list, a splitter set is specified, e.g., " \t" to use ' ' and '\t'
   * as possible splits
   */
  String splitters() default ", \t";

  /**
   * indirect with value as the new section name, the field name remains the same
   */
  String defaultIndirectSectionName() default "";

  /**
   * see ConfigReader.getIndirectStringArray this config tells which is the default
   * indirectSectionName in that function
   */
  String defaultArrayIndirectSectionName() default "";
}
