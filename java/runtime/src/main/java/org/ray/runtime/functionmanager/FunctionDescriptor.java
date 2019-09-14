package org.ray.runtime.functionmanager;

import java.util.List;
import org.ray.runtime.generated.Common.Language;

/**
 * Base interface of a Ray task's function descriptor.
 *
 * A function descriptor is a list of strings that can uniquely describe a function. It's used to
 * load a function in workers.
 */
public interface FunctionDescriptor {

  /**
   * @return A list of strings represents the functions.
   */
  List<String> toList();

  /**
   * @return The language of the function.
   */
  Language getLanguage();
}
