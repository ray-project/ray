package org.ray.runtime;

import org.ray.api.RayActor;
import org.ray.runtime.generated.Common.Language;

public abstract class AbstractRayActor implements RayActor {

  public abstract Language getLanguage();
}
