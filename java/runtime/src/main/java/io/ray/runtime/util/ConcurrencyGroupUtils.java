package io.ray.runtime.util;
import com.google.common.base.Preconditions;

import io.ray.api.Ray;
import io.ray.api.concurrencygroup.ConcurrencyGroup;
import io.ray.api.concurrencygroup.annotations.DefConcurrencyGroup;
import io.ray.api.concurrencygroup.annotations.DefConcurrencyGroups;
import io.ray.api.concurrencygroup.annotations.UseConcurrencyGroup;
import io.ray.api.function.RayFuncR;
import io.ray.runtime.ConcurrencyGroupImpl;
import io.ray.runtime.RayRuntimeInternal;
import io.ray.runtime.functionmanager.JavaFunctionDescriptor;
import java.lang.invoke.SerializedLambda;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/// TODO: cache this.
public final class ConcurrencyGroupUtils {

  public static List<ConcurrencyGroup> extractConcurrencyGroupsByAnnotations(
    RayFuncR<?> actorConstructorLambda) {
    SerializedLambda serializedLambda = LambdaUtils.getSerializedLambda(actorConstructorLambda);
    Class<?> actorClz = getReturnTypeFromSignature(serializedLambda.getInstantiatedMethodType());

    ArrayList<ConcurrencyGroup> ret = new ArrayList<ConcurrencyGroup>();
    DefConcurrencyGroups concurrencyGroupsDefinitionAnnotation =
      actorClz.getAnnotation(DefConcurrencyGroups.class);
    if (concurrencyGroupsDefinitionAnnotation == null) {
      /// This actor is not defined with concurrency groups definition.
      return ret;
    }

    Map<String, ConcurrencyGroupImpl> concurrencyGroupsMap = new HashMap<>();
    DefConcurrencyGroup[] defAnnotations = concurrencyGroupsDefinitionAnnotation.value();
    if (defAnnotations.length == 0) {
      throw new IllegalArgumentException("TODO");
    }
    for (DefConcurrencyGroup def : defAnnotations) {
      concurrencyGroupsMap.put(
        def.name(), new ConcurrencyGroupImpl(def.name(), def.maxConcurrency()));
    }

    Method[] methods = actorClz.getMethods();
    for (Method method : methods) {
      UseConcurrencyGroup useConcurrencyGroupAnnotation =
        method.getAnnotation(UseConcurrencyGroup.class);
      if (useConcurrencyGroupAnnotation != null) {
        String concurrencyGroupName = useConcurrencyGroupAnnotation.name();
        Preconditions.checkState(concurrencyGroupsMap.containsKey(concurrencyGroupName));
        concurrencyGroupsMap
          .get(concurrencyGroupName)
          .addJavaFunctionDescriptor(
            new JavaFunctionDescriptor(
              method.getDeclaringClass().getName(),
              method.getName(),
              MethodUtils.getSignature(method)));
      }
    }

    concurrencyGroupsMap.forEach(
      (key, value) -> {
        ret.add(value);
      });
    return ret;
  }

  private static Class<?> getReturnTypeFromSignature(String signature) {
    final int startIndex = signature.indexOf(')');
    final int endIndex = signature.lastIndexOf(';');
    final String className = signature.substring(startIndex + 2, endIndex).replace('/', '.');
    Class<?> actorClz;
    try {
      try {
        actorClz = Class.forName(className);
      } catch (ClassNotFoundException e) {
        /// This code path indicates that here might be in another thread of a worker.
        /// So try to load the class from URLClassLoader of this worker.
        ClassLoader cl =
          ((RayRuntimeInternal) Ray.internal()).getWorkerContext().getCurrentClassLoader();
        actorClz = Class.forName(className, true, cl);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return actorClz;
  }
}
