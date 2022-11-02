package io.ray.runtime.util;

import com.google.common.base.Preconditions;
import io.ray.api.concurrencygroup.ConcurrencyGroup;
import io.ray.api.concurrencygroup.annotations.DefConcurrencyGroup;
import io.ray.api.concurrencygroup.annotations.DefConcurrencyGroups;
import io.ray.api.concurrencygroup.annotations.UseConcurrencyGroup;
import io.ray.api.function.RayFuncR;
import io.ray.runtime.ConcurrencyGroupImpl;
import io.ray.runtime.functionmanager.JavaFunctionDescriptor;
import java.lang.invoke.SerializedLambda;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/// TODO(qwang): cache this.
public final class ConcurrencyGroupUtils {

  public static List<ConcurrencyGroup> extractConcurrencyGroupsByAnnotations(
      RayFuncR<?> actorConstructorLambda) {
    SerializedLambda serializedLambda = LambdaUtils.getSerializedLambda(actorConstructorLambda);
    Class<?> actorClz =
        MethodUtils.getReturnTypeFromSignature(serializedLambda.getInstantiatedMethodType());

    /// Extract the concurrency groups definition.
    ArrayList<ConcurrencyGroup> ret = new ArrayList<ConcurrencyGroup>();
    Map<String, ConcurrencyGroupImpl> allConcurrencyGroupsMap =
        extractConcurrencyGroupsFromClassAnnotation(actorClz);
    Class<?>[] interfaces = actorClz.getInterfaces();
    for (Class<?> interfaceClz : interfaces) {
      Preconditions.checkState(interfaceClz != null);
      Map<String, ConcurrencyGroupImpl> currConcurrencyGroups =
          extractConcurrencyGroupsFromClassAnnotation(interfaceClz);
      allConcurrencyGroupsMap.putAll(currConcurrencyGroups);
    }

    /// Extract the using of concurrency groups which annotated on the actor methods.
    Method[] methods = actorClz.getMethods();
    for (Method method : methods) {
      UseConcurrencyGroup useConcurrencyGroupAnnotation =
          method.getAnnotation(UseConcurrencyGroup.class);
      if (useConcurrencyGroupAnnotation != null) {
        String concurrencyGroupName = useConcurrencyGroupAnnotation.name();
        Preconditions.checkState(allConcurrencyGroupsMap.containsKey(concurrencyGroupName));
        allConcurrencyGroupsMap
            .get(concurrencyGroupName)
            .addJavaFunctionDescriptor(
                new JavaFunctionDescriptor(
                    method.getDeclaringClass().getName(),
                    method.getName(),
                    MethodUtils.getSignature(method)));
      }
    }

    allConcurrencyGroupsMap.forEach(
        (key, value) -> {
          ret.add(value);
        });
    return ret;
  }

  /// Extract the concurrency groups from the class annotation.
  /// Both work for class and interface.
  private static Map<String, ConcurrencyGroupImpl> extractConcurrencyGroupsFromClassAnnotation(
      Class<?> clz) {
    Map<String, ConcurrencyGroupImpl> ret = new HashMap<>();
    DefConcurrencyGroups concurrencyGroupsDefinitionAnnotation =
        clz.getAnnotation(DefConcurrencyGroups.class);
    if (concurrencyGroupsDefinitionAnnotation != null) {
      DefConcurrencyGroup[] defAnnotations = concurrencyGroupsDefinitionAnnotation.value();
      if (defAnnotations.length == 0) {
        throw new IllegalArgumentException("TODO");
      }
      for (DefConcurrencyGroup def : defAnnotations) {
        ret.put(def.name(), new ConcurrencyGroupImpl(def.name(), def.maxConcurrency()));
      }
    } else {
      /// Code path of that no annotation or 1 annotation definition.
      DefConcurrencyGroup defConcurrencyGroup = clz.getAnnotation(DefConcurrencyGroup.class);
      if (defConcurrencyGroup != null) {
        ret.put(
            defConcurrencyGroup.name(),
            new ConcurrencyGroupImpl(
                defConcurrencyGroup.name(), defConcurrencyGroup.maxConcurrency()));
      } else {
        /// This actor is not defined with concurrency groups annotation.
        return ret;
      }
    }
    return ret;
  }
}
