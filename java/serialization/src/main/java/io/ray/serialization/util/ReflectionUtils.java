package io.ray.serialization.util;

import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@SuppressWarnings("UnstableApiUsage")
public class ReflectionUtils {
  public static boolean hasPublicNoArgConstructor(Class<?> clazz) {
    Constructor<?> constructor = getNoArgConstructor(clazz);
    return constructor != null && Modifier.isPublic(constructor.getModifiers());
  }

  public static Constructor<?> getNoArgConstructor(Class<?> clazz) {
    if (clazz.isInterface()) {
      return null;
    }
    if (Modifier.isAbstract(clazz.getModifiers())) {
      return null;
    }
    Constructor<?>[] constructors = clazz.getDeclaredConstructors();
    if (constructors.length == 0) {
      return null;
    } else {
      return Stream.of(constructors)
          .filter((c) -> c.getParameterCount() == 0)
          .findAny()
          .orElse(null);
    }
  }

  /**
   * @return all methods named by {@code methodName}, for covariant return type, return the most
   *     specific method.
   */
  public static List<Method> findMethods(Class<?> cls, String methodName) {
    List<Class<?>> classes = new ArrayList<>();
    Class<?> clazz = cls;
    while (clazz != null) {
      classes.add(clazz);
      clazz = clazz.getSuperclass();
    }
    classes.addAll(getAllInterfaces(cls));
    if (classes.indexOf(Object.class) == -1) {
      classes.add(Object.class);
    }

    LinkedHashMap<List<Class<?>>, Method> methods = new LinkedHashMap<>();
    for (Class<?> aClass : classes) {
      for (Method m : aClass.getDeclaredMethods()) {
        if (m.getName().equals(methodName)) {
          List<Class<?>> params = Arrays.asList(m.getParameterTypes());
          Method method = methods.get(params);
          if (method == null) {
            methods.put(params, m);
          } else {
            // for covariant return type, use the most specific method
            if (method.getReturnType().isAssignableFrom(m.getReturnType())) {
              methods.put(params, m);
            }
          }
        }
      }
    }
    return new ArrayList<>(methods.values());
  }

  /**
   * Gets a <code>List</code> of all interfaces implemented by the given class and its superclasses.
   *
   * <p>The order is determined by looking through each interface in turn as declared in the source
   * file and following its hierarchy up.
   */
  public static List<Class<?>> getAllInterfaces(Class<?> cls) {
    if (cls == null) {
      return null;
    }

    LinkedHashSet<Class<?>> interfacesFound = new LinkedHashSet<>();
    getAllInterfaces(cls, interfacesFound);
    return new ArrayList<>(interfacesFound);
  }

  private static void getAllInterfaces(Class<?> cls, LinkedHashSet<Class<?>> interfacesFound) {
    while (cls != null) {
      Class[] interfaces = cls.getInterfaces();
      for (Class anInterface : interfaces) {
        if (!interfacesFound.contains(anInterface)) {
          interfacesFound.add(anInterface);
          getAllInterfaces(anInterface, interfacesFound);
        }
      }

      cls = cls.getSuperclass();
    }
  }

  /** @return true if any method named {@code methodName} has exception */
  public static boolean hasException(Class<?> cls, String methodName) {
    List<Method> methods = findMethods(cls, methodName);
    if (methods.isEmpty()) {
      String msg = String.format("class %s doesn't have method %s", cls, methodName);
      throw new IllegalArgumentException(msg);
    }
    return methods.get(0).getExceptionTypes().length > 0;
  }

  public static Class<?> getReturnType(Class<?> cls, String methodName) {
    List<Method> methods = findMethods(cls, methodName);
    if (methods.isEmpty()) {
      String msg = String.format("class %s doesn't have method %s", cls, methodName);
      throw new IllegalArgumentException(msg);
    }
    Set<? extends Class<?>> returnTypes =
        methods.stream().map(Method::getReturnType).collect(Collectors.toSet());
    Preconditions.checkArgument(returnTypes.size() == 1);
    return methods.get(0).getReturnType();
  }

  /**
   * Return a field named <code>fieldName</code> from <code>cls</code>. Search parent class if not
   * found.
   */
  public static Field getField(Class<?> cls, String fieldName) {
    Class<?> clazz = cls;
    do {
      Field[] fields = clazz.getDeclaredFields();
      for (Field field : fields) {
        if (field.getName().equals(fieldName)) {
          return field;
        }
      }
      clazz = clazz.getSuperclass();
    } while (clazz != null);

    String msg = String.format("class %s doesn't have field %s", cls, fieldName);
    throw new IllegalArgumentException(msg);
  }

  /** @return generic type arguments of <code>typeToken</code> */
  public static List<TypeToken<?>> getTypeArguments(TypeToken typeToken) {
    if (typeToken.getType() instanceof ParameterizedType) {
      ParameterizedType parameterizedType = (ParameterizedType) typeToken.getType();
      return Arrays.stream(parameterizedType.getActualTypeArguments())
          .map(TypeToken::of)
          .collect(Collectors.toList());
    } else {
      return new ArrayList<>();
    }
  }

  /**
   * @return generic type arguments of <code>typeToken</code>, includes generic type arguments of
   *     generic type arguments recursively
   */
  public static List<TypeToken<?>> getAllTypeArguments(TypeToken typeToken) {
    List<TypeToken<?>> types = getTypeArguments(typeToken);
    LinkedHashSet<TypeToken<?>> allTypeArguments = new LinkedHashSet<>(types);
    for (TypeToken<?> type : types) {
      allTypeArguments.addAll(getAllTypeArguments(type));
    }

    return new ArrayList<>(allTypeArguments);
  }

  public static String getClassNameWithoutPackage(Class<?> clz) {
    String className = clz.getName();
    int index = className.lastIndexOf(".");
    if (index != -1) {
      return className.substring(index + 1);
    } else {
      return className;
    }
  }
}
