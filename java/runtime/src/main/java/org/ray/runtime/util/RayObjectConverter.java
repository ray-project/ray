package org.ray.runtime.util;

import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;
import org.ray.api.exception.RayActorException;
import org.ray.api.exception.RayTaskException;
import org.ray.api.exception.RayWorkerException;
import org.ray.api.exception.UnreconstructableException;
import org.ray.runtime.RayActorImpl;
import org.ray.runtime.generated.Gcs.ErrorType;
import org.ray.runtime.proxyTypes.RayObjectProxy;

public class RayObjectConverter {
  public static final byte[] JAVA_OBJECT_META = "JAVA_OBJECT".getBytes();

  private static final Converter[] fixedConverters = new Converter[] {
      new RawTypeConverter(),
      new ErrorTypeConverter(RayWorkerException.INSTANCE, ErrorType.WORKER_DIED),
      new ErrorTypeConverter(RayActorException.INSTANCE, ErrorType.ACTOR_DIED),
      new ErrorTypeConverter(UnreconstructableException.INSTANCE, ErrorType.OBJECT_UNRECONSTRUCTABLE),
      // TODO (kfstorm): support any exceptions to support cross-language.
      new RayActorConverter(),
  };

  private final FSTConverter[] fstConverters;

  public RayObjectConverter(ClassLoader classLoader) {
    this.fstConverters = new FSTConverter[] {
        new FSTConverter(String.valueOf(ErrorType.TASK_EXCEPTION.getNumber()).getBytes(),
            obj -> obj instanceof RayTaskException, classLoader),
        new FSTConverter(JAVA_OBJECT_META, obj -> true, classLoader),
        // TODO (kfstorm): remove this fallback behavior after support pass by value with metadata
        new FSTConverter(null, obj -> true, classLoader),
    };
  }

  private Stream<Converter> getConverters() {
    return Stream.concat(Arrays.stream(fixedConverters), Arrays.stream(fstConverters));
  }

  public RayObjectProxy toRayObject(Object javaObject) {
    Optional<RayObjectProxy> value =
        getConverters().map(converter -> converter.toRayObject(javaObject)).filter(Objects::nonNull).findFirst();
    if (value.isPresent()) {
      byte[] metadata = value.get().metadata;
      Preconditions.checkState(metadata != null && metadata.length > 0);
      Preconditions.checkState(value.get().data != null);
      return value.get();
    }
    throw new IllegalArgumentException("Cannot convert the Java object to ray object: " + javaObject);
  }

  public Object fromRayObject(RayObjectProxy rayObject) {
    Preconditions.checkState(rayObject != null);
    Optional<WrappedObject> obj = getConverters()
        .map(converter -> converter.fromRayObject(rayObject))
        .filter(Objects::nonNull)
        .findFirst();
    if (obj.isPresent()) {
      return obj.get().getJavaObject();
    }
    throw new IllegalArgumentException("Cannot convert the ray object to Java object.");
  }

  static class WrappedObject {
    private Object javaObject;

    WrappedObject(Object javaObject) {
      this.javaObject = javaObject;
    }

    public Object getJavaObject() {
      return javaObject;
    }
  }

  /**
   * For conversion between Java object and ray object for a specific object type.
   */
  interface Converter {
    /**
     * @param object the Java object to convert from.
     * @return converted ray object. Null if this converter cannot process the input object.
     */
    RayObjectProxy toRayObject(Object javaObject);

    /**
     * @param rayObject the ray object to convert form.
     * @return the original object wrapped by {@link WrappedObject}. Null if cannot process the
     *     input ray object.
     */
    WrappedObject fromRayObject(RayObjectProxy rayObject);
  }

  static class RawTypeConverter implements Converter {
    private static final byte[] RAW_TYPE_META = "RAW".getBytes();

    @Override
    public RayObjectProxy toRayObject(Object javaObject) {
      if (javaObject instanceof byte[]) {
        return new RayObjectProxy((byte[]) javaObject, RAW_TYPE_META);
      }
      return null;
    }

    @Override
    public WrappedObject fromRayObject(RayObjectProxy rayObject) {
      if (Arrays.equals(rayObject.metadata, RAW_TYPE_META)) {
        return new WrappedObject(rayObject.data);
      }
      return null;
    }
  }

  static class ErrorTypeConverter implements Converter {
    private final Object instance;
    private final RayObjectProxy instanceValue;

    ErrorTypeConverter(Object instance, ErrorType errorType) {
      this.instance = instance;
      this.instanceValue = new RayObjectProxy(new byte[0],
          String.valueOf(errorType.getNumber()).getBytes());
    }

    @Override
    public RayObjectProxy toRayObject(Object javaObject) {
      if (this.instance.equals(javaObject)) {
        return instanceValue;
      }
      return null;
    }

    @Override
    public WrappedObject fromRayObject(RayObjectProxy rayObject) {
      if (Arrays.equals(this.instanceValue.metadata, rayObject.metadata)
          && Arrays.equals(instanceValue.data, rayObject.data)) {
        return new WrappedObject(instance);
      }
      return null;
    }
  }

  static class RayActorConverter implements Converter {
    private static final byte[] ACTOR_HANDLE_META = "ACTOR_HANDLE".getBytes();

    @Override
    public RayObjectProxy toRayObject(Object javaObject) {
      if (javaObject instanceof RayActorImpl) {
        return new RayObjectProxy(((RayActorImpl) javaObject).fork().Binary(), ACTOR_HANDLE_META);
      }
      return null;
    }

    @Override
    public WrappedObject fromRayObject(RayObjectProxy rayObject) {
      if (Arrays.equals(rayObject.metadata, ACTOR_HANDLE_META)) {
        return new WrappedObject(RayActorImpl.fromBinary(rayObject.data));
      }
      return null;
    }
  }

  static class FSTConverter implements Converter {
    private final byte[] expectedMetadata;
    private final Function<Object, Boolean> canConvert;
    private final ClassLoader classLoader;

    FSTConverter(byte[] expectedMetadata, Function<Object, Boolean> canConvert, ClassLoader classLoader) {
      this.expectedMetadata = expectedMetadata;
      this.canConvert = canConvert;
      this.classLoader = classLoader;
    }

    @Override
    public RayObjectProxy toRayObject(Object javaObject) {
      if (canConvert.apply(javaObject)) {
        return new RayObjectProxy(Serializer.encode(javaObject, classLoader), expectedMetadata);
      }
      return null;
    }

    @Override
    public WrappedObject fromRayObject(RayObjectProxy rayObject) {
      if (((expectedMetadata == null || expectedMetadata.length == 0) &&
          (rayObject.metadata == null || rayObject.metadata.length == 0)) ||
          Arrays.equals(rayObject.metadata, expectedMetadata)) {
        return new WrappedObject(Serializer.decode(rayObject.data, classLoader));
      }
      return null;
    }
  }
}
