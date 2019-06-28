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

  public RayObjectProxy toValue(Object object) {
    Optional<RayObjectProxy> value =
        getConverters().map(converter -> converter.toValue(object)).filter(Objects::nonNull).findFirst();
    if (value.isPresent()) {
      byte[] metadata = value.get().metadata;
      Preconditions.checkState(metadata != null && metadata.length > 0);
      Preconditions.checkState(value.get().data != null);
      return value.get();
    }
    throw new IllegalArgumentException("Cannot convert the object to object value: " + object);
  }

  public Object fromValue(RayObjectProxy objectValue) {
    Preconditions.checkState(objectValue != null);
    Optional<WrappedObject> obj = getConverters()
        .map(converter -> converter.fromValue(objectValue))
        .filter(Objects::nonNull)
        .findFirst();
    if (obj.isPresent()) {
      return obj.get().getObject();
    }
    throw new IllegalArgumentException("Cannot convert the object value back to an object");
  }

  static class WrappedObject {
    private Object object;

    WrappedObject(Object object) {
      this.object = object;
    }

    public Object getObject() {
      return object;
    }
  }

  /**
   * For convert to and from object value for a specific object type.
   */
  interface Converter {
    /**
     * @param object the object to convert from.
     * @return converted object value. Null if this converter cannot process the input object.
     */
    RayObjectProxy toValue(Object object);

    /**
     * @param objectValue the object value to convert form.
     * @return the original object wrapped by {@link WrappedObject}. Null if cannot process the
     *     input object value.
     */
    WrappedObject fromValue(RayObjectProxy objectValue);
  }

  static class RawTypeConverter implements Converter {
    private static final byte[] RAW_TYPE_META = "RAW".getBytes();

    @Override
    public RayObjectProxy toValue(Object object) {
      if (object instanceof byte[]) {
        return new RayObjectProxy((byte[]) object, RAW_TYPE_META);
      }
      return null;
    }

    @Override
    public WrappedObject fromValue(RayObjectProxy objectValue) {
      if (Arrays.equals(objectValue.metadata, RAW_TYPE_META)) {
        return new WrappedObject(objectValue.data);
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
    public RayObjectProxy toValue(Object object) {
      if (this.instance.equals(object)) {
        return instanceValue;
      }
      return null;
    }

    @Override
    public WrappedObject fromValue(RayObjectProxy objectValue) {
      if (Arrays.equals(this.instanceValue.metadata, objectValue.metadata)
          && Arrays.equals(instanceValue.data, objectValue.data)) {
        return new WrappedObject(instance);
      }
      return null;
    }
  }

  static class RayActorConverter implements Converter {
    private static final byte[] ACTOR_HANDLE_META = "ACTOR_HANDLE".getBytes();

    @Override
    public RayObjectProxy toValue(Object object) {
      if (object instanceof RayActorImpl) {
        return new RayObjectProxy(((RayActorImpl) object).fork().Binary(), ACTOR_HANDLE_META);
      }
      return null;
    }

    @Override
    public WrappedObject fromValue(RayObjectProxy objectValue) {
      if (Arrays.equals(objectValue.metadata, ACTOR_HANDLE_META)) {
        return new WrappedObject(RayActorImpl.fromBinary(objectValue.data));
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
    public RayObjectProxy toValue(Object object) {
      if (canConvert.apply(object)) {
        return new RayObjectProxy(Serializer.encode(object, classLoader), expectedMetadata);
      }
      return null;
    }

    @Override
    public WrappedObject fromValue(RayObjectProxy objectValue) {
      if (((expectedMetadata == null || expectedMetadata.length == 0) &&
          (objectValue.metadata == null || objectValue.metadata.length == 0)) ||
          Arrays.equals(objectValue.metadata, expectedMetadata)) {
        return new WrappedObject(Serializer.decode(objectValue.data, classLoader));
      }
      return null;
    }
  }
}
