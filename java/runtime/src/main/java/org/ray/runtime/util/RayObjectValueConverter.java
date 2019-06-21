package org.ray.runtime.util;

import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;
import org.ray.api.exception.RayActorException;
import org.ray.api.exception.RayWorkerException;
import org.ray.api.exception.UnreconstructableException;
import org.ray.runtime.RayActorImpl;
import org.ray.runtime.generated.ErrorType;
import org.ray.runtime.proxyTypes.RayObjectValueProxy;

public class RayObjectValueConverter {
  private static final Converter[] fixedConverters = new Converter[] {
      new RawTypeConverter(),
      new ErrorTypeConverter(RayWorkerException.INSTANCE, ErrorType.WORKER_DIED),
      new ErrorTypeConverter(RayActorException.INSTANCE, ErrorType.ACTOR_DIED),
      new ErrorTypeConverter(UnreconstructableException.INSTANCE, ErrorType.OBJECT_UNRECONSTRUCTABLE),
      // TODO (kfstorm): support any exceptions to support cross-language.
      new RayActorConverter(),
  };

  private final FSTConverter fstConverter;

  public RayObjectValueConverter(ClassLoader classLoader) {
    this.fstConverter = new FSTConverter(classLoader);
  }

  private Stream<Converter> getConverters() {
    return Stream.concat(Arrays.stream(fixedConverters), Stream.of(fstConverter));
  }

  public RayObjectValueProxy toValue(Object object) {
    Optional<RayObjectValueProxy> value =
        getConverters().map(converter -> converter.toValue(object)).filter(Objects::nonNull).findFirst();
    if (value.isPresent()) {
      byte[] metadata = value.get().metadata;
      Preconditions.checkState(metadata != null && metadata.length > 0);
      Preconditions.checkState(value.get().data != null);
      return value.get();
    }
    throw new IllegalArgumentException("Cannot convert the object to object value: " + object);
  }

  public Object fromValue(RayObjectValueProxy objectValue) {
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
    RayObjectValueProxy toValue(Object object);

    /**
     * @param objectValue the object value to convert form.
     * @return the original object wrapped by {@link WrappedObject}. Null if cannot process the
     *     input object value.
     */
    WrappedObject fromValue(RayObjectValueProxy objectValue);
  }

  static class RawTypeConverter implements Converter {
    private static final byte[] RAW_TYPE_META = "RAW".getBytes();

    @Override
    public RayObjectValueProxy toValue(Object object) {
      if (object instanceof byte[]) {
        return new RayObjectValueProxy((byte[]) object, RAW_TYPE_META);
      }
      return null;
    }

    @Override
    public WrappedObject fromValue(RayObjectValueProxy objectValue) {
      if (Arrays.equals(objectValue.metadata, RAW_TYPE_META)) {
        return new WrappedObject(objectValue.data);
      }
      return null;
    }
  }

  static class SingleInstanceConverter implements Converter {
    private final Object instance;
    private final RayObjectValueProxy instanceValue;

    SingleInstanceConverter(Object instance, RayObjectValueProxy objectValue) {
      this.instanceValue = objectValue;
      this.instance = instance;
    }

    @Override
    public RayObjectValueProxy toValue(Object object) {
      if (this.instance.equals(object)) {
        return instanceValue;
      }
      return null;
    }

    @Override
    public WrappedObject fromValue(RayObjectValueProxy objectValue) {
      if (Arrays.equals(this.instanceValue.metadata, objectValue.metadata)
          && Arrays.equals(instanceValue.data, objectValue.data)) {
        return new WrappedObject(instance);
      }
      return null;
    }
  }

  static class ErrorTypeConverter extends SingleInstanceConverter {
    ErrorTypeConverter(Object instance, int errorType) {
      super(instance, new RayObjectValueProxy(new byte[0], String.valueOf(errorType).getBytes()));
    }
  }

  static class RayActorConverter implements Converter {
    private static final byte[] ACTOR_HANDLE_META = "ACTOR_HANDLE".getBytes();

    @Override
    public RayObjectValueProxy toValue(Object object) {
      if (object instanceof RayActorImpl) {
        return new RayObjectValueProxy(((RayActorImpl) object).fork().Binary(), ACTOR_HANDLE_META);
      }
      return null;
    }

    @Override
    public WrappedObject fromValue(RayObjectValueProxy objectValue) {
      if (Arrays.equals(objectValue.metadata, ACTOR_HANDLE_META)) {
        return new WrappedObject(RayActorImpl.fromBinary(objectValue.data));
      }
      return null;
    }
  }

  public static class FSTConverter implements Converter {
    public static final byte[] JAVA_OBJECT_META = "JAVA_OBJECT".getBytes();

    private final ClassLoader classLoader;

    public FSTConverter(ClassLoader classLoader) {
      this.classLoader = classLoader;
    }

    @Override
    public RayObjectValueProxy toValue(Object object) {
      return new RayObjectValueProxy(Serializer.encode(object, classLoader), JAVA_OBJECT_META);
    }

    @Override
    public WrappedObject fromValue(RayObjectValueProxy objectValue) {
      if (Arrays.equals(objectValue.metadata, JAVA_OBJECT_META)
          // TODO (kfstorm): remove this fallback behavior after support pass by value with metadata
          || objectValue.metadata == null || objectValue.metadata.length == 0
      ) {
        return new WrappedObject(Serializer.decode(objectValue.data, classLoader));
      }
      return null;
    }
  }
}
