package io.ray.runtime.serialization.serializers;

import com.google.common.base.Preconditions;
import io.ray.runtime.io.MemoryBuffer;
import io.ray.runtime.serialization.RaySerde;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

public class UnmodifiableCollectionSerializer extends Serializer<Object> {
  private static final Field SOURCE_COLLECTION_FIELD;
  private static final Field SOURCE_MAP_FIELD;

  static {
    try {
      SOURCE_COLLECTION_FIELD =
          Class.forName("java.util.Collections$UnmodifiableCollection").getDeclaredField("c");
      SOURCE_COLLECTION_FIELD.setAccessible(true);
      SOURCE_MAP_FIELD =
          Class.forName("java.util.Collections$UnmodifiableMap").getDeclaredField("m");
      SOURCE_MAP_FIELD.setAccessible(true);
    } catch (Exception e) {
      throw new RuntimeException(
          "Could not access source collection "
              + "field in java.util.Collections$UnmodifiableCollection.",
          e);
    }
  }

  private final UnmodifiableCollection unmodifiableCollection;

  public UnmodifiableCollectionSerializer(
      RaySerde raySerDe, Class<Object> cls, UnmodifiableCollection unmodifiableCollection) {
    super(raySerDe, cls);
    this.unmodifiableCollection = unmodifiableCollection;
  }

  @Override
  public void write(MemoryBuffer buffer, Object value) {
    Preconditions.checkArgument(value.getClass() == cls);
    final UnmodifiableCollection unmodifiableCollection = this.unmodifiableCollection;
    try {
      raySerDe.serializeReferencableToJava(
          buffer, unmodifiableCollection.sourceCollectionField.get(value));
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Object read(MemoryBuffer buffer) {
    final Object sourceCollection = raySerDe.deserializeReferencableFromJava(buffer);
    return unmodifiableCollection.create(sourceCollection);
  }

  enum UnmodifiableCollection {
    COLLECTION(
        Collections.unmodifiableCollection(Collections.singletonList("")).getClass(),
        SOURCE_COLLECTION_FIELD) {
      @Override
      public Object create(final Object sourceCollection) {
        return Collections.unmodifiableCollection((Collection<?>) sourceCollection);
      }
    },
    RANDOM_ACCESS_LIST(
        Collections.unmodifiableList(new ArrayList<Void>()).getClass(), SOURCE_COLLECTION_FIELD) {
      @Override
      public Object create(final Object sourceCollection) {
        return Collections.unmodifiableList((List<?>) sourceCollection);
      }
    },
    LIST(Collections.unmodifiableList(new LinkedList<Void>()).getClass(), SOURCE_COLLECTION_FIELD) {
      @Override
      public Object create(final Object sourceCollection) {
        return Collections.unmodifiableList((List<?>) sourceCollection);
      }
    },
    SET(Collections.unmodifiableSet(new HashSet<Void>()).getClass(), SOURCE_COLLECTION_FIELD) {
      @Override
      public Object create(final Object sourceCollection) {
        return Collections.unmodifiableSet((Set<?>) sourceCollection);
      }
    },
    SORTED_SET(
        Collections.unmodifiableSortedSet(new TreeSet<>()).getClass(), SOURCE_COLLECTION_FIELD) {
      @Override
      public Object create(final Object sourceCollection) {
        return Collections.unmodifiableSortedSet((SortedSet<?>) sourceCollection);
      }
    },
    MAP(Collections.unmodifiableMap(new HashMap<Void, Void>()).getClass(), SOURCE_MAP_FIELD) {
      @Override
      public Object create(final Object sourceCollection) {
        return Collections.unmodifiableMap((Map<?, ?>) sourceCollection);
      }
    },
    SORTED_MAP(Collections.unmodifiableSortedMap(new TreeMap<>()).getClass(), SOURCE_MAP_FIELD) {
      @Override
      public Object create(final Object sourceCollection) {
        return Collections.unmodifiableSortedMap((SortedMap<?, ?>) sourceCollection);
      }
    };

    private final Class<?> type;
    private final Field sourceCollectionField;

    UnmodifiableCollection(final Class<?> type, final Field sourceCollectionField) {
      this.type = type;
      this.sourceCollectionField = sourceCollectionField;
    }

    public abstract Object create(Object sourceCollection);

    static UnmodifiableCollection valueOfType(final Class<?> type) {
      for (final UnmodifiableCollection item : values()) {
        if (item.type == type) {
          return item;
        }
      }
      throw new IllegalArgumentException("The type " + type + " is not supported.");
    }
  }

  /**
   * Creates a new {@link UnmodifiableCollectionSerializer} and registers its serializer for the
   * several unmodifiable Collections that can be created via {@link Collections}, including {@link
   * Map}s.
   *
   * @see Collections#unmodifiableCollection(Collection)
   * @see Collections#unmodifiableList(List)
   * @see Collections#unmodifiableSet(Set)
   * @see Collections#unmodifiableSortedSet(SortedSet)
   * @see Collections#unmodifiableMap(Map)
   * @see Collections#unmodifiableSortedMap(SortedMap)
   */
  @SuppressWarnings("unchecked")
  public static void registerSerializers(RaySerde raySerDe) {
    for (UnmodifiableCollection item : UnmodifiableCollection.values()) {
      raySerDe.registerSerializer(
          item.type,
          new UnmodifiableCollectionSerializer(raySerDe, (Class<Object>) item.type, item));
    }
  }
}
