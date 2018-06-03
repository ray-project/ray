package org.ray.api;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * A RayMap&lt;K&gt; maintains a map from K to RayObject&lt;V&gt;,
 * and serves as parameters and/or return values of Ray calls.
 */
public class RayMap<K, V> extends HashMap<K, V> {

  private static final long serialVersionUID = 7296072498584721265L;

  private final HashMap<K, RayObject<V>> ids = new HashMap<>();

  public HashMap<K, RayObject<V>> Objects() {
    return ids;
  }

  @Override
  public int size() {
    // throw new UnsupportedOperationException();
    return ids.size();
  }

  @Override
  public boolean isEmpty() {
    //throw new UnsupportedOperationException();
    return ids.isEmpty();
  }

  @Override
  public boolean containsKey(Object key) {
    //throw new UnsupportedOperationException();
    return ids.containsKey(key);
  }

  @Override
  public boolean containsValue(Object value) {
    //throw new UnsupportedOperationException();
    return ids.containsValue(value);
  }

  // TODO: try to use multiple get
  public Map<K, V> get() {
    Map<K, V> objs = new HashMap<>();
    for (Map.Entry<K, RayObject<V>> id : ids.entrySet()) {
      objs.put(id.getKey(), id.getValue().get());
    }
    return objs;
  }

  public <TM> Map<K, TM> getMeta() {
    Map<K, TM> metas = new HashMap<>();
    for (Map.Entry<K, RayObject<V>> id : ids.entrySet()) {
      TM meta = id.getValue().getMeta();
      metas.put(id.getKey(), meta);
    }
    return metas;
  }

  @Override
  public V get(Object key) {
    return ids.get(key).get();
  }

  public <TM> TM getMeta(K key) {
    return ids.get(key).getMeta();
  }

  public RayObject<V> Get(K key) {
    return ids.get(key);
  }

  @RayDisabled
  @Deprecated
  @Override
  public V put(K key, V value) {
    throw new UnsupportedOperationException();
  }

  public RayObject<V> put(K key, RayObject<V> value) {
    return ids.put(key, value);
  }

  @RayDisabled
  @Deprecated
  @Override
  public V remove(Object key) {
    throw new UnsupportedOperationException();
  }

  public RayObject<V> Remove(K key) {
    return ids.remove(key);
  }

  @RayDisabled
  @Deprecated
  @Override
  public void putAll(Map<? extends K, ? extends V> m) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void clear() {
    //throw new UnsupportedOperationException();
    ids.clear();
  }

  @Override
  public Set<K> keySet() {
    return ids.keySet();
  }

  @RayDisabled
  @Deprecated
  @Override
  public Collection<V> values() {
    throw new UnsupportedOperationException();
  }

  public Collection<RayObject<V>> Values() {
    return ids.values();
  }

  @RayDisabled
  @Deprecated
  @Override
  public Set<java.util.Map.Entry<K, V>> entrySet() {
    throw new UnsupportedOperationException();
  }

  public Set<java.util.Map.Entry<K, RayObject<V>>> EntrySet() {
    return ids.entrySet();
  }
}
