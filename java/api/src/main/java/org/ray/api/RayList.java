package org.ray.api;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

/**
 * A RayList&lt;E&gt; holds a list of RayObject&lt;E&gt;,
 * and can serves as parameters and/or return values of Ray calls.
 */
public class RayList<E> extends ArrayList<E> {

  private static final long serialVersionUID = 2129403593610953658L;

  private final ArrayList<RayObject<E>> ids = new ArrayList<>();

  public List<RayObject<E>> Objects() {
    return ids;
  }

  @Override
  public int size() {
    // throw new UnsupportedOperationException();
    return ids.size();
  }

  @Override
  public boolean isEmpty() {
    // throw new UnsupportedOperationException();
    return ids.isEmpty();
  }

  @Override
  public boolean contains(Object o) {
    // throw new UnsupportedOperationException();
    return ids.contains(o);
  }

  @RayDisabled
  @Deprecated
  @Override
  public Iterator<E> iterator() {
    throw new UnsupportedOperationException();
  }

  public Iterator<RayObject<E>> Iterator() {
    return ids.iterator();
  }

  @Override
  public Object[] toArray() {
    //throw new UnsupportedOperationException();
    return ids.toArray();
  }

  @Override
  public <T> T[] toArray(T[] a) {
    //throw new UnsupportedOperationException();
    return ids.toArray(a);
  }

  @RayDisabled
  @Deprecated
  @Override
  public boolean add(E e) {
    throw new UnsupportedOperationException();
  }

  public boolean add(RayObject<E> e) {
    return ids.add(e);
  }

  @Override
  public boolean remove(Object o) {
    //throw new UnsupportedOperationException();
    return ids.remove(o);
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    //throw new UnsupportedOperationException();
    return ids.containsAll(c);
  }

  @RayDisabled
  @Deprecated
  @Override
  public boolean addAll(Collection<? extends E> c) {
    throw new UnsupportedOperationException();
  }

  @RayDisabled
  @Deprecated
  @Override
  public boolean addAll(int index, Collection<? extends E> c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    //throw new UnsupportedOperationException();
    return ids.removeAll(c);
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    //throw new UnsupportedOperationException();
    return ids.retainAll(c);
  }

  @Override
  public void clear() {
    //throw new UnsupportedOperationException();
    ids.clear();
  }

  public List<E> get() {
    List<UniqueID> objectIds = new ArrayList<>();
    for (RayObject<E> id : ids) {
      objectIds.add(id.getId());
    }
    return Ray.get(objectIds);
  }

  public <T> List<T> getMeta() {
    List<UniqueID> objectIds = new ArrayList<>();
    for (RayObject<E> id : ids) {
      objectIds.add(id.getId());
    }
    return Ray.getMeta(objectIds);
  }

  @Override
  public E get(int index) {
    return ids.get(index).get();
  }

  public <TM> TM getMeta(int index) {
    return ids.get(index).getMeta();
  }

  public RayObject<E> Get(int index) {
    return ids.get(index);
  }

  @RayDisabled
  @Deprecated
  @Override
  public E set(int index, E element) {
    throw new UnsupportedOperationException();
  }

  public RayObject<E> set(int index, RayObject<E> element) {
    return ids.set(index, element);
  }

  @RayDisabled
  @Deprecated
  @Override
  public void add(int index, E element) {
    throw new UnsupportedOperationException();
  }

  public void add(int index, RayObject<E> element) {
    ids.add(index, element);
  }

  @RayDisabled
  @Deprecated
  @Override
  public E remove(int index) {
    throw new UnsupportedOperationException();
  }

  public RayObject<E> Remove(int index) {
    return ids.remove(index);
  }

  @Override
  public int indexOf(Object o) {
    //throw new UnsupportedOperationException();
    return ids.indexOf(o);
  }

  @Override
  public int lastIndexOf(Object o) {
    //throw new UnsupportedOperationException();
    return ids.lastIndexOf(o);
  }

  @RayDisabled
  @Deprecated
  @Override
  public ListIterator<E> listIterator() {
    throw new UnsupportedOperationException();
  }

  @RayDisabled
  @Deprecated
  @Override
  public ListIterator<E> listIterator(int index) {
    throw new UnsupportedOperationException();
  }

  @RayDisabled
  @Deprecated
  @Override
  public List<E> subList(int fromIndex, int toIndex) {
    throw new UnsupportedOperationException();
  }
}
