package backtest.struct;

import backtest.utils.Util;

import java.util.*;

public class ArrayMap<K extends Comparable<? super K>, V> {
  class Entry<EntryKey, EntryValue> implements Map.Entry<EntryKey, EntryValue> {
    private EntryKey k;
    private EntryValue v;

    public Entry(EntryKey k, EntryValue v) {
      this.k = k;
      this.v = v;
    }

    public Entry(Map.Entry<EntryKey, EntryValue> entry) {
      this.k = entry.getKey();
      this.v = entry.getValue();
    }

    @Override
    public EntryKey getKey() {
      return k;
    }

    @Override
    public EntryValue getValue() {
      return v;
    }

    @Override
    public EntryValue setValue(EntryValue newValue) {
      EntryValue oldValue = v;
      v = newValue;
      return oldValue;
    }

    @Override
    public boolean equals(Object o) {
      if (o == null) return false;
      if (!(o instanceof Map.Entry))
        return false;
      Map.Entry<?, ?> e = (Map.Entry<?, ?>) o;
      return (k == null ? e.getKey() == null : k.equals(e.getKey())) &&
          (v == null ? e.getValue() == null : v.equals(e.getValue()));
    }

    public String toString() {
      return k + ": " + v;
    }
  }

  private class EntrySet extends AbstractSet<Map.Entry<K, V>> {
    @Override
    public Iterator<Map.Entry<K, V>> iterator() {
      return list.iterator();
    }

    @Override
    public int size() {
      return ArrayMap.this.size();
    }
  }

  private class EntryComparator implements Comparator<Map.Entry<K, V>> {
    @Override
    public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
      return o1.getKey().compareTo(o2.getKey());
    }
  }

  protected List<Map.Entry<K, V>> list;
  private EntrySet entrySet;
  protected EntryComparator entryComparator;

  public ArrayMap() {
    list = new ArrayList<>();
    entrySet = new EntrySet();
    entryComparator = new EntryComparator();
  }

  public ArrayMap(ArrayMap<K, V> arrayMap) {
    entrySet = new EntrySet();
    entryComparator = new EntryComparator();
    for (Map.Entry<K, V> entry : arrayMap.entrySet()) {
      list.add(new Entry<>(entry));
    }
  }

  public ArrayMap<K, V> put(K key, V val) {
    if (val != null && key != null)
      list.add(new Entry<>(key, val));
    return this;
  }

  public Map.Entry<K, V> getEntry(int i) {
    return list.get(i);
  }

  public int size() {
    return list.size();
  }

  public Set<Map.Entry<K, V>> entrySet() {
    return entrySet;
  }

  public void sortByKey() {
    Collections.sort(list, entryComparator);
  }

  public V lastValue() {
    if (size() == 0) throw new NoSuchElementException();
    return list.get(list.size() - 1).getValue();
  }

  public K lastKey() {
    if (size() == 0) throw new NoSuchElementException();
    return list.get(list.size() - 1).getKey();
  }

  public V firstValue() {
    if (size() == 0) throw new NoSuchElementException();
    return list.get(0).getValue();
  }

  public K firstKey() {
    if (size() == 0) throw new NoSuchElementException();
    return list.get(0).getKey();
  }

  /**
   * Simply check equality of elements one by one. Therefore need to sort beforehand explicitly.
   */
  public boolean equals(ArrayMap<K, V> map) {
    if (this.size() != map.size()) return false;
    Set<Map.Entry<K, V>> thisSet = this.entrySet();
    Set<Map.Entry<K, V>> otherSet = map.entrySet();
    Iterator<Map.Entry<K, V>> thisIt = thisSet.iterator();
    Iterator<Map.Entry<K, V>> otherIt = otherSet.iterator();
    while (thisIt.hasNext() && otherIt.hasNext()) {
      if (!thisIt.next().equals(otherIt.next())) return false;
    }
    return true;
  }

  public void print() {
    Util.printColl(list);
  }

  public Iterator<Map.Entry<K, V>> iterator() {
    return list.iterator();
  }

  public V getValue(int i) {
    if (i >= 0 && i < size()) return list.get(i).getValue();
    return null;
  }

  public void setValue(int i, V v) {
    if (i >= 0 && i < size()) list.get(i).setValue(v);
  }

  public K getKey(int i) {
    if (i>=0 && i < size()) return list.get(i).getKey();
    return null;
  }

  public void removeFirst() {
    if (!list.isEmpty()) list.remove(0);
  }
}