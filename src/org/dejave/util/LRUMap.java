package org.dejave.util;

import java.util.Map;
import java.util.LinkedHashMap;
import java.util.Set;

public class LRUMap <K, V> {

    public static final int DEFAULT_CAPACITY = 1000;

    private int capacity;
    private Map<K, V> map;

    public LRUMap() { this(DEFAULT_CAPACITY); }

    public LRUMap(int cap) {
        capacity = cap;
        float factor = 0.75f;
        int mapCapacity = (int) Math.ceil(capacity / factor) + 1;
        map = new LinkedHashMap<K, V>(mapCapacity, factor, true) {
            protected boolean removeEldestEntry(Map.Entry<K, V> entry) {
                boolean ret = size() > LRUMap.this.capacity;
                if (ret) LRUMap.this.onRemove(entry);
                return ret;
            }
        };
    }

    public void put(K k, V v) { map.put(k, v); }
    public V get(K k) { return map.get(k); }
    public int size() { return map.size(); }
    public void clear() { map.clear(); }
    public boolean containsKey(K k) { return map.containsKey(k); }    
    public boolean containsValue(V v) { return map.containsValue(v); }
    public V remove(K k) { return map.remove(k); }
    public Set<Map.Entry<K, V> > entrySet() { return map.entrySet(); }
    protected void onRemove(Map.Entry<K, V> entry) {}
    
    public String toString() { return map.toString(); }
    
    public static void main (String [] args) {
        LRUMap<Integer, String> map = new LRUMap<Integer, String>(3);
        System.out.println("will insert one");
        map.put(1, "one");
        System.out.println("one inserted, will insert two");
        map.put(2, "two");
        System.out.println("two inserted, will insert three");
        map.put(3, "three");
        System.out.println("three inserted, will insert four");
        System.out.println(map.toString());
        map.get(1);
        System.out.println(map.toString());
        map.put(4, "four");
        System.out.println("four inserted");
        System.out.println(map.toString());
    }
}
