package org.dejave.util;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.LinkedList;
import java.util.Collections;

public class DuplicateHashMap <K extends Comparable<? super K>, V> {
    private Map<K, List<V>> map;

    public DuplicateHashMap() {
	map = new HashMap<K, List<V> >();
    }

    public void put(K k, V v) {
	List<V> list = map.get(k);
	if (list == null) list = new LinkedList<V>();
	list.add(v);
	map.put(k, list);
    }

    public int size() {
        int sz = 0;
        for (List<V> list : map.values())
            sz += list.size();
        return sz;
    }

    public List<V> get(K k) {
        List<V> res = map.get(k);
        if (res == null) return Collections.emptyList();
        else return res;
    }

    public List<V> remove(K k) { return map.remove(k); }

    public boolean containsKey(K k) { return get(k) != null; }

    @Override
    public String toString() { return map.toString(); }

    public void clear() { map.clear(); }

}
				       
