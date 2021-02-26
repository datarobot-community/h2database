/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.util.HashMap;
import java.util.Set;

import org.h2.value.Value;

/**
 * This hash map supports keys of type Value.
 *
 * @param <V> the value type
 */
public class ValueHashMap<V> extends HashMap<Value,V> {

    public static <V> ValueHashMap<V> newInstance() {
        return new ValueHashMap<>();
    }

    public Set<Value> keys() {
        return keySet();
    }
}
