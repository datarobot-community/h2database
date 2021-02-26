/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.dullesopen.h2.external;

import org.h2.api.ErrorCode;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.index.BaseIndex;
import org.h2.index.Cursor;
import org.h2.index.IndexType;
import org.h2.message.DbException;
import org.h2.mvstore.DataUtils;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.h2.mvstore.db.MVIndex;
import org.h2.mvstore.db.ValueDataType;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.TableBase;
import org.h2.table.TableFilter;
import org.h2.util.New;
import org.h2.value.*;

import java.util.*;

/**
 * A set of indices stored in a separate MVStore and explicitly create by CREATE INDEX statement
 *
 * @author Pavel Ganelin
 */
public class DiskSecondaryIndex extends BaseIndex implements MVIndex {

    /**
     * The multi-value table.
     */
    final TableBase mvTable;

    private final int keyColumns;

    /**
     * map with the actual lookup keys for the index.
     */
    private MVMap<Value, Value> dataMap;

    public DiskSecondaryIndex(Database db, TableBase table, int id, String indexName, String prefixedIndexName,
                              IndexColumn[] columns, IndexType indexType, MVStore store) {
        this.mvTable = table;
        initBaseIndex(table, id, prefixedIndexName, columns, indexType);
        if (!database.isStarting()) {
            checkIndexColumnTypes(columns);
        }
        // always store the row key in the map key,
        // even for unique indexes, as some of the index columns could be null
        keyColumns = columns.length + 1;
        int[] sortTypes = new int[keyColumns];
        for (int i = 0; i < columns.length; i++) {
            sortTypes[i] = columns[i].sortType;
        }
        sortTypes[keyColumns - 1] = SortOrder.ASCENDING;
        ValueDataType keyType = new ValueDataType(
                db.getCompareMode(), db, sortTypes);
        ValueDataType valueType = new ValueDataType(null, null, null);
        dataMap = store.openMap(
                indexName, new MVMap.Builder<Value, Value>().keyType(keyType).valueType(valueType));
        if (!keyType.equals(dataMap.getKeyType())) {
            throw DbException.throwInternalError("Incompatible key type");
        }
    }

    @Override
    public void addRowsToBuffer(List<Row> rows, String bufferName) {
        MVMap<Value, Value> map = openMap(bufferName);
        for (Row row : rows) {
            ValueArray key = convertToKey(row);
            map.put(key, ValueNull.INSTANCE);
        }
    }

    @Override
    public void addBufferedRows(List<String> bufferNames) {
        ArrayList<String> mapNames = New.arrayList(bufferNames);
        final CompareMode compareMode = database.getCompareMode();
        /**
         * A source of values.
         */
        class Source implements Comparable<Source> {
            Value value;
            Iterator<Value> next;
            int sourceId;

            @Override
            public int compareTo(Source o) {
                int comp = value.compareTo(o.value, compareMode);
                if (comp == 0) {
                    comp = sourceId - o.sourceId;
                }
                return comp;
            }
        }
        TreeSet<Source> sources = new TreeSet<Source>();
        for (int i = 0; i < bufferNames.size(); i++) {
            MVMap<Value, Value> map = openMap(bufferNames.get(i));
            Iterator<Value> it = map.keyIterator(null);
            if (it.hasNext()) {
                Source s = new Source();
                s.value = it.next();
                s.next = it;
                s.sourceId = i;
                sources.add(s);
            }
        }
        try {
            while (true) {
                Source s = sources.first();
                Value v = s.value;

                if (indexType.isUnique()) {
                    Value[] array = ((ValueArray) v).getList();
                    // don't change the original value
                    array = Arrays.copyOf(array, array.length);
                    array[keyColumns - 1] = ValueLong.get(Long.MIN_VALUE);
                    ValueArray unique = ValueArray.get(array);
                    ValueArray key = (ValueArray) getLatestCeilingKey(dataMap, unique);
                    if (key != null) {
                        SearchRow r2 = convertToSearchRow(key);
                        SearchRow row = convertToSearchRow((ValueArray) v);
                        if (compareRows(row, r2) == 0) {
                            if (!containsNullAndAllowMultipleNull(r2)) {
                                throw getDuplicateKeyException(key.toString());
                            }
                        }
                    }
                }

                dataMap.put(v, ValueNull.INSTANCE);

                Iterator<Value> it = s.next;
                if (!it.hasNext()) {
                    sources.remove(s);
                    if (sources.size() == 0) {
                        break;
                    }
                } else {
                    Value nextValue = it.next();
                    sources.remove(s);
                    s.value = nextValue;
                    sources.add(s);
                }
            }
        } finally {
            for (String tempMapName : mapNames) {
                MVMap<Value, Value> map = openMap(tempMapName);
                map.getStore().removeMap(map);
            }
        }
    }

    private MVMap<Value, Value> openMap(String mapName) {
        int[] sortTypes = new int[keyColumns];
        for (int i = 0; i < indexColumns.length; i++) {
            sortTypes[i] = indexColumns[i].sortType;
        }
        sortTypes[keyColumns - 1] = SortOrder.ASCENDING;
        ValueDataType keyType = new ValueDataType(
                database.getCompareMode(), database, sortTypes);
        ValueDataType valueType = new ValueDataType(null, null, null);
        MVMap.Builder<Value, Value> builder =
                new MVMap.Builder<Value, Value>().keyType(keyType).valueType(valueType);
        MVMap<Value, Value> map = database.getMvStore().
                getStore().openMap(mapName, builder);
        if (!keyType.equals(map.getKeyType())) {
            throw DbException.throwInternalError("Incompatible key type");
        }
        return map;
    }

    @Override
    public void close(Session session) {
        // ok
    }

    @Override
    public void add(Session session, Row row) {
        ValueArray array = convertToKey(row);
        ValueArray unique = null;
        if (indexType.isUnique()) {
            // this will detect committed entries only
            unique = convertToKey(row);
            unique.getList()[keyColumns - 1] = ValueLong.get(Long.MIN_VALUE);
            ValueArray key = (ValueArray) getLatestCeilingKey(dataMap, unique);
            if (key != null) {
                SearchRow r2 = convertToSearchRow(key);
                if (compareRows(row, r2) == 0) {
                    if (!containsNullAndAllowMultipleNull(r2)) {
                        throw getDuplicateKeyException(key.toString());
                    }
                }
            }
        }
        dataMap.put(array, ValueNull.INSTANCE);
        if (indexType.isUnique()) {
            Iterator<Value> it = keyIterator(dataMap, unique, true);
            while (it.hasNext()) {
                ValueArray k = (ValueArray) it.next();
                SearchRow r2 = convertToSearchRow(k);
                if (compareRows(row, r2) != 0) {
                    break;
                }
                if (containsNullAndAllowMultipleNull(r2)) {
                    // this is allowed
                    continue;
                }
                if (isSameTransaction(dataMap, k)) {
                    continue;
                }
                if (dataMap.get(k) != null) {
                    // committed
                    throw getDuplicateKeyException(k.toString());
                }
                throw DbException.get(ErrorCode.CONCURRENT_UPDATE_1, table.getName());
            }
        }
    }

    @Override
    public void remove(Session session, Row row) {
        ValueArray array = convertToKey(row);
        Value old = dataMap.remove(array);
        if (old == null) {
            throw DbException.get(ErrorCode.ROW_NOT_FOUND_WHEN_DELETING_1,
                    getSQL() + ": " + row.getKey());
        }
    }

    @Override
    public Cursor find(Session session, SearchRow first, SearchRow last) {
        return find(session, first, false, last);
    }

    private Cursor find(Session session, SearchRow first, boolean bigger, SearchRow last) {
        ValueArray min = convertToKey(first);
        if (min != null) {
            min.getList()[keyColumns - 1] = ValueLong.get(Long.MIN_VALUE);
        }
        if (bigger && min != null) {
            // search for the next: first skip 1, then 2, 4, 8, until
            // we have a higher key; then skip 4, 2,...
            // (binary search), until 1
            int offset = 1;
            while (true) {
                ValueArray v = (ValueArray) relativeKey(dataMap, min, offset);
                if (v != null) {
                    boolean foundHigher = false;
                    for (int i = 0; i < keyColumns - 1; i++) {
                        int idx = columnIds[i];
                        Value b = first.getValue(idx);
                        if (b == null) {
                            break;
                        }
                        Value a = v.getList()[i];
                        if (database.compare(a, b) > 0) {
                            foundHigher = true;
                            break;
                        }
                    }
                    if (!foundHigher) {
                        offset += offset;
                        min = v;
                        continue;
                    }
                }
                if (offset > 1) {
                    offset /= 2;
                    continue;
                }
                if (dataMap.get(v) == null) {
                    min = (ValueArray) dataMap.higherKey(min);
                    if (min == null) {
                        break;
                    }
                    continue;
                }
                min = v;
                break;
            }
            if (min == null) {
                return new MVStoreCursor(session,
                        Collections.<Value>emptyList().iterator(), null);
            }
        }
        return new MVStoreCursor(session, dataMap.keyIterator(min), last);
    }

    private ValueArray convertToKey(SearchRow r) {
        if (r == null) {
            return null;
        }
        Value[] array = new Value[keyColumns];
        for (int i = 0; i < columns.length; i++) {
            Column c = columns[i];
            int idx = c.getColumnId();
            Value v = r.getValue(idx);
            if (v != null) {
                array[i] = v.convertTo(c.getType());
            }
        }
        array[keyColumns - 1] = ValueLong.get(r.getKey());
        return ValueArray.get(array);
    }

    /**
     * Convert array of values to a SearchRow.
     *
     * @param key the index key
     * @return the row
     */
    SearchRow convertToSearchRow(ValueArray key) {
        Value[] array = key.getList();
        SearchRow searchRow = mvTable.getTemplateRow();
        searchRow.setKey((array[array.length - 1]).getLong());
        Column[] cols = getColumns();
        for (int i = 0; i < array.length - 1; i++) {
            Column c = cols[i];
            int idx = c.getColumnId();
            Value v = array[i];
            searchRow.setValue(idx, v);
        }
        return searchRow;
    }

    @Override
    public TableBase getTable() {
        return mvTable;
    }

    @Override
    public double getCost(Session session, int[] masks, TableFilter[] filters, int filter,
                   SortOrder sortOrder, HashSet<Column> allColumnsSet)
    {
        return 10 * getCostRangeIndex(masks,
                dataMap.sizeAsLong(), filters, filter, sortOrder, false, allColumnsSet);
    }

    @Override
    public void remove(Session session) {
        dataMap.getStore().removeMap(dataMap);
    }

    @Override
    public void truncate(Session session) {
        dataMap.clear();
    }

    @Override
    public boolean canGetFirstOrLast() {
        return true;
    }

    @Override
    public Cursor findFirstOrLast(Session session, boolean first) {
        Value key = first ? dataMap.firstKey() : dataMap.lastKey();
        while (true) {
            if (key == null) {
                return new MVStoreCursor(session,
                        Collections.<Value>emptyList().iterator(), null);
            }
            if (((ValueArray) key).getList()[0] != ValueNull.INSTANCE) {
                break;
            }
            key = first ? dataMap.higherKey(key) : dataMap.lowerKey(key);
        }
        ArrayList<Value> list = New.arrayList();
        list.add(key);
        MVStoreCursor cursor = new MVStoreCursor(session, list.iterator(), null);
        cursor.next();
        return cursor;
    }

    @Override
    public boolean needRebuild() {
        return dataMap.sizeAsLong() == 0;
    }

    @Override
    public long getRowCount(Session session) {
        return dataMap.sizeAsLong();
    }

    @Override
    public long getRowCountApproximation() {
        return dataMap.sizeAsLong();
    }

    @Override
    public long getDiskSpaceUsed() {
        // TODO estimate disk space usage
        return 0;
    }

    @Override
    public boolean canFindNext() {
        return true;
    }

    @Override
    public Cursor findNext(Session session, SearchRow higherThan, SearchRow last) {
        return find(session, higherThan, true, last);
    }

    @Override
    public void checkRename() {
        // ok
    }

    public Value getLatestCeilingKey(MVMap<Value, Value> map, Value key) {
        Iterator<Value> cursor = map.keyIterator(key);
        while (cursor.hasNext()) {
            key = cursor.next();
            if (map.get(key) != null) {
                return key;
            }
        }
        return null;
    }

    public boolean isSameTransaction(MVMap<Value, Value> map, Value key) {
        return true;
    }

    public Value relativeKey(MVMap<Value, Value> map, Value key, long offset) {
        Value k = offset > 0 ? map.ceilingKey(key) : map.floorKey(key);
        if (k == null) {
            return k;
        }
        long index = map.getKeyIndex(k);
        return map.getKey(index + offset);
    }

    /**
     * Iterate over keys.
     *
     * @param from               the first key to return
     * @param includeUncommitted whether uncommitted entries should be
     *                           included
     * @return the iterator
     */
    public Iterator<Value> keyIterator(final MVMap<Value, Value> map, final Value from, final boolean includeUncommitted) {
        return new Iterator<Value>() {
            private Value currentKey = from;
            private org.h2.mvstore.Cursor<Value, Value> cursor = map.cursor(currentKey);

            {
                fetchNext();
            }

            private void fetchNext() {
                while (cursor.hasNext()) {
                    Value k = cursor.next();
                    currentKey = k;
                    if (includeUncommitted) {
                        return;
                    }
                    if (map.containsKey(k)) {
                        return;
                    }
                }
                currentKey = null;
            }

            @Override
            public boolean hasNext() {
                return currentKey != null;
            }

            @Override
            public Value next() {
                Value result = currentKey;
                fetchNext();
                return result;
            }

            @Override
            public void remove() {
                throw DataUtils.newUnsupportedOperationException(
                        "Removing is not supported");
            }
        };
    }

    /**
     * A cursor.
     */
    class MVStoreCursor implements Cursor {

        private final Session session;
        private final Iterator<Value> it;
        private final SearchRow last;
        private Value current;
        private SearchRow searchRow;
        private Row row;

        public MVStoreCursor(Session session, Iterator<Value> it, SearchRow last) {
            this.session = session;
            this.it = it;
            this.last = last;
        }

        @Override
        public Row get() {
            if (row == null) {
                SearchRow r = getSearchRow();
                if (r != null) {
                    row = mvTable.getRow(session, r.getKey());
                }
            }
            return row;
        }

        @Override
        public SearchRow getSearchRow() {
            if (searchRow == null) {
                if (current != null) {
                    searchRow = convertToSearchRow((ValueArray) current);
                }
            }
            return searchRow;
        }

        @Override
        public boolean next() {
            current = it.hasNext() ? it.next() : null;
            searchRow = null;
            if (current != null) {
                if (last != null && compareRows(getSearchRow(), last) > 0) {
                    searchRow = null;
                    current = null;
                }
            }
            row = null;
            return current != null;
        }

        @Override
        public boolean previous() {
            throw DbException.getUnsupportedException("previous");
        }

    }


}
