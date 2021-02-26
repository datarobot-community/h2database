/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 *
 * added by PG:
 * based on MVPrimaryIndex; instead of referencing to MVTable refers to ExternalTable
 *
 */
package com.dullesopen.h2.external;

import org.h2.api.ErrorCode;
import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.index.Cursor;
import org.h2.index.IndexType;
import org.h2.message.DbException;
import org.h2.mvstore.DataUtils;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.h2.mvstore.db.ValueDataType;
import org.h2.result.Row;
import org.h2.result.RowImpl;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.TableFilter;
import org.h2.value.Value;
import org.h2.value.ValueArray;
import org.h2.value.ValueLong;

import java.util.*;
import java.util.Map.Entry;

/**
 * A table stored in a MVStore.
 *
 * @author Pavel Ganelin
 */
public class DiskPrimaryIndex extends ExternalIndex {
// ------------------------------ FIELDS ------------------------------

    /**
     * The minimum long value.
     */
    static final ValueLong MIN = ValueLong.get(Long.MIN_VALUE);

    /**
     * The maximum long value.
     */
    static final ValueLong MAX = ValueLong.get(Long.MAX_VALUE);

    /**
     * map with the actual table data
     */
    private MVMap<Value, Value> dataMap;

    private long lastKey;
    private int mainIndexColumn = -1;

// --------------------------- CONSTRUCTORS ---------------------------

    public DiskPrimaryIndex(Database db, DiskTable table, int id,
                            IndexColumn[] columns, IndexType indexType, MVStore store) {
        initBaseIndex(table, id, table.getName() + "_DATA", columns, indexType);
        int[] sortTypes = new int[columns.length];
        for (int i = 0; i < columns.length; i++) {
            sortTypes[i] = SortOrder.ASCENDING;
        }
        ValueDataType keyType = new ValueDataType(null, null, null);
        ValueDataType valueType = new ValueDataType(db.getCompareMode(), db,
                sortTypes);
        dataMap = store.openMap(Info.PRIMARY, new MVMap.Builder<Value, Value>().keyType(keyType)
                .valueType(valueType));
        Value k = dataMap.lastKey();
        lastKey = k == null ? 0 : k.getLong();
    }

// --------------------- GETTER / SETTER METHODS ---------------------

    public int getMainIndexColumn() {
        return mainIndexColumn;
    }

    public void setMainIndexColumn(int mainIndexColumn) {
        this.mainIndexColumn = mainIndexColumn;
    }

// ------------------------ INTERFACE METHODS ------------------------


// --------------------- Interface DbObject ---------------------

    @Override
    public String getCreateSQL() {
        return null;
    }

    @Override
    public void checkRename() {
        // ok
    }

// --------------------- Interface Index ---------------------

    @Override
    public String getPlanSQL() {
        return table.getSQL() + ".tableScan";
    }

    @Override
    public void close(Session session) {
        // ok
    }

    @Override
    public void add(Session session, Row row) {
        if (mainIndexColumn == -1) {
            if (row.getKey() == 0) {
                row.setKey(++lastKey);
            }
        } else {
            long c = row.getValue(mainIndexColumn).getLong();
            row.setKey(c);
        }

        Value key = ValueLong.get(row.getKey());
        Value old = dataMap.get(key);
        if (old != null) {
            String sql = "PRIMARY KEY ON " + table.getSQL();
            if (mainIndexColumn >= 0 && mainIndexColumn < indexColumns.length) {
                sql += "(" + indexColumns[mainIndexColumn].getSQL() + ")";
            }
            DbException e = DbException.get(ErrorCode.DUPLICATE_KEY_1, sql);
            e.setSource(this);
            throw e;
        }
        dataMap.put(key, ValueArray.get(row.getValueList()));
        lastKey = Math.max(lastKey, row.getKey());
    }

    @Override
    public void remove(Session session, Row row) {
        Value old = dataMap.remove(ValueLong.get(row.getKey()));
        if (old == null) {
            throw DbException.get(ErrorCode.ROW_NOT_FOUND_WHEN_DELETING_1,
                    getSQL() + ": " + row.getKey());
        }
    }

    @Override
    public Cursor find(Session session, SearchRow first, SearchRow last) {
        ValueLong min, max;
        if (first == null) {
            min = MIN;
        } else if (mainIndexColumn < 0) {
            min = ValueLong.get(first.getKey());
        } else {
            ValueLong v = (ValueLong) first.getValue(mainIndexColumn);
            if (v == null) {
                min = ValueLong.get(first.getKey());
            } else {
                min = v;
            }
        }
        if (last == null) {
            max = MAX;
        } else if (mainIndexColumn < 0) {
            max = ValueLong.get(last.getKey());
        } else {
            ValueLong v = (ValueLong) last.getValue(mainIndexColumn);
            if (v == null) {
                max = ValueLong.get(last.getKey());
            } else {
                max = v;
            }
        }
        return new MVStoreCursor(entryIterator(dataMap, min), max);
    }

    @Override
    public void remove(Session session) {
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
        ValueLong v = (ValueLong) (first ? dataMap.firstKey() : dataMap.lastKey());
        if (v == null) {
            return new MVStoreCursor(Collections
                    .<Entry<Value, Value>>emptyList().iterator(), null);
        }
        Value value = dataMap.get(v);
        Entry<Value, Value> e = new DataUtils.MapEntry<Value, Value>(v, value);
        @SuppressWarnings("unchecked")
        List<Entry<Value, Value>> list = Arrays.asList(e);
        MVStoreCursor c = new MVStoreCursor(list.iterator(), v);
        c.next();
        return c;
    }

    @Override
    public boolean needRebuild() {
        return false;
    }

    @Override
    public long getRowCount(Session session) {
        return dataMap.sizeAsLong();
    }

    @Override
    public long getRowCountApproximation() {
        return getRowCountMax();
    }

    @Override
    public long getDiskSpaceUsed() {
        // TODO estimate disk space usage
        return 0;
    }

    @Override
    public int getColumnIndex(Column col) {
        // can not use this index - use the delegate index instead
        return -1;
    }

    @Override
    public Row getRow(Session session, long key) {
        Value v = dataMap.get(ValueLong.get(key));
        ValueArray array = (ValueArray) v;
        Row row = new RowImpl(array.getList(), 0);
        row.setKey(key);
        return row;
    }

    @Override
    public boolean isRowIdIndex() {
        return true;
    }

// -------------------------- OTHER METHODS --------------------------

    public Iterator<Entry<Value, Value>> entryIterator(final MVMap<Value, Value> map, final Value from) {
        return new Iterator<Entry<Value, Value>>() {
            private Entry<Value, Value> current;
            private Value currentKey = from;
            private org.h2.mvstore.Cursor<Value, Value> cursor = map.cursor(currentKey);

            {
                fetchNext();
            }

            private void fetchNext() {
                while (cursor.hasNext()) {
                    Value k;
                    k = cursor.next();
                    final Value key = k;
                    Value data = cursor.getValue();
                    if (data != null) {
                        @SuppressWarnings("unchecked") final Value value = data;
                        current = new DataUtils.MapEntry<Value, Value>(key, value);
                        currentKey = key;
                        return;
                    }
                }
                current = null;
                currentKey = null;
            }

            @Override
            public boolean hasNext() {
                return current != null;
            }

            @Override
            public Entry<Value, Value> next() {
                Entry<Value, Value> result = current;
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
     * The maximum number of rows, including uncommitted rows of any session.
     *
     * @return the maximum number of rows
     */
    public long getRowCountMax() {
        return dataMap.sizeAsLong();
    }

// -------------------------- INNER CLASSES --------------------------

    /**
     * A cursor.
     */
    class MVStoreCursor implements Cursor {
        private final Iterator<Entry<Value, Value>> it;
        private final ValueLong last;
        private Entry<Value, Value> current;
        private Row row;

        public MVStoreCursor(Iterator<Entry<Value, Value>> it, ValueLong last) {
            this.it = it;
            this.last = last;
        }

        @Override
        public Row get() {
            if (row == null) {
                if (current != null) {
                    ValueArray array = (ValueArray) current.getValue();
                    row = new RowImpl(array.getList(), 0);
                    row.setKey(current.getKey().getLong());
                }
            }
            return row;
        }

        @Override
        public SearchRow getSearchRow() {
            return get();
        }

        @Override
        public boolean next() {
            current = it.hasNext() ? it.next() : null;
            if (current != null && current.getKey().getLong() > last.getLong()) {
                current = null;
            }
            row = null;
            return current != null;
        }

        @Override
        public boolean previous() {
            throw DbException.getUnsupportedException("previous");
        }
    }

    @Override
    public double getCost(Session session, int[] masks,
                          TableFilter[] filters, int filter, SortOrder sortOrder,
                          HashSet<Column> allColumnsSet) {
        long cost = 10 * (dataMap.sizeAsLong() + Constants.COST_ROW_OFFSET);
        return cost;
    }

}
