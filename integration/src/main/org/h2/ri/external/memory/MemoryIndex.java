package org.h2.ri.external.memory;

import org.h2.contrib.external.ExternalIndex;
import org.h2.engine.Constants;
import org.h2.engine.Session;
import org.h2.index.Cursor;
import org.h2.index.IndexType;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.TableFilter;

import java.util.ArrayList;
import java.util.HashSet;

public class MemoryIndex extends ExternalIndex {
// ------------------------------ FIELDS ------------------------------

    private final MemoryTable tableData;

    private long rowCount;

// --------------------------- CONSTRUCTORS ---------------------------

    public MemoryIndex(MemoryTable table, int id, IndexColumn[] columns, IndexType indexType) {
        initBaseIndex(table, id, table.getName() + "_TABLE_SCAN", columns, indexType);
        tableData = table;
    }

// ------------------------ INTERFACE METHODS ------------------------


// --------------------- Interface DbObject ---------------------

    public String getCreateSQL() {
        return null;
    }

    public void checkRename() {
        throw DbException.getUnsupportedException("MEMORY");
    }

// --------------------- Interface Index ---------------------

    public void close(Session session) {
    }

    public void add(Session session, Row row) {
        tableData.add(row);
        rowCount++;
    }

    public void remove(Session session, Row row) {
        tableData.remove(row);
        rowCount--;
    }

    public Cursor find(Session session, SearchRow first, SearchRow last) {
        return new MemoryCursor(tableData);
    }

    @Override
    public double getCost(Session session, int[] masks,
                          TableFilter[] filters, int filter, SortOrder sortOrder,
                          HashSet<Column> allColumnsSet) {
        return tableData.getRowCount(session) + Constants.COST_ROW_OFFSET;
    }

    public void remove(Session session) {
        truncate(session);
    }

    public void truncate(Session session) {
        tableData.rows = new ArrayList<Row>();
        tableData.reset();
        rowCount = 0;
    }

    public boolean canGetFirstOrLast() {
        return false;
    }

    public Cursor findFirstOrLast(Session session, boolean first) {
        throw DbException.getUnsupportedException("MEMORY");
    }

    public boolean needRebuild() {
        return false;
    }

    public long getRowCount(Session session) {
        return rowCount;
    }

    public long getRowCountApproximation() {
        return rowCount;
    }

    @Override
    public long getDiskSpaceUsed() {
        return 0;
    }

    public int getColumnIndex(Column col) {
        // the scan index cannot use any columns
        return -1;
    }

    public Row getRow(Session session, long key) {
        return tableData.rows.get((int) key);
    }

// -------------------------- OTHER METHODS --------------------------

    @Override
    public int getMainIndexColumn() {
        return -1;
    }

    @Override
    public long getRowCountMax() {
        return rowCount;
    }

    @Override
    public void setMainIndexColumn(int mainIndexColumn) {
    }
}
