/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import java.util.ArrayList;
import java.util.HashSet;

import org.h2.api.ErrorCode;
import org.h2.engine.Constants;
import org.h2.engine.Session;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.TableFilter;
import org.h2.table.TableLink;
import org.h2.util.New;
import org.h2.util.StatementBuilder;
import org.h2.value.Value;
import org.h2.value.ValueNull;

/**
 * A linked index is a index for a linked (remote) table.
 * It is backed by an index on the remote table which is accessed over JDBC.
 */
public class LinkedIndex extends BaseIndex {

    private final TableLink link;
    private final String targetTableName;
    private long rowCount;
    private boolean view;
    private final List<TableLink.ColumnLinkMetaData> columnLinkMetaData;

    public LinkedIndex(TableLink table, int id, IndexColumn[] columns,
                       IndexType indexType, List<TableLink.ColumnLinkMetaData> columnLinkMetaData) {
        initBaseIndex(table, id, null, columns, indexType);
        link = table;
        view=link.isView();
        this.columnLinkMetaData = columnLinkMetaData;
        targetTableName = link.getQualifiedTable();
    }

    @Override
    public String getCreateSQL() {
        return null;
    }

    @Override
    public void close(Session session) {
        // nothing to do
    }

    private static boolean isNull(Value v) {
        return v == null || v == ValueNull.INSTANCE;
    }

    @Override
    public void add(Session session, Row row) {
        if (view)
            throw DbException.get(ErrorCode.OPERATION_NOT_SUPPORTED_WITH_LINKED_VIEW,
                    table.getName());

        ArrayList<Value> params = New.arrayList();
        StatementBuilder buff = new StatementBuilder("INSERT INTO ");
        buff.append(targetTableName).append(" VALUES(");
        List<TableLink.ColumnLinkMetaData> paramMetaData = New.arrayList();
        for (int i = 0; i < row.getColumnCount(); i++) {
            Value v = row.getValue(i);
            buff.appendExceptFirst(", ");
            if (v == null) {
                buff.append("DEFAULT");
            } else if (isNull(v)) {
                buff.append("NULL");
            } else {
                buff.append('?');
                params.add(v);
                paramMetaData.add(columnLinkMetaData.get(i));
            }
        }
        buff.append(')');
        String sql = buff.toString();
        try {
            link.execute(sql, params, true, paramMetaData);
            rowCount++;
        } catch (Exception e) {
            throw TableLink.wrapException(sql, e);
        }
    }

    @Override
    public Cursor find(Session session, SearchRow first, SearchRow last) {
        ArrayList<Value> params = New.arrayList();
        StatementBuilder buff = new StatementBuilder(view ?
                targetTableName :
                "SELECT * FROM " + targetTableName + " T");
        List<TableLink.ColumnLinkMetaData> paramMetaData = New.arrayList();
        for (int i = 0; first != null && i < first.getColumnCount(); i++) {
            Value v = first.getValue(i);
            if (v != null) {
                buff.appendOnlyFirst(" WHERE ");
                buff.appendExceptFirst(" AND ");
                Column col = table.getColumn(i);
                buff.append(col.getSQL());
                if (v == ValueNull.INSTANCE) {
                    buff.append(" IS NULL");
                } else {
                    buff.append(">=");
                    addParameter(buff, col);
                    params.add(v);
                    paramMetaData.add(columnLinkMetaData.get(i));
                }
            }
        }
        for (int i = 0; last != null && i < last.getColumnCount(); i++) {
            Value v = last.getValue(i);
            if (v != null) {
                buff.appendOnlyFirst(" WHERE ");
                buff.appendExceptFirst(" AND ");
                Column col = table.getColumn(i);
                buff.append(col.getSQL());
                if (v == ValueNull.INSTANCE) {
                    buff.append(" IS NULL");
                } else {
                    buff.append("<=");
                    addParameter(buff, col);
                    params.add(v);
                    paramMetaData.add(columnLinkMetaData.get(i));
                }
            }
        }
        String sql = buff.toString();
        try {
            PreparedStatement prep = link.execute(sql, params, false, paramMetaData);
            ResultSet rs = prep.getResultSet();
            return new LinkedCursor(link, rs, session, sql, prep);
        } catch (Exception e) {
            throw TableLink.wrapException(sql, e);
        }
    }

    private void addParameter(StatementBuilder buff, Column col) {
        if (col.getType() == Value.STRING_FIXED && link.isOracle()) {
            // workaround for Oracle
            // create table test(id int primary key, name char(15));
            // insert into test values(1, 'Hello')
            // select * from test where name = ? -- where ? = "Hello" > no rows
            buff.append("CAST(? AS CHAR(").append(col.getPrecision()).append("))");
        } else {
            buff.append('?');
        }
    }

    @Override
    public double getCost(Session session, int[] masks,
            TableFilter[] filters, int filter, SortOrder sortOrder,
            HashSet<Column> allColumnsSet) {

        long precalculatedRowCount = link.getPrecalculatedRowCount();
        if (precalculatedRowCount > 0) return 10 * getCostRangeIndex(masks, precalculatedRowCount,
                filters, filter, sortOrder, true, allColumnsSet);

        return 100 + getCostRangeIndex(masks, rowCount +
                Constants.COST_ROW_OFFSET, filters, filter, sortOrder, false, allColumnsSet);
    }

    @Override
    public void remove(Session session) {
        // nothing to do
    }

    @Override
    public void truncate(Session session) {
        // nothing to do
    }

    @Override
    public void checkRename() {
        throw DbException.getUnsupportedException("LINKED");
    }

    @Override
    public boolean needRebuild() {
        return false;
    }

    @Override
    public boolean canGetFirstOrLast() {
        return false;
    }

    @Override
    public Cursor findFirstOrLast(Session session, boolean first) {
        // TODO optimization: could get the first or last value (in any case;
        // maybe not optimized)
        throw DbException.getUnsupportedException("LINKED");
    }

    @Override
    public void remove(Session session, Row row) {
        if (view)
            throw DbException.get(ErrorCode.OPERATION_NOT_SUPPORTED_WITH_LINKED_VIEW,
                    table.getName(), null);
        ArrayList<Value> params = New.arrayList();
        List<TableLink.ColumnLinkMetaData> paramMetaData = New.arrayList();
        StatementBuilder buff = new StatementBuilder("DELETE FROM ");
        buff.append(targetTableName).append(" WHERE ");
        for (int i = 0; i < row.getColumnCount(); i++) {
            buff.appendExceptFirst("AND ");
            Column col = table.getColumn(i);
            buff.append(col.getSQL());
            Value v = row.getValue(i);
            if (isNull(v)) {
                buff.append(" IS NULL ");
            } else {
                buff.append('=');
                addParameter(buff, col);
                params.add(v);
                paramMetaData.add(columnLinkMetaData.get(i));
                buff.append(' ');
            }
        }
        String sql = buff.toString();
        try {
            PreparedStatement prep = link.execute(sql, params, false, paramMetaData);
            int count = prep.executeUpdate();
            link.reusePreparedStatement(prep, sql);
            rowCount -= count;
        } catch (Exception e) {
            throw TableLink.wrapException(sql, e);
        }
    }

    /**
     * Update a row using a UPDATE statement. This method is to be called if the
     * emit updates option is enabled.
     *
     * @param oldRow the old data
     * @param newRow the new data
     */
    public void update(Row oldRow, Row newRow) {
        if (view)
            throw DbException.get(ErrorCode.OPERATION_NOT_SUPPORTED_WITH_LINKED_VIEW,
                    table.getName(), null);
        ArrayList<Value> params = New.arrayList();
        List<TableLink.ColumnLinkMetaData> paramsMetaData = New.arrayList();
        StatementBuilder buff = new StatementBuilder("UPDATE ");
        buff.append(targetTableName).append(" SET ");
        for (int i = 0; i < newRow.getColumnCount(); i++) {
            buff.appendExceptFirst(", ");
            buff.append(table.getColumn(i).getSQL()).append('=');
            Value v = newRow.getValue(i);
            if (v == null) {
                buff.append("DEFAULT");
            } else {
                buff.append('?');
                params.add(v);
                paramsMetaData.add(columnLinkMetaData.get(i));
            }
        }
        buff.append(" WHERE ");
        buff.resetCount();
        for (int i = 0; i < oldRow.getColumnCount(); i++) {
            Column col = table.getColumn(i);
            buff.appendExceptFirst(" AND ");
            buff.append(col.getSQL());
            Value v = oldRow.getValue(i);
            if (isNull(v)) {
                buff.append(" IS NULL");
            } else {
                buff.append('=');
                params.add(v);
                paramsMetaData.add(columnLinkMetaData.get(i));
                addParameter(buff, col);
            }
        }
        String sql = buff.toString();
        try {
            link.execute(sql, params, true, paramsMetaData);
        } catch (Exception e) {
            throw TableLink.wrapException(sql, e);
        }
    }

    @Override
    public long getRowCount(Session session) {
        return rowCount;
    }

    @Override
    public long getRowCountApproximation() {
        return rowCount;
    }

    @Override
    public long getDiskSpaceUsed() {
        return 0;
    }
}
