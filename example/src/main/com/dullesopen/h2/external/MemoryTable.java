package com.dullesopen.h2.external;

import org.h2.command.ddl.CreateTableData;
import org.h2.index.IndexType;
import org.h2.result.Row;
import org.h2.result.RowImpl;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.TableType;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Pavel Ganelin
 */

public class MemoryTable extends ExternalTable {
// ------------------------------ FIELDS ------------------------------

    long firstFree = -1;
    List<Row> rows = new ArrayList<Row>();
    private final String filename;

// --------------------------- CONSTRUCTORS ---------------------------

    public MemoryTable(CreateTableData data, String filename) {
        super(data, Mode.ANY,false);
        this.filename = filename;
        setColumns(data.columns.toArray(new Column[data.columns.size()]));
        primaryIndex = new MemoryIndex(this, data.id, IndexColumn.wrap(columns), IndexType.createScan(data.persistData));
        indexes.add(primaryIndex);
        firstFree = -1;
    }

// -------------------------- OTHER METHODS --------------------------

    void add(Row row) {
        if (firstFree == -1) {
            int key = rows.size();
            row.setKey(key);
            rows.add(row);
        } else {
            long key = firstFree;
            Row free = rows.get((int) key);
            firstFree = free.getKey();
            row.setKey(key);
            rows.set((int) key, row);
        }
    }

    @Override
    protected String getIndexFileName() {
        return filename;
    }

    Row getNextRow(Row row) {
        long key;
        if (row == null) {
            key = -1;
        } else {
            key = row.getKey();
        }
        while (true) {
            key++;
            if (key >= rows.size()) {
                return null;
            }
            row = rows.get((int) key);
            if (!row.isEmpty()) {
                return row;
            }
        }
    }

    public TableType getTableType() {
        return TableType.TABLE_LINK;
    }

    void remove(Row row) {
        Row free = new RowImpl(null, 0);
        free.setKey(firstFree);
        long key = row.getKey();
        rows.set((int) key, free);
        firstFree = key;
    }

    void reset() {
        rows = new ArrayList<Row>();
        firstFree = -1;
    }
}
