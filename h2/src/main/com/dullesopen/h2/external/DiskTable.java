package com.dullesopen.h2.external;

import org.h2.command.ddl.CreateTableData;
import org.h2.engine.Session;
import org.h2.index.IndexType;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.value.Value;

import java.io.File;

/**
 * A table stored in a separate MVStore. Used to emulate permanent external tables stored in a separate file.
 *
 * @author Pavel Ganelin
 */
public class DiskTable extends ExternalTable {
// ------------------------------ FIELDS ------------------------------

    private final MVStore tableStore;
    private final String filename;
    private final String indexFileName;
    private Column rowIdColumn;

// --------------------------- CONSTRUCTORS ---------------------------

    /**
     * create table from SQL statement.
     *
     * @param data
     * @param filename
     * @param indexFileName
     */
    DiskTable(CreateTableData data, String filename, String indexFileName, boolean append, boolean readOnly) {
        super(data, append ? Mode.ANY : Mode.INSERT,readOnly);
        this.filename = filename;
        this.indexFileName = indexFileName;
        this.isHidden = data.isHidden;
        tableStore = StoreCache.open(filename,readOnly);
        MVMap<String, Info.Table> meta = tableStore.openMap(Info.TABLES);
        meta.put(Info.TABLE, new Info.Table(data));
        addPrimaryIndex();
        markTableAsModified();
    }

    /**
     * Initialize the table.
     */
    private void addPrimaryIndex() {
        primaryIndex = new DiskPrimaryIndex(database,
                this, getId(),
                IndexColumn.wrap(getColumns()),
                IndexType.createScan(true),
                tableStore
        );
        indexes.add(primaryIndex);
    }

    /**
     * pickup existing table from the file.
     *
     * @param data
     * @param tableStore
     * @param indexFileName
     */
    DiskTable(Session session, CreateTableData data, MVStore tableStore, String filename, String indexFileName, boolean append, boolean readOnly) {
        super(data, append ? Mode.ANY : Mode.READ,readOnly);
        this.filename = filename;
        this.indexFileName = indexFileName;
        this.tableStore = tableStore;
        this.database = session.getDatabase();
        addPrimaryIndex();
        if (new File(this.indexFileName).exists()) {
            loadIndexStore(readOnly);
            readExistingIndices(session);
        }
    }

// --------------------- GETTER / SETTER METHODS ---------------------

    @Override
    protected String getIndexFileName() {
        return indexFileName;
    }

    @Override
    public Column getRowIdColumn() {
        if (rowIdColumn == null) {
            rowIdColumn = new Column(Column.ROWID, Value.LONG);
            rowIdColumn.setTable(this, -1);
        }
        return rowIdColumn;
    }

// ------------------------ CANONICAL METHODS ------------------------

    @Override
    public String toString() {
        return getSQL();
    }

// -------------------------- OTHER METHODS --------------------------

    @Override
    public void close(Session session) {
        super.close(session);
        StoreCache.close(tableStore, filename);
    }

    @Override
    public boolean isMVStore() {
        return true;
    }
}