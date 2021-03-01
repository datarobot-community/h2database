package org.h2.contrib.external;

import org.h2.api.DatabaseEventListener;
import org.h2.api.ErrorCode;
import org.h2.command.ddl.Analyze;
import org.h2.command.ddl.CreateTableData;
import org.h2.constraint.Constraint;
import org.h2.constraint.ConstraintReferential;
import org.h2.contrib.external.support.ExternalSecondaryIndex;
import org.h2.engine.DbObject;
import org.h2.engine.Session;
import org.h2.engine.SysProperties;
import org.h2.index.Cursor;
import org.h2.index.Index;
import org.h2.index.IndexType;
import org.h2.index.MultiVersionIndex;
import org.h2.message.DbException;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.h2.mvstore.db.MVIndex;
import org.h2.result.Row;
import org.h2.result.SortOrder;
import org.h2.schema.SchemaObject;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.TableBase;
import org.h2.table.TableType;
import org.h2.util.MathUtils;
import org.h2.util.New;
import org.h2.value.Value;

import java.util.*;

/**
 * External table with by indexes managed by MVStore
 *
 * @author Pavel Ganelin
 */
public abstract class ExternalTable extends TableBase {
// ------------------------------ FIELDS ------------------------------

    public static final String NAME_INDEX_SEPARATOR = "$";

    private static boolean PREFIX_INDEX_NAME_WITH_TABLE_NAME = true;
    protected final ArrayList<Index> indexes = New.arrayList();
    /**
     * Separate MV store for index file
     */
    protected MVStore indexStore;
    protected ExternalIndex primaryIndex;
    private final long createModificationId;
    private long lastModificationId;

    private int changesSinceAnalyze;
    private int temporaryMapId;
    private int nextAnalyze;

    /**
     * This variable is used to allow disk table to emulate two types of external storage.
     * When append is true(default) the external storage allow all types of modification an any moment.
     * When append is false the external storage supports only adding rows before the first commit.
     * Remove operation is not supported and after the first commit the external storage is reopened
     * for read only mode only
     */

    private final Mode mode;
    private boolean readOnly;

    private static final int SCALE_ROW_SIZE = 10;
// --------------------------- CONSTRUCTORS ---------------------------

    public ExternalTable(CreateTableData data, Mode mode, boolean readOnly) {
        super(data);
        this.mode = mode;
        this.readOnly = readOnly;
        nextAnalyze = database.getSettings().analyzeAuto;
        createModificationId = getMaxDataModificationId();
    }

    @Override
    public long getMaxDataModificationId() {
        return lastModificationId;
    }

// --------------------- GETTER / SETTER METHODS ---------------------

    @Override
    public ArrayList<Index> getIndexes() {
        return indexes;
    }

// ------------------------ INTERFACE METHODS ------------------------


// --------------------- Interface DbObject ---------------------

    @Override
    public void removeChildrenAndResources(Session session) {
        super.removeChildrenAndResources(session);
        // go backwards because database.removeIndex will
        // call table.removeIndex
        while (indexes.size() > 1) {
            Index index = indexes.get(1);
            if (index.getName() != null) {
                database.removeSchemaObject(session, index);
            }
            // needed for session temporary indexes
            indexes.remove(index);
        }
        if (SysProperties.CHECK) {
            for (SchemaObject obj : database.getAllSchemaObjects(DbObject.INDEX)) {
                Index index = (Index) obj;
                if (index.getTable() == this) {
                    DbException.throwInternalError(
                            "index not dropped: " + index.getName());
                }
            }
        }
        primaryIndex.remove(session);
        database.removeMeta(session, getId());
        primaryIndex = null;
        close(session);
        invalidate();
    }

    @Override
    public void checkRename() {
        // ok
    }

// -------------------------- OTHER METHODS --------------------------

    @Override
    public Index addIndex(Session session, String indexName, int indexId,
                          IndexColumn[] cols, IndexType indexType, boolean create,
                          String indexComment) {
        String prefixedIndexName = PREFIX_INDEX_NAME_WITH_TABLE_NAME ? getName() + NAME_INDEX_SEPARATOR + indexName : getName();
        if (indexType.isPrimaryKey()) {
            for (IndexColumn c : cols) {
                Column column = c.column;
                if (column.isNullable()) {
                    throw DbException.get(
                            ErrorCode.COLUMN_MUST_NOT_BE_NULLABLE_1,
                            column.getName());
                }
                column.setPrimaryKey(true);
            }
        }
        boolean isSessionTemporary = isTemporary() && !isGlobalTemporary();
        if (!isSessionTemporary) {
            database.lockMeta(session);
        }
        Index index;
        // TODO support in-memory indexes
        //  if (isPersistIndexes() && indexType.isPersistent()) {
        int mainIndexColumn;
        mainIndexColumn = getMainIndexColumn(indexType, cols);
        if (indexStore == null)
            loadIndexStore(readOnly);
        if (primaryIndex.getRowCountMax() != 0) {
            mainIndexColumn = -1;
        }
        if (mainIndexColumn != -1) {
            primaryIndex.setMainIndexColumn(mainIndexColumn);
            index = primaryIndex;
        } else {
            ExternalSecondaryIndex i = new ExternalSecondaryIndex(session.getDatabase(),
                    this, indexId,
                    indexName,
                    prefixedIndexName, cols, indexType, indexStore);

            MVMap<String, Info.IndexData> indices = indexStore.openMap(Info.INDICES, new MVMap.Builder<String, Info.IndexData>());
            if (indices.containsKey(indexName))
                throw DbException.get(ErrorCode.INDEX_ALREADY_EXISTS_1, indexName);

            // use original index name, so that map will not have table name inside it
            // If we rename table and index file the index within table still will be valid.
            indices.put(indexName, new Info.IndexData(indexType, cols));

            index = i;
            if (i.needRebuild()) {
                rebuildIndex(session, i, prefixedIndexName);
            }
        }
        index.setTemporary(isTemporary());
        if (index.getCreateSQL() != null) {
            index.setComment(indexComment);
            if (isSessionTemporary) {
                session.addLocalTempTableIndex(index);
            } else {
                database.addSchemaObject(session, index);
            }
        }
        indexes.add(index);
        setModified();
        return index;
    }

    protected abstract String getIndexFileName();

    protected int getMainIndexColumn(IndexType indexType, IndexColumn[] cols) {
        if (primaryIndex.getMainIndexColumn() != -1) {
            return -1;
        }
        if (!indexType.isPrimaryKey() || cols.length != 1) {
            return -1;
        }
        IndexColumn first = cols[0];
        if (first.sortType != SortOrder.ASCENDING) {
            return -1;
        }
        switch (first.column.getType()) {
            case Value.BYTE:
            case Value.SHORT:
            case Value.INT:
            case Value.LONG:
                break;
            default:
                return -1;
        }
        return first.column.getColumnId();
    }

    protected void loadIndexStore(boolean readOnly) {

        String indexFileName = getIndexFileName();

        indexStore = StoreCache.open(indexFileName, readOnly);
    }

    protected void rebuildIndex(Session session, MVIndex index, String indexName) {
        try {
            rebuildIndexBlockMerge(session, index);
        } catch (DbException e) {
            getSchema().freeUniqueName(indexName);
            try {
                index.remove(session);
            } catch (DbException e2) {
                // this could happen, for example on failure in the storage
                // but if that is not the case it means
                // there is something wrong with the database
                trace.error(e2, "could not remove index");
                throw e2;
            }
            throw e;
        }
    }

    private void rebuildIndexBlockMerge(Session session, MVIndex index) {
        // Read entries in memory, sort them, write to a new map (in sorted
        // order); repeat (using a new map for every block of 1 MB) until all
        // record are read. Merge all maps to the target (using merge sort;
        // duplicates are detected in the target). For randomly ordered data,
        // this should use relatively few write operations.
        // A possible optimization is: change the buffer size from "row count"
        // to "amount of memory", and buffer index keys instead of rows.
        Index scan = getScanIndex(session);
        long remaining = scan.getRowCount(session);
        long total = remaining;
        Cursor cursor = scan.find(session, null, null);
        long i = 0;

        int bufferSize=0;
        ArrayList<Row> buffer = null;

        String n = getName() + ":" + index.getName();
        int t = MathUtils.convertLongToInt(total);
        ArrayList<String> bufferNames = New.arrayList();
        while (cursor.next()) {
            Row row = cursor.get();
            if (buffer==null) {
                bufferSize = estimateBufferSize(row);
                buffer = New.arrayList(bufferSize);
            }
            buffer.add(row);
            database.setProgress(DatabaseEventListener.STATE_CREATE_INDEX, n,
                    MathUtils.convertLongToInt(i++), t);
            if (buffer.size() >= bufferSize) {
                sortRows(buffer, index);
                String mapName = nextTemporaryMapName();
                index.addRowsToBuffer(buffer, mapName);
                bufferNames.add(mapName);
                buffer.clear();
            }
            remaining--;
        }
        if (buffer==null) {
            buffer = New.arrayList(0);
        }
        sortRows(buffer, index);
        if (bufferNames.size() > 0) {
            String mapName = nextTemporaryMapName();
            index.addRowsToBuffer(buffer, mapName);
            bufferNames.add(mapName);
            buffer.clear();
            index.addBufferedRows(bufferNames);
        } else {
            addRowsToIndex(session, buffer, index);
        }
        if (SysProperties.CHECK && remaining != 0) {
            DbException.throwInternalError("rowcount remaining=" + remaining
                    + " " + getName());
        }
    }

    private int estimateBufferSize(Row row) {
        return SysProperties.MAX_MEMORY_ROWS / 2 * SCALE_ROW_SIZE / Math.max(row.getColumnCount(), SCALE_ROW_SIZE);
    }

    private int scale(int maxMemoryRows, int length) {
        return maxMemoryRows == Integer.MAX_VALUE || length <= SCALE_ROW_SIZE ?
                maxMemoryRows :
                maxMemoryRows * SCALE_ROW_SIZE / length;
    }


    @Override
    public Index getScanIndex(Session session) {
        if (mode == Mode.INSERT)
            throw DbException.get(ErrorCode.TABLE_IS_INSERT_ONLY, getName());
        return primaryIndex;
    }

    public synchronized String nextTemporaryMapName() {
        return "temp." + temporaryMapId++;
    }

    private static void addRowsToIndex(Session session, ArrayList<Row> list,
                                       Index index) {
        sortRows(list, index);
        for (Row row : list) {
            index.add(session, row);
        }
        list.clear();
    }

    private static void sortRows(ArrayList<Row> list, final Index index) {
        Collections.sort(list, new Comparator<Row>() {
            @Override
            public int compare(Row r1, Row r2) {
                return index.compareRows(r1, r2);
            }
        });
    }

    @Override
    public void addRow(Session session, Row row) {
        if (mode == Mode.READ)
            throw DbException.get(ErrorCode.TABLE_IS_READ_ONLY, getName());

        markTableAsModified();
        try {
            for (int i = 0, size = indexes.size(); i < size; i++) {
                Index index = indexes.get(i);
                index.add(session, row);
            }
        } catch (Throwable e) {
            DbException de = DbException.convert(e);
            if (de.getErrorCode() == ErrorCode.DUPLICATE_KEY_1) {
                for (int j = 0; j < indexes.size(); j++) {
                    Index index = indexes.get(j);
                    if (index.getIndexType().isUnique() &&
                            index instanceof MultiVersionIndex) {
                        MultiVersionIndex mv = (MultiVersionIndex) index;
                        if (mv.isUncommittedFromOtherSession(session, row)) {
                            throw DbException.get(
                                    ErrorCode.CONCURRENT_UPDATE_1,
                                    index.getName());
                        }
                    }
                }
            }
            throw de;
        }
        analyzeIfRequired(session);
    }

    /**
     * mark table as modified so that commit will trigger writing the data to external table
     */
    protected void markTableAsModified() {
        lastModificationId = database.getNextModificationDataId();
    }

    private void analyzeIfRequired(Session session) {
        if (nextAnalyze == 0 || nextAnalyze > changesSinceAnalyze++) {
            return;
        }
        changesSinceAnalyze = 0;
        int n = 2 * nextAnalyze;
        if (n > 0) {
            nextAnalyze = n;
        }
        int rows = session.getDatabase().getSettings().analyzeSample / 10;
        Analyze.analyzeTable(session, this, rows, false);
    }

    @Override
    public boolean canDrop() {
        return true;
    }

    @Override
    public boolean canGetRowCount() {
        return true;
    }

    @Override
    public boolean canTruncate() {
        if (getCheckForeignKeyConstraints() && database.getReferentialIntegrity()) {
            ArrayList<Constraint> constraints = getConstraints();
            if (constraints != null) {
                for (int i = 0, size = constraints.size(); i < size; i++) {
                    Constraint c = constraints.get(i);
                    if (!(c.getConstraintType().equals(Constraint.REFERENTIAL))) {
                        continue;
                    }
                    ConstraintReferential ref = (ConstraintReferential) c;
                    if (ref.getRefTable() == this) {
                        return false;
                    }
                }
            }
        }
        return true;
    }

    @Override
    public ArrayList<Session> checkDeadlock(Session session, Session clash,
                                            Set<Session> visited) {
        return null;
    }

    @Override
    public void checkSupportAlter() {
        // ok
    }

    @Override
    public void close(Session session) {
        if (indexStore != null) {
            String indexFileName = getIndexFileName();
            StoreCache.close(indexStore, indexFileName);
            indexStore = null;
        }
    }

    @Override
    public long getDiskSpaceUsed() {
        return primaryIndex.getDiskSpaceUsed();
    }

    @Override
    public Row getRow(Session session, long key) {
        return primaryIndex.getRow(session, key);
    }

    @Override
    public long getRowCount(Session session) {
        return primaryIndex.getRowCount(session);
    }

    @Override
    public long getRowCountApproximation() {
        return primaryIndex.getRowCountApproximation();
    }

    @Override
    public TableType getTableType() {
        return TableType.TABLE;
    }

    @Override
    public Index getUniqueIndex() {
        return primaryIndex;
    }

    protected boolean isCommitRequired() {
        switch (mode) {
            case ANY:
                return isDirty();
            case INSERT:
                return true;
            case READ:
                return false;
            default:
                throw new IllegalArgumentException();
        }
    }

    protected boolean isDirty() {
        return getMaxDataModificationId() != createModificationId;
    }

    @Override
    public boolean isDeterministic() {
        return true;
    }

    @Override
    public boolean isLockedExclusively() {
        return false;
    }

    @Override
    public boolean isLockedExclusivelyBy(Session session) {
        return false;
    }

    @Override
    public boolean lock(Session session, boolean exclusive,
                     boolean forceLockEvenInMvcc) {
        return false;
    }

    protected void readExistingIndices(Session session) {
        MVMap<String, Info.IndexData> indices = indexStore.<String, Info.IndexData>openMap(Info.INDICES);
        for (Map.Entry<String, Info.IndexData> entry : indices.entrySet()) {
            String indexName = PREFIX_INDEX_NAME_WITH_TABLE_NAME ? getName() + NAME_INDEX_SEPARATOR + entry.getKey() : entry.getKey();
            entry.getKey();
            Info.IndexData value = entry.getValue();
            int indexId = database.allocateObjectId();
            IndexColumn[] columns = value.export();
            IndexColumn.mapColumns(columns, this);
            ExternalSecondaryIndex index = new ExternalSecondaryIndex(database,
                    this, indexId,
                    entry.getKey(),
                    indexName, columns, value.createIndexType(), indexStore);
            indexes.add(index);
            database.addSchemaObject(session, index);
        }
    }

    @Override
    public void removeIndex(Index index) {
        super.removeIndex(index);
        MVMap<String, Info.IndexData> indices = indexStore.openMap(Info.INDICES, new MVMap.Builder<String, Info.IndexData>());
        String indexName = index.getName();
        if (PREFIX_INDEX_NAME_WITH_TABLE_NAME) {
            String prefix = getName() + NAME_INDEX_SEPARATOR;
            if (!indexName.startsWith(prefix))
                throw DbException.get(ErrorCode.INDEX_NOT_FOUND_1, indexName);
            indexName = indexName.substring(prefix.length());
        }
        if (!indices.containsKey(indexName))
            throw DbException.get(ErrorCode.INDEX_NOT_FOUND_1, indexName);

        indices.remove(indexName);
    }

    @Override
    public void removeRow(Session session, Row row) {
        if (mode != Mode.ANY)
            throw DbException.get(ErrorCode.TABLE_IS_READ_ONLY, getName());
        markTableAsModified();
        try {
            for (int i = indexes.size() - 1; i >= 0; i--) {
                Index index = indexes.get(i);
                index.remove(session, row);
            }
        } catch (Throwable e) {
            throw DbException.convert(e);
        }
        analyzeIfRequired(session);
    }

    @Override
    public void truncate(Session session) {
        if (mode != Mode.ANY)
            throw DbException.get(ErrorCode.TABLE_IS_READ_ONLY, getName());
        markTableAsModified();
        for (int i = indexes.size() - 1; i >= 0; i--) {
            Index index = indexes.get(i);
            index.truncate(session);
        }
        changesSinceAnalyze = 0;
    }

    @Override
    public void unlock(Session s) {
    }

// -------------------------- ENUMERATIONS --------------------------

    /**
     * External table can be opened in three different modes:
     */
    public enum Mode {
        /**
         * READ only, any modifications are disabled
         */
        READ,
        /**
         * INSERT only mode. READ and any other modifications are disabled
         */
        INSERT,
        /**
         * Any operations are permitted.
         */
        ANY,
    }
}
