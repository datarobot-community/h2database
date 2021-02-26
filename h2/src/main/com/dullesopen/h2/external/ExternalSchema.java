package com.dullesopen.h2.external;

import org.h2.api.ErrorCode;
import org.h2.command.ddl.CreateTableData;
import org.h2.engine.Database;
import org.h2.engine.DbObject;
import org.h2.engine.Session;
import org.h2.engine.User;
import org.h2.index.Index;
import org.h2.message.DbException;
import org.h2.schema.Schema;
import org.h2.schema.SchemaObject;
import org.h2.table.Table;

import java.util.*;

/**
 * Base class for directory based schemas
 *
 * @author Pavel Ganelin
 */
public abstract class ExternalSchema extends Schema {
// ------------------------------ FIELDS ------------------------------

    protected final Map<String, ExternalTable> tables = new LinkedHashMap<String, ExternalTable>();

    /**
     * during ALTER TABLES a table can be created and already represented in the external storage
     * but not added to the schema yet and so we should not load it on demand
     * (otherwise it triggers table already exists error)
     */
    private Set<String> altered = new LinkedHashSet<String>();
    ;

// --------------------------- CONSTRUCTORS ---------------------------

    @SuppressWarnings("WeakerAccess")
    protected ExternalSchema(Database database, int id, String schemaName, User owner, boolean system) {
        super(database, id, schemaName, owner, system);
    }

// ------------------------ INTERFACE METHODS ------------------------


// --------------------- Interface DbObject ---------------------

    public String getCreateSQL() {
        StringBuffer buf = new StringBuffer();
        buf.append("CREATE SCHEMA ");
        buf.append(getSQL());
        buf.append(" AUTHORIZATION ");
        buf.append(getOwner().getSQL());
        buf.append(" EXTERNAL ");
        buf.append("(\"");
        buf.append(getClass().getName());
        buf.append("\")");
        buf.append(getOwner().getSQL());
        return buf.toString();
    }

// -------------------------- OTHER METHODS --------------------------

    @Override
    public void close(Session session) {
        tables.clear();
        tablesAndViews.clear();
        indexes.clear();
    }

    @Override
    public void commit(Session session, boolean ddl) {
        if (!ddl) {
            boolean uncommitted = false;
            for (ExternalTable table : tables.values()) {
                uncommitted = table.isCommitRequired();
                if (uncommitted)
                    break;
            }
            if (uncommitted) {
                for (Table table : tables.values()) {
                    table.close(session);
                }
                tables.clear();
                tablesAndViews.clear();
                indexes.clear();
                invalidateCache();
            }
        }
    }

    /**
     * H2 database uses queries cache to reuse previous queries and another cache for prepared statements.
     * As a result DiskTable can persist as an object in misc caches even when it was removed from the corresponding schema.
     * invalidate all prepared statements and clear all cached statements. Incrementing NextModificationMetaId forces
     * database to invalidate all the caches later.
     */
    private void invalidateCache() {
        database.getNextModificationMetaId();
    }

    @Override
    public Table createTable(CreateTableData data) {
        data.schema = this;
        ExternalTable table = createExternalTable(data);
        altered.add(table.getName());
        return table;
    }

    /**
     * factory method to create table from SQL statement
     */

    protected abstract ExternalTable createExternalTable(CreateTableData data);


    public Index findIndex(Session session, String name) {
        Index index = super.findIndex(session, name);
        if (index == null && name.contains(ExternalTable.NAME_INDEX_SEPARATOR)) {
            // the table may be flushed to disk together with indices, we may need to bring it back to memory
            int i = name.indexOf(ExternalTable.NAME_INDEX_SEPARATOR);
            String tableName = name.substring(0, i);
            Table table = findTableOrView(session, tableName);
            if (table != null)
                index = super.findIndex(session, name);
        }
        return index;
    }

    private Table findActiveTableOrView(Session session, String name) {
        Table table = super.findTableOrView(session, name);
        if (table != null)
            return table;
        else {
            return tables.get(name);
        }
    }

    @Override
    public Table findTableOrView(Session session, String name) {
        Table table = findActiveTableOrView(session, name);
        if (table != null)
            return table;
        if (altered.contains(name))
            return null;

        if (disableLoadOnDemand())
            return null;
        else {
            Table old = loadTableOrViewOnDemand(session, name);
            if (old != null) {
                add(old);
            }
            return old;
        }
    }

    @SuppressWarnings("WeakerAccess")
    protected boolean disableLoadOnDemand() {
        return false;
    }

    @Override
    public Table getTableOrViewByName(Session session, String name) {
        Table table = super.getTableOrViewByName(session, name);
        if (table != null)
            return table;
        table = loadTableOrViewOnDemand(session, name);
        if (table != null)
            add(table);
        return table;
    }


    /**
     * load preexisting table or view from the external storage and create the corresponding table in H2 database
     */
    abstract protected Table loadTableOrViewOnDemand(Session session, String name);

    /**
     * this method is also called during connection close and we do not want to load all tables on demand.
     * When connection is closing the force is false to indicate that if the external table was not loaded so far we can just ignore it
     * the session argument is false
     *
     * @param session
     * @param force
     * @return
     */
    public ArrayList<Table> getAllTablesAndViews(Session session, boolean force) {
        ArrayList<Table> list = super.getAllTablesAndViews(session, force);
        if (force) {
            List<String> uppers = getMembersList();
            for (String upper : uppers) {
                Table table = tablesAndViews.get(upper);
                if (table == null) {
                    table = loadTableOrViewOnDemand(session, upper);
                    if (table != null) {
                        list.add(table);
                        add(table);
                    }
                }
            }
        }
        return list;
    }

    protected abstract List<String> getMembersList();


    @Override
    public Table getTableOrView(Session session, String name) {
        Table table = findTableOrView(session, name);
        if (table == null) {
            throw DbException.get(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, name);
        }
        return table;
    }

    @Override
    public void remove(Session session, SchemaObject obj) {
        super.remove(session, obj);
        tables.remove(obj.getName());
    }

    /**
     * if Table is active than we need to close and flush it, otherwise
     * only rename the external resources
     *
     * @param session
     * @param obj     the object to rename
     * @param newName the new name
     */
    @Override
    public void rename(Session session, SchemaObject obj, String newName) {
        String oldname = obj.getName();
        Table t = findActiveTableOrView(session, obj.getName());
        if (t != null) {
            super.rename(session, obj, newName);
            if (obj.getType() == DbObject.TABLE_OR_VIEW) {
                ExternalTable table = tables.remove(oldname);
                if (table != null) {
                    table.close(session);
                    // remove from H2 map to force loading of the external table on demand
                    remove(session, obj);

                    tablesAndViews.remove(oldname);
                    String prefixed = oldname + ExternalTable.NAME_INDEX_SEPARATOR;
                    for (Iterator<Map.Entry<String, Index>> it = indexes.entrySet().iterator(); it.hasNext(); ) {
                        Map.Entry<String, Index> entry = it.next();
                        if (entry.getKey().startsWith(prefixed)) {
                            it.remove();
                        }
                    }
                    renameExternalResource(oldname, newName);
                    // reload table with new indices
                    findTableOrView(session, newName);
                }
            }
        } else
            renameExternalResource(oldname, newName);

    }

    /**
     * hook to rename external resources after ALTER TABLE
     *
     * @param oldName
     * @param newName
     */
    protected void renameExternalResource(String oldName, String newName) {
    }

    @Override
    public void add(SchemaObject obj) {
        super.add(obj);
        if (obj instanceof ExternalTable) {
            tables.put(obj.getName(), (ExternalTable) obj);
            altered.remove(obj.getName());
        }

    }
}
