package com.dullesopen.h2.external;

/**
 * @author Pavel Ganelin
 * <p/>
 * Each table is saved in a separate file and indices for the table are saved in a separate file.
 * The schema have two mode of operations:
 * append=true (default). Any database operation is permitted on the disk table
 * append=false. The table has a lifecycle:
 * a) CREATE
 * b) execute any number (including zero) of INSERT statement
 * c) execute COMMIT
 * d) now table is read only and only SELECT statements are permitted in the table.
 */

import org.h2.api.ErrorCode;
import org.h2.command.ddl.CreateTableData;
import org.h2.engine.Database;
import org.h2.engine.DbObject;
import org.h2.engine.Session;
import org.h2.engine.User;
import org.h2.message.DbException;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.h2.table.Column;
import org.h2.table.Table;
import org.h2.table.TableView;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * directory based schema.
 *
 * @author Pavel Ganelin
 */
public class DiskSchema extends ExternalSchema {
// ------------------------------ FIELDS ------------------------------

    private static final String TABLE_EXTENSION = "table";
    private static final String VIEW_EXTENSION = "view";
    private static final String INDEX_EXTENSION = "index";

    private final String directory;
    private final boolean append;
    private final boolean readOnly;

// --------------------------- CONSTRUCTORS ---------------------------

    DiskSchema(Database database, int id, String schemaName, User owner, String options) {
        super(database, id, schemaName, owner, false);
        String[] ss = options.split("\\s*;\\s*");
        String directory = null;
        boolean append = true;
        boolean readOnly = false;
        for (String opt : ss) {
            int i = opt.indexOf("=");
            if (i != 0) {
                String name = opt.substring(0, i);
                String value = opt.substring(i + 1);
                if (name.equals("dir")) {
                    directory = value;
                    continue;
                } else if (name.equals("append")) {
                    append = Boolean.parseBoolean(value);
                    continue;
                } else if (name.equals("readonly")) {
                    readOnly = Boolean.parseBoolean(value);
                    continue;
                }
            }
            throw DbException.convert(new IllegalArgumentException("invalid options: " + options));
        }
        if (directory == null)
            throw DbException.convert(new IllegalArgumentException("invalid options: " + options));
        this.directory = directory;
        this.append = append;
        this.readOnly = readOnly;
    }

// -------------------------- OTHER METHODS --------------------------

    protected DiskTable createExternalTable(CreateTableData data) {
        String filename = filename(directory, data.tableName, TABLE_EXTENSION);
        return new DiskTable(data, filename, filename + "." + INDEX_EXTENSION, append, readOnly);
    }

    /**
     * generate file name for the disk file for MV storage
     *
     * @param dir
     * @param tableName
     * @param extension
     * @return
     */
    private static String filename(String dir, String tableName, String extension) {
        return dir + "/" + tableName.toLowerCase() + "." + extension;
    }

    public TableView createView(int id, final String name, String querySQL, Column[] columnTemplates, Session sysSession, boolean recursive) {
        try {
            String viewfile = filename(directory, name, VIEW_EXTENSION);
            File file = new File(viewfile);
            return new DiskView(this, id, name, querySQL, columnTemplates, sysSession, recursive, file);
        } catch (IOException e) {
            throw DbException.convert(e);
        }
    }

    public void flush(Session session) {
        for (Table table : tables.values()) {
            table.close(session);
        }
        tables.clear();
        tablesAndViews.clear();
        indexes.clear();
        session.getDatabase().getNextModificationMetaId();
    }

    /**
     * list all table files in the directory
     */
    protected List<String> getMembersList() {
        String[] list = new File(directory).list(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.endsWith("." + TABLE_EXTENSION) || name.endsWith("." + VIEW_EXTENSION);
            }
        });

        java.util.List<String> members = new ArrayList<String>();
        for (String basename : list) {
            int i = basename.lastIndexOf(".");
            String upper = basename.toUpperCase().substring(0, i);
            members.add(upper);
        }
        return members;
    }

    protected Table loadTableOrViewOnDemand(Session session, String name) {
        String tablefile = filename(directory, name, TABLE_EXTENSION);
        File view = new File(filename(directory, name, VIEW_EXTENSION));

        if (new File(tablefile).exists()) {
            return loadTable(this, session, name, tablefile);
        } else if (view.exists()) {
            return DiskView.loadFromFile(this, session.getDatabase().allocateObjectId(),
                    name, session, view);
        } else
            return null;
    }

    private static Table loadTable(DiskSchema diskSchema, Session session, String name, String filename) {
        MVStore store = StoreCache.open(filename, diskSchema.readOnly);
        MVMap<String, Info.Table> meta = store.openMap(Info.TABLES);
        Info.Table table = meta.get(Info.TABLE);
        CreateTableData data = table.export();

        data.id = session.getDatabase().allocateObjectId();
        data.tableName = name;
        data.create = true;
        data.persistData = true;
        data.persistIndexes = true;
        data.session = session;
        data.schema = diskSchema;

        return new DiskTable(session, data, store, filename,
                filename + "." + INDEX_EXTENSION, diskSchema.append, diskSchema.readOnly);
    }

    @Override
    public void removeExternalResources(int type, String name) {
        if (type == DbObject.TABLE_OR_VIEW) {
            String tablefile = filename(directory, name, TABLE_EXTENSION);
            new File(tablefile).delete();
            new File(tablefile + "." + INDEX_EXTENSION).delete();
            String viewfile = filename(directory, name, VIEW_EXTENSION);
            new File(viewfile).delete();
        }
    }

    @Override
    protected void renameExternalResource(String oldName, String newName) {
        String tablefile = filename(directory, oldName, TABLE_EXTENSION);

        String viewfile = filename(directory, oldName, VIEW_EXTENSION);

        if (new File(tablefile).exists()) {
            String newFile = filename(directory, newName, TABLE_EXTENSION);
            boolean success = new File(tablefile).renameTo(new File(newFile));
            if (!success) {
                String msg = MessageFormat.format("can not rename file: {0} to {1}", oldName, newName);
                throw DbException.get(ErrorCode.IO_EXCEPTION_1, msg);
            }
            if (new File(tablefile + "." + INDEX_EXTENSION).exists()) {
                success = new File(tablefile + "." + INDEX_EXTENSION).
                        renameTo(new File(newFile + "." + INDEX_EXTENSION));
                if (!success) {
                    String msg = MessageFormat.format("can not rename file: {0}.{1} to {2}.{1}", oldName, INDEX_EXTENSION, newName);
                    throw DbException.get(ErrorCode.IO_EXCEPTION_1, msg);
                }
            }
        } else if (new File(viewfile).exists()) {
            String newFile = filename(directory, newName, VIEW_EXTENSION);
            boolean success = new File(viewfile).renameTo(new File(newFile));
            if (!success) {
                String msg = MessageFormat.format("can not rename file: {0} to {1}", oldName, newName);
                throw DbException.get(ErrorCode.IO_EXCEPTION_1, msg);
            }
        }
    }
}
