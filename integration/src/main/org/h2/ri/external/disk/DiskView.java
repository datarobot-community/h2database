package org.h2.ri.external.disk;

import org.h2.api.ErrorCode;
import org.h2.engine.Session;
import org.h2.message.DbException;
import org.h2.table.Column;
import org.h2.table.TableView;

import java.io.*;
import java.text.MessageFormat;

class DiskView extends TableView {
// ------------------------------ FIELDS ------------------------------

    private final File file;

// -------------------------- STATIC METHODS --------------------------

    static DiskView loadFromFile(DiskSchema diskSchema, int id
            , String name, Session session, File file) {

        if (!file.exists())
            throw DbException.get(ErrorCode.IO_EXCEPTION_1,
                    "File not found:" + file.getAbsolutePath());

        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            int n = Integer.parseInt(reader.readLine());
            Column[] columns = new Column[n];
            for (int i = 0; i < columns.length; i++) {
                String[] s = reader.readLine().split(",");
                columns[i] = new Column(s[0],
                        Integer.parseInt(s[1]), Long.parseLong(s[2]), Integer.parseInt(s[3]), Integer.parseInt(s[4]));
            }
            StringBuilder buf = new StringBuilder();
            String s;
            while ((s = reader.readLine()) != null) {
                buf.append(s).append("\n");
            }
            String sql = buf.toString();
            return new DiskView(diskSchema, id, name, sql, columns, session, file);
        } catch (IOException e) {
            throw DbException.get(ErrorCode.IO_EXCEPTION_1, e, e.getMessage(), file.getAbsolutePath());
        }
    }

// --------------------------- CONSTRUCTORS ---------------------------

    /**
     * create VIEW from CREATE VIEW statement, assume that the external view file does not exists
     */
    private DiskView(DiskSchema diskSchema, int id, String name, String sql, Column[] columns, Session session, File file) {
        super(diskSchema, id, name, sql, null, columns, session, false);
        this.file = file;
    }

    /**
     * load existing view from the external file
     */
    DiskView(DiskSchema diskSchema, int id, String name, String querySQL, Column[] columnTemplates, Session sysSession, boolean recursive, File file) throws IOException {
        super(diskSchema, id, name, querySQL, null, columnTemplates, sysSession, recursive);
        this.file = file;

        if (file.exists()) {
            String msg = MessageFormat.format("Can not create table {0} file already exists", name);
            throw DbException.get(ErrorCode.IO_EXCEPTION_1, msg);
        }

        try (PrintStream writer = new PrintStream(new FileOutputStream(file))) {
            writer.println(columns.length);
            for (Column c : columns) {
                String line = MessageFormat.format("{0},{1,number,#},{2,number,#},{3,number,#},{4,number,#}",
                        c.getName(), c.getType(), c.getPrecision(), c.getScale(), c.getDisplaySize());
                writer.println(line);
            }
            writer.println(querySQL);
        }
    }

// ------------------------ INTERFACE METHODS ------------------------


// --------------------- Interface DbObject ---------------------

    @Override
    public void removeChildrenAndResources(Session session) {
        // after super call the name will be reset to null, so to preserve it
        // save in the temporary variable
        String name = getName();
        super.removeChildrenAndResources(session);
        if (file.exists()) {
            boolean success = file.delete();
            if (!success)
                throw DbException.get(ErrorCode.IO_EXCEPTION_1,
                        "can not delete file:" + file.getAbsolutePath());
        }
    }
}
