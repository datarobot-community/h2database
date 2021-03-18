package org.h2.ri.external.memory;

import org.h2.command.ddl.CreateTableData;
import org.h2.engine.Database;
import org.h2.engine.User;
import org.h2.schema.Schema;
import org.h2.table.Table;

public class MemorySchema extends Schema {
// ------------------------------ FIELDS ------------------------------

    private final String filename;

// --------------------------- CONSTRUCTORS ---------------------------

    public MemorySchema(Database database, int id, String schemaName, User owner, String filename) {
        super(database, id, schemaName, owner, false);
        this.filename = filename;
    }

// ------------------------ INTERFACE METHODS ------------------------


// --------------------- Interface DbObject ---------------------

    public String getCreateSQL() {
        StringBuffer buff = new StringBuffer();
        buff.append("CREATE SCHEMA ");
        buff.append(getSQL());
        buff.append(" AUTHORIZATION ");
        buff.append(getOwner().getSQL());
        buff.append(" EXTERNAL ");
        buff.append("(\"");
        buff.append(getClass().getName());
        buff.append("\")");
        buff.append(getOwner().getSQL());
        return buff.toString();
    }

// -------------------------- OTHER METHODS --------------------------

    @Override
    public Table createTable(CreateTableData data) {
        data.schema = this;
        return new MemoryTable(data, filename);
    }
}
