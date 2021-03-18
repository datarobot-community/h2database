package org.h2.contrib.external.demo;

import org.h2.command.ddl.CreateTableData;
import org.h2.engine.Database;
import org.h2.engine.User;
import org.h2.schema.Schema;
import org.h2.table.Table;

public class DemoSchema extends Schema {
    private final DemoContext context;
    // --------------------------- CONSTRUCTORS ---------------------------

    public DemoSchema(Database database, int id, String schemaName, User owner) {
        super(database, id, schemaName, owner, false);
        context = (DemoContext) database.clientContext;
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
        throw new UnsupportedOperationException("Simple failed for: " + context.label);
    }
}
