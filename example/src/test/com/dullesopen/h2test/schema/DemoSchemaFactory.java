package com.dullesopen.h2test.schema;

import org.h2.engine.Database;
import org.h2.engine.User;
import org.h2.schema.Schema;
import org.h2.schema.SchemaFactory;

/**
 * @author Pavel Ganelin
 */
public class DemoSchemaFactory implements SchemaFactory {
// ------------------------ INTERFACE METHODS ------------------------


// --------------------- Interface SchemaFactory ---------------------

    public Schema create(Database database, int id, String schemaName, User owner, String parameters) {
        return new com.dullesopen.h2test.schema.DemoSchema(database, id, schemaName, owner);
    }
}
