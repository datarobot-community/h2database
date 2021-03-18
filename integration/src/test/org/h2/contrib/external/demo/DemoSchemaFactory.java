package org.h2.contrib.external.demo;

import org.h2.engine.Database;
import org.h2.engine.User;
import org.h2.schema.Schema;
import org.h2.schema.SchemaFactory;

public class DemoSchemaFactory implements SchemaFactory {

    public Schema create(Database database, int id, String schemaName, User owner, String parameters) {
        return new DemoSchema(database, id, schemaName, owner);
    }
}
