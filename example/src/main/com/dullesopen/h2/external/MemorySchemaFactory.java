package com.dullesopen.h2.external;

import org.h2.engine.Database;
import org.h2.engine.User;
import org.h2.schema.Schema;
import org.h2.schema.SchemaFactory;

/**
 * @author Pavel Ganelin
 */
public class MemorySchemaFactory implements SchemaFactory  {
// ------------------------ INTERFACE METHODS ------------------------


// --------------------- Interface SchemaFactory ---------------------

    public Schema create(Database database, int id, String schemaName, User user, String filename) {
        return new MemorySchema(database, id, schemaName, user,filename);
    }
}
