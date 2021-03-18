package org.h2.ri.external.memory;

import org.h2.engine.Database;
import org.h2.engine.User;
import org.h2.schema.Schema;
import org.h2.schema.SchemaFactory;

public class MemorySchemaFactory implements SchemaFactory {
// ------------------------ INTERFACE METHODS ------------------------


// --------------------- Interface SchemaFactory ---------------------

    public Schema create(Database database, int id, String schemaName, User user, String filename) {
        return new MemorySchema(database, id, schemaName, user, filename);
    }
}
