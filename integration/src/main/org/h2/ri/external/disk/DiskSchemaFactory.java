package org.h2.ri.external.disk;

import org.h2.engine.Database;
import org.h2.engine.User;
import org.h2.schema.Schema;
import org.h2.schema.SchemaFactory;

public class DiskSchemaFactory implements SchemaFactory {

    public Schema create(Database database, int id, String schemaName, User user, String options) {
        return new DiskSchema(database, id, schemaName, user, options);
    }
}
