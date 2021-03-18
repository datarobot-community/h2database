/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: PG
 */
package org.h2.schema;

import org.h2.engine.*;

public interface SchemaFactory  {
    Schema create(Database database, int id, String schemaName, User owner, String parameters) ;
}
