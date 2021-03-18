/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */

package org.h2.contrib.link;

import java.sql.Connection;

public interface TableLinkColumnHandlerFactory {

    TableLinkColumnHandler create(Connection connection, String schema, String table);
}
