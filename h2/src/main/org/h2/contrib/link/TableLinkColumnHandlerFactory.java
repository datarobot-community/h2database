package org.h2.contrib.link;

import java.sql.Connection;

public interface TableLinkColumnHandlerFactory {

    TableLinkColumnHandler create(Connection connection, String schema, String table);

}
