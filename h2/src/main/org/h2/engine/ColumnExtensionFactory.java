package org.h2.engine;

import java.sql.Connection;

public interface ColumnExtensionFactory {

    ColumnExtension create(Connection connection);

}
