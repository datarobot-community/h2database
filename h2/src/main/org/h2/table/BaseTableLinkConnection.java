package org.h2.table;

import java.sql.Connection;

public interface BaseTableLinkConnection {

    void close(boolean force);

    Connection getConnection();
}
