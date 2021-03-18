/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */

package org.h2.contrib.link;

import java.sql.Connection;

/**
 * Add notification for linked query execution
 */

public interface LinkedQueryExecutionReporter {

    /**
     * @param action true is only prepare statement is called without actual execution
     * @param schema  name of the H2 schema
     * @param sql   SQL statement to be executed
     * @param connection
     */
    void report(Action action, String schema, String sql, Connection connection);

    enum Action {
        PREPARE,
        EXECUTE,
        REUSE,
        EXECUTE_QUERY,
    }
}
