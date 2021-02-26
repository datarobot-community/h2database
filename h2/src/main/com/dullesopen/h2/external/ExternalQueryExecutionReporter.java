package com.dullesopen.h2.external;

import java.sql.Connection;

/**
 * Add notification for external RDBM query execution
 */

public interface ExternalQueryExecutionReporter {

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
