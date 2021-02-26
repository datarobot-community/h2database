/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.jdbc;

import org.h2.engine.Constants;

import java.sql.SQLWarning;

/**
 * Represents a database warning.
 */
public class JdbcSQLWarning extends SQLWarning {

    private static final long serialVersionUID = 1L;
    private final String originalMessage;
    private String message;
    private String sql;

    /**
     * Creates a SQLException.
     *
     * @param message   the reason
     * @param sql       the SQL statement
     * @param state     the SQL state
     * @param errorCode the error code
     */
    public JdbcSQLWarning(String message, String sql, String state, int errorCode) {
        super(message, state, errorCode);
        this.originalMessage = message;
        setSQL(sql);
        buildMessage();
    }

    /**
     * Get the detail error message.
     *
     * @return the message
     */
    public String getMessage() {
        return message;
    }

    public String getSQL() {
        return sql;
    }

    /**
     * INTERNAL
     */
    public void setSQL(String sql) {
        this.sql = sql;
    }

    private void buildMessage() {
        StringBuilder buff = new StringBuilder(originalMessage == null ? "- " : originalMessage);
        if (sql != null) {
            buff.append("; SQL statement:\n").append(sql);
        }
        buff.append(" [").append(getErrorCode()).append('-').append(Constants.BUILD_ID).append(']');
        message = buff.toString();
    }

}
