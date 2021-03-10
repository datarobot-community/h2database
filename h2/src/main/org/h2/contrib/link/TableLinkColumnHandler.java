package org.h2.contrib.link;

import org.h2.engine.Session;
import org.h2.table.Column;
import org.h2.value.Value;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public interface TableLinkColumnHandler {

    /**
     * create H2 column from the external database column
     */
    Column createColumn(String name,
                        int sqlType,
                        int type,
                        String typename,
                        long precision,
                        int scale,
                        int displaySize) throws java.sql.SQLException;

    /**
     * Create H2 Value taking into account extension mapping
     */
    Value createValue(Session session, ResultSet rs, int columnIndex, int type);

    /**
     * Bind parameter for PreparedStatement from H2 to RDMBS
     */
    void bindParameterValue(PreparedStatement prep, Value v, int param, int sqlType, String sqlTypeName) throws SQLException;
}
