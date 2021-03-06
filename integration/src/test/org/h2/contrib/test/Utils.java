package org.h2.contrib.test;

import java.sql.SQLException;
import java.sql.Statement;

public class Utils {
    public static void drop(Statement sa, String sql) {
        try {
            sa.execute(sql);
        } catch (SQLException e) {
        }
    }

    public static String truncate(SQLException e) {
        String message = e.getMessage();
        return message.substring(0, message.indexOf("; SQL statement:\n"));
    }

    public static String teradata(SQLException e) {
        String message = e.getMessage();
        return message.substring(message.indexOf("[SQLState"));
    }
}
