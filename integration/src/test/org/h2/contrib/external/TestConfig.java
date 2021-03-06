package org.h2.contrib.external;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class TestConfig {
// ------------------------------ FIELDS ------------------------------

    public static final boolean MSSQL = false;
    public static final boolean TERADATA = true;
    public static final boolean ORACLE = true;
    public static final String ORACLE_URL = "jdbc:oracle:thin:@//oracle-db:1521/ORCLPDB1.localdomain";
    public static final String TERADATA_URL = "jdbc:teradata://teradata-db/DBS_PORT=1025";
    public static final String TERADATA_DRIVER = "com.teradata.jdbc.TeraDriver";

    public static Connection getTeradataConnection() throws SQLException, ClassNotFoundException {
        if (TestConfig.TERADATA) {
            Class.forName(TestConfig.TERADATA_DRIVER);
            return DriverManager.getConnection(TestConfig.TERADATA_URL, "h2user", "h2pass");
        } else {
            return null;
        }
    }

    public static Connection getTeradataConnectionFastLoad() throws SQLException, ClassNotFoundException {
        if (TestConfig.TERADATA) {
            Class.forName(TestConfig.TERADATA_DRIVER);
            return DriverManager.getConnection(TestConfig.TERADATA_URL + ",TYPE=FASTLOAD", "h2user", "h2pass");
        } else {
            return null;
        }
    }

    public static Connection getOracleConnection() throws SQLException, ClassNotFoundException {
        if (TestConfig.ORACLE) {
            Class.forName("oracle.jdbc.OracleDriver");
            return DriverManager.getConnection(TestConfig.ORACLE_URL, "h2user", "h2pass");
        } else {
            return null;
        }
    }
}
