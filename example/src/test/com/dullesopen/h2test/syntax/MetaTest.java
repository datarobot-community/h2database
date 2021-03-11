package com.dullesopen.h2test.syntax;


import com.dullesopen.h2test.TestConfig;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.SQLException;

public class MetaTest {
// ------------------------------ FIELDS ------------------------------

    private static final boolean DB2 = false;

// -------------------------- TEST METHODS --------------------------

    @BeforeClass
    public void setUp() throws Exception {
        Class.forName("org.h2.Driver");
    }

// -------------------------- OTHER METHODS --------------------------

    @Test(groups = {"db2"})
    public void db2() throws Exception {
        if (DB2) {
            String driver = "com.ibm.db2.jcc.DB2Driver";
            Class.forName(driver);
            String url = "jdbc:db2:sample";
            String user = "db2admin";
            String password = "pass4db2";

            Connection linked = DriverManager.getConnection(url, user, password);
            info("DB2", linked);
        }
    }

    private void info(String type, Connection linked) throws SQLException {
        DatabaseMetaData meta = linked.getMetaData();
        if (false) {
            System.out.println("==============================================");
            System.out.println("type: " + type);

            System.out.println("supportsMixedCaseIdentifiers= " + meta.supportsMixedCaseIdentifiers());
            System.out.println("storesUpperCaseIdentifiers= " + meta.storesUpperCaseIdentifiers());
            System.out.println("storesLowerCaseIdentifiers= " + meta.storesLowerCaseIdentifiers());
            System.out.println("storesMixedCaseIdentifiers= " + meta.storesMixedCaseIdentifiers());
            System.out.println("supportsMixedCaseQuotedIdentifiers= " + meta.supportsMixedCaseQuotedIdentifiers());
            System.out.println("storesUpperCaseQuotedIdentifiers= " + meta.storesUpperCaseQuotedIdentifiers());
            System.out.println("storesLowerCaseQuotedIdentifiers= " + meta.storesLowerCaseQuotedIdentifiers());
            System.out.println("storesMixedCaseQuotedIdentifiers= " + meta.storesMixedCaseQuotedIdentifiers());
        }
    }

    @Test
    public void h2() throws SQLException {
        String user = "sa";
        String password = "pass";
        String url = "jdbc:h2:mem:another";
        Connection linked = DriverManager.getConnection(url, user, password);
        info("H2", linked);
    }

    /* Temporary removed @Test*/
    public void mssql() throws Exception {
        String driver = "net.sourceforge.jtds.jdbc.Driver";
        Class.forName(driver);

        String user = "myuser";
        String password = "mypassword";
        String url = "jdbc:jtds:sqlserver://localhost";

        Connection linked = DriverManager.getConnection(url, user, password);
        info("MSSQL", linked);
    }

    @Test
    public void oracle() throws Exception {
        if (TestConfig.ORACLE) {

            Connection linked = TestConfig.getOracleConnection();
            info("Oracle", linked);
        }
    }

    @Test(groups = {"teradata"})
    public void teradata() throws Exception {
        if (TestConfig.TERADATA) {
            Connection linked = TestConfig.getTeradataConnection();
            info("Teradata", linked);
        }
    }
}