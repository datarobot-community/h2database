package com.dullesopen.h2test.features;

import com.dullesopen.h2test.TestConfig;
import com.dullesopen.h2test.Utils;
import org.h2.index.IndexType;
import org.h2.jdbc.JdbcConnection;
import org.testng.Assert;
import org.testng.annotations.*;

import java.sql.*;
import java.text.MessageFormat;

import static org.testng.Assert.assertEquals;

/**
 * Miscellaneous test for linked table
 */
@SuppressWarnings({"SqlNoDataSourceInspection", "SqlDialectInspection"})
public class TableLinkTest {

    //TODO: What to do with default schema in Teradata?
    private static final boolean BROKEN_WITHOUT_EXPLICIT_SCHEMA = false;
    // ------------------------------ FIELDS ------------------------------

    private Connection h2;
    private Connection oracle;
    private Connection teradata;

// -------------------------- TEST METHODS --------------------------

    @BeforeMethod
    protected void setUp() throws Exception {
        Class.forName("org.h2.Driver");
        h2 = DriverManager.getConnection("jdbc:h2:mem:");
    }

    @AfterMethod
    protected void tearDown() throws Exception {
        h2.close();
    }

    @BeforeClass
    protected void beforeClass() throws Exception {
        oracle = TestConfig.getOracleConnection();
        if (oracle != null) {
            oracle.setAutoCommit(false);
        }
        teradata = TestConfig.getTeradataConnection();
    }

    @AfterClass
    protected void afterClass() throws Exception {
        if (oracle != null) {
            oracle.close();
        }
        if (teradata != null) {
            teradata.close();
        }
    }

// -------------------------- STATIC METHODS --------------------------

    @Test(enabled = false)
    public static void mixedCaseCreateTable() throws Exception {
        if (TestConfig.MSSQL) {
            Class.forName("net.sourceforge.jtds.jdbc.Driver");
            Class.forName("org.h2.Driver");

            Connection linked = DriverManager.getConnection("jdbc:jtds:sqlserver://localhost:1433", "myuser", "mypassword");

            Statement mssql = linked.createStatement();
            Utils.drop(mssql, "DROP TABLE testtable");
            mssql.execute("CREATE TABLE testtable(lower INT, UPPER INT, MiXeD INT)");

            Connection h2 = DriverManager.getConnection("jdbc:h2:mem:ms");

            Statement sb = h2.createStatement();
            {
                sb.execute("CREATE LINKED TABLE two('net.sourceforge.jtds.jdbc.Driver','jdbc:jtds:sqlserver://localhost:1433','myuser','mypassword', 'testtable')");
                ResultSet rs = sb.executeQuery("SELECT * from  two");
                ResultSetMetaData md = rs.getMetaData();
                Assert.assertEquals(md.getColumnName(1), "LOWER");
                Assert.assertEquals(md.getColumnName(2), "UPPER");
                Assert.assertEquals(md.getColumnName(3), "MIXED");

                rs = sb.executeQuery("SELECT lower,upper,mixed from  two");

                rs = sb.executeQuery("SELECT LOWER,UPPER,MIXED from  two");
            }
            {
                sb.execute("CREATE LINKED TABLE three('net.sourceforge.jtds.jdbc.Driver','jdbc:jtds:sqlserver://localhost:1433','myuser','mypassword', '', 'testtable')");
                ResultSet rs = sb.executeQuery("SELECT * from  three");
                ResultSetMetaData md = rs.getMetaData();
                Assert.assertEquals(md.getColumnName(1), "LOWER");
                Assert.assertEquals(md.getColumnName(2), "UPPER");
                Assert.assertEquals(md.getColumnName(3), "MIXED");

                rs = sb.executeQuery("SELECT lower,upper,mixed from  three");

                rs = sb.executeQuery("SELECT LOWER,UPPER,MIXED from  three");
            }
            linked.close();
            h2.close();
        }
    }

// -------------------------- OTHER METHODS --------------------------

    /**
     * confirm that H2 automatically converts datetime to date
     *
     * @throws Exception
     */
    @Test
    public void datetime2date() throws Exception {
        if (TestConfig.ORACLE) {
            Statement ora = oracle.createStatement();
            Utils.drop(ora, "DROP TABLE TWO");
            ora.execute("CREATE TABLE TWO (DT DATE)");
            oracle.commit();

            Statement sa = h2.createStatement();
            Utils.drop(sa, "DROP TABLE ONE");
            sa.execute("CREATE TABLE ONE (DT DATE)");
            sa.execute("INSERT INTO ONE VALUES (TIMESTAMP'2008-01-08 10:11:15')");

            sa.execute(MessageFormat.format("CREATE LINKED TABLE TWOL(''oracle.jdbc.driver.OracleDriver'', ''{0}'', ''h2user'', ''h2pass'', ''TWO'');",
                    TestConfig.ORACLE_URL));

            sa.execute("INSERT INTO TWOL SELECT DT FROM ONE");
            ResultSet rs = sa.executeQuery("select * FROM TWOL");
            rs.next();
            assertEquals(rs.getDate(1).toString(), "2008-01-08");
            Utils.drop(ora, "DROP TABLE TWO");
            Utils.drop(sa, "DROP TABLE ONE");
        }
    }

    /**
     * Oracle specific types BINARY_DOUBLE and BINARY_FLOAT
     *
     * @throws Exception
     */
    @Test
    public void binaryFloat() throws Exception {
        if (TestConfig.ORACLE) {
            Statement ora = oracle.createStatement();
            Utils.drop(ora, "DROP TABLE ONE");
            ora.execute("CREATE TABLE ONE (D BINARY_DOUBLE, F BINARY_FLOAT)");
            ora.execute("INSERT INTO ONE VALUES(12345.56789, 8765.4321)");
            oracle.commit();

            Statement sa = h2.createStatement();
            Utils.drop(sa, "DROP TABLE TWO");

            sa.execute(MessageFormat.format("CREATE LINKED TABLE TWO(''oracle.jdbc.driver.OracleDriver'', ''{0}'', ''h2user'', ''h2pass'', ''H2USER'', ''ONE'');",
                    TestConfig.ORACLE_URL));

            ResultSet rs = sa.executeQuery("select * FROM TWO");
            rs.next();
            assertEquals(rs.getDouble(1), 12345.56789);
            // notice expected loss of precision for BINARY_FLOAT
            assertEquals(rs.getDouble(2), 8765.431640625);
            Utils.drop(ora, "DROP TABLE ONE");
            Utils.drop(sa, "DROP TABLE TWO");
        }
    }

    /**
     * Oracle table as reserved keyword
     *
     * @throws Exception
     */
    @Test
    public void reserved() throws Exception {
        if (TestConfig.ORACLE) {

            Statement ora = oracle.createStatement();
            Utils.drop(ora, "DROP TABLE \"TABLE\"");
            ora.execute("CREATE TABLE \"TABLE\" (\"VIEW\" INTEGER )");
            ora.execute("INSERT INTO \"TABLE\" VALUES(12345)");
            oracle.commit();

            Statement sa = h2.createStatement();
            Utils.drop(sa, "DROP TABLE TWO");

            sa.execute(MessageFormat.format("CREATE LINKED TABLE TWO(''oracle.jdbc.driver.OracleDriver'', ''{0}'', ''h2user'', ''h2pass'', ''H2USER'', ''TABLE'');",
                    TestConfig.ORACLE_URL));

            ResultSet rs = sa.executeQuery("select * FROM TWO");
            rs.next();
            assertEquals(rs.getDouble(1), 12345.0);
            Utils.drop(ora, "DROP TABLE ONE");
            Utils.drop(sa, "DROP TABLE TWO");
        }
    }

    /**
     * @throws Exception
     */
    @Test
    public void metaInfoUdtError() throws Exception {
        if (TestConfig.TERADATA) {
            Statement tera = teradata.createStatement();
            Utils.drop(tera, "DROP TABLE ONE");
            tera.execute("CREATE TABLE ONE (i integer)");
            tera.execute("INSERT INTO ONE VALUES(1234)");
            Utils.drop(tera, "DROP TABLE H2SCHEMA.TWO");
            tera.execute("CREATE TABLE H2SCHEMA.TWO (j integer)");
            tera.execute("INSERT INTO H2SCHEMA.TWO VALUES(5678)");
            teradata.commit();

            Statement sa = h2.createStatement();

            if (BROKEN_WITHOUT_EXPLICIT_SCHEMA)
                sa.execute(MessageFormat.format("CREATE LINKED TABLE THREE(''{0}'', ''{1}'', ''h2user'', ''h2pass'', ''ONE'');",
                        TestConfig.TERADATA_DRIVER, TestConfig.TERADATA_URL));

            sa.execute(MessageFormat.format("CREATE LINKED TABLE FOUR(''{0}'', ''{1}'', ''h2user'', ''h2pass'', ''H2SCHEMA'', ''TWO'');",
                    TestConfig.TERADATA_DRIVER, TestConfig.TERADATA_URL));

            if (BROKEN_WITHOUT_EXPLICIT_SCHEMA) {
                ResultSet rs = sa.executeQuery("select * FROM THREE");
                Assert.assertEquals(rs.getMetaData().getColumnName(1), "I");
                rs.next();
                assertEquals(rs.getInt(1), 1234);
                rs.close();
            }
            {
                ResultSet rs = sa.executeQuery("select * FROM FOUR");
                Assert.assertEquals(rs.getMetaData().getColumnName(1), "J");
                rs.next();
                assertEquals(rs.getInt(1), 5678);
                rs.close();
            }
            Utils.drop(tera, "DROP TABLE ONE");
            Utils.drop(tera, "DROP TABLE H2SCHEMA.TWO");
            if (BROKEN_WITHOUT_EXPLICIT_SCHEMA)
                Utils.drop(sa, "DROP TABLE THREE");
            Utils.drop(sa, "DROP TABLE FOUR");
        }
    }

    /**
     * the test reproduce the bug the JDTS driver does not return information about indexes
     *
     * @throws Exception
     */
    @Test
    public void indexes() throws Exception {
        if (TestConfig.MSSQL) {
            Class.forName("net.sourceforge.jtds.jdbc.Driver");

            Connection linked = DriverManager.getConnection("jdbc:jtds:sqlserver://localhost:1433", "myuser", "mypassword");

            Statement fstat = linked.createStatement();
            Utils.drop(fstat, "DROP TABLE testtable");
            fstat.execute("CREATE TABLE testtable(lower INT, UPPER INT, MiXeD INT)");
            ResultSet rs = linked.getMetaData().getIndexInfo(null, null, "testtable", false, false);
            IndexType indexType = null;

            while (rs.next()) {
                short type = rs.getShort("TYPE");
                String newIndex = rs.getString("INDEX_NAME");

                boolean unique = !rs.getBoolean("NON_UNIQUE");
                String col = rs.getString("COLUMN_NAME");
            }


            linked.close();
        }
    }

    @Test(enabled = false)
    public void linkedPerformanceForeign() throws Exception {
        int N = 1000;

        String driver = "com.ibm.db2.jcc.DB2Driver";
        String url = "jdbc:db2:sample";
        String user = "db2admin";
        String password = "pass4db2";
        size(N, driver, url, user, password);
        size(2 * N, driver, url, user, password);
        size(4 * N, driver, url, user, password);
        size(8 * N, driver, url, user, password);
        size(16 * N, driver, url, user, password);
    }

    private void size(int size, String driver, String url, String user, String password) throws ClassNotFoundException, SQLException {
        Class.forName(driver);
        Class.forName("org.h2.Driver");
        {
            Connection ca = DriverManager.getConnection(url, user, password);

            Statement sa = ca.createStatement();

            Utils.drop(sa, "DROP TABLE LL");
            Utils.drop(sa, "DROP TABLE RR");
            Utils.drop(sa, "DROP TABLE J");
            sa.execute("CREATE TABLE LL (C INT NOT NULL PRIMARY KEY)");
            sa.execute("CREATE TABLE RR (C INT NOT NULL PRIMARY KEY)");
            //sa.execute("CREATE UNIQUE INDEX LI ON LL(C);");
            //sa.execute("CREATE UNIQUE INDEX RI ON RR(C);");

            PreparedStatement left = ca.prepareStatement("INSERT INTO LL VALUES(?)");
            PreparedStatement right = ca.prepareStatement("INSERT INTO RR VALUES(?)");
            for (int i = 0; i < size; i++) {
                left.setInt(1, i);
                right.setInt(1, i);
                left.executeUpdate();
                right.executeUpdate();
            }
            left.close();
            right.close();

            long t1 = System.nanoTime();
            sa.execute("CREATE TABLE J AS (SELECT LL.C  FROM LL , RR WHERE LL.C=RR.C) DATA INITIALLY DEFERRED REFRESH IMMEDIATE");
            //sa.execute("CREATE TABLE J AS SELECT C FROM LL");
            long t2 = System.nanoTime();
            if (false) System.out.println("internal = " + size + " : " + ((t2 - t1) / 1000000 / 1000.));
            sa.close();
            ca.close();
        }
        Connection cb = DriverManager.getConnection("jdbc:h2:mem:two");
        Statement sb = cb.createStatement();


        String l = MessageFormat.format("CREATE LINKED TABLE FL(''{0}'', ''{1}'', ''{2}'', ''{3}'', ''LL'');", driver, url, user, password);
        String r = MessageFormat.format("CREATE LINKED TABLE FR(''{0}'', ''{1}'', ''{2}'', ''{3}'', ''RR'');", driver, url, user, password);
        sb.execute(l);
        sb.execute(r);

        long t3 = System.nanoTime();
        sb.execute("CREATE TABLE J AS SELECT FL.C FROM FL LEFT OUTER JOIN FR ON FL.C=FR.C");
        long t4 = System.nanoTime();

        if (false) System.out.println("linked   = " + size + " : " + ((t4 - t3) / 1000000 / 1000.));
        sb.close();
        cb.close();
    }

    @Test(enabled = false)
    public void linkedPerformanceNative() throws Exception {
        int N = 10000;
        String driver = "org.h2.Driver";
        String s = "jdbc:h2:mem:one";
        String user = "linkuser";
        String password = "linkpass";
        size(N, driver, s, user, password);
        size(2 * N, driver, s, user, password);
        size(2 * N, driver, s, user, password);
        size(2 * N, driver, s, user, password);
    }

    /**
     * verify that h2 can extract proper case column names in case of mixed case database
     *
     * @throws Exception
     */
    @Test
    public void mixedCase() throws Exception {
        if (TestConfig.MSSQL) {
            Class.forName("net.sourceforge.jtds.jdbc.Driver");
            Class.forName("org.h2.Driver");

            Connection linked = DriverManager.getConnection("jdbc:jtds:sqlserver://localhost:1433", "myuser", "mypassword");

            Statement fstat = linked.createStatement();
            Utils.drop(fstat, "DROP TABLE testtable");
            fstat.execute("CREATE TABLE testtable(lower INT, UPPER INT, MiXeD INT)");

            Connection h2 = DriverManager.getConnection("jdbc:h2:mem:ms");

            Statement sb = h2.createStatement();
            sb.execute("CREATE SCHEMA mssql LINKED ('net.sourceforge.jtds.jdbc.Driver','jdbc:jtds:sqlserver://localhost:1433','myuser','mypassword','')");
            ResultSet rs = sb.executeQuery("SELECT * from  mssql.testtable ");
            ResultSetMetaData md = rs.getMetaData();

            Assert.assertEquals(md.getColumnName(1), "LOWER");
            Assert.assertEquals(md.getColumnName(2), "UPPER");
            Assert.assertEquals(md.getColumnName(3), "MIXED");

            rs = sb.executeQuery("SELECT lower,upper,mixed from  mssql.testtable ");

            rs = sb.executeQuery("SELECT LOWER,UPPER,MIXED from  mssql.testtable ");

            linked.close();
            h2.close();
        }
    }

    @Test
    public void testCallLinkSchemaNonPublic() throws Exception {
        Class.forName("org.h2.Driver");

        Connection ca = DriverManager.getConnection("jdbc:h2:mem:one", "linkuser", "linkpass");

        Connection cb = DriverManager.getConnection("jdbc:h2:mem:two");

        Statement sa = ca.createStatement();

        Statement sb = cb.createStatement();

        sa.execute("CREATE TABLE GOODFOO (X NUMBER)");

        sa.execute("CREATE SCHEMA S");
        sa.execute("CREATE TABLE S.BADFOO (X NUMBER)");


        sb.execute("CALL LINK_SCHEMA('GOOD', '', 'jdbc:h2:mem:one', 'linkuser', 'linkpass', 'PUBLIC'); ");
        sb.execute("CALL LINK_SCHEMA('BAD', '', 'jdbc:h2:mem:one', 'linkuser', 'linkpass', 'S'); ");

        sb.executeQuery("SELECT * FROM GOOD.GOODFOO");
        sb.executeQuery("SELECT * FROM BAD.BADFOO");

        ca.close();
        cb.close();
    }

    @Test
    public void testCallStaleLink() throws Exception {
        Class.forName("org.h2.Driver");

        Connection ca = DriverManager.getConnection("jdbc:h2:mem:sl1", "linkuser", "linkpass");
        Connection cb = DriverManager.getConnection("jdbc:h2:mem:sl2");
        Statement sa = ca.createStatement();
        Statement sb = cb.createStatement();
        sa.execute("CREATE TABLE ONE (X NUMBER)");
        sb.execute("CALL LINK_SCHEMA('GOOD', '', 'jdbc:h2:mem:sl1', 'linkuser', 'linkpass', 'PUBLIC'); ");
        sb.executeQuery("SELECT * FROM GOOD.ONE");
        sa.execute("CREATE TABLE TWO (X NUMBER)");
        try {
            sb.executeQuery("SELECT * FROM GOOD.TWO");  //FAILED
            Assert.fail("this is expected to fail because LINK_SHEMA does not refresh schemas");
        } catch (SQLException e) {
        }

        ca.close();
        cb.close();
    }

    @Test
    public void testDuplicateTable() throws Exception {
        Connection ca = DriverManager.getConnection("jdbc:h2:mem:one", "linkuser", "linkpass");

        Statement sa = ca.createStatement();
        sa.execute("CREATE SCHEMA Y");

        sa.execute("CREATE TABLE A( C INT)");
        sa.execute("CREATE TABLE Y.A (C INT)");

        Connection cb = DriverManager.getConnection("jdbc:h2:mem:two");

        Statement sb = cb.createStatement();

        sb.execute("CREATE LINKED TABLE one('org.h2.Driver', 'jdbc:h2:mem:one', 'linkuser', 'linkpass', 'Y.A');"); //OK
        try {
            sb.execute("CREATE LINKED TABLE two('org.h2.Driver', 'jdbc:h2:mem:one', 'linkuser', 'linkpass', 'A');"); //FAILS
            Assert.fail("still an error in the last version of H2");
        } catch (SQLException e) {
        }
        ca.close();
        cb.close();
    }

    /**
     * Originally from H2 test suite
     * TODO linked tables does not actually close the connection, because connection is part of schema now,
     * it is not done on demand
     */

    @Test
    public void testLinkDrop() throws Exception {
        Class.forName("org.h2.Driver");
        Connection connA = DriverManager.getConnection("jdbc:h2:mem:a");
        Statement statA = connA.createStatement();
        statA.execute("CREATE TABLE TEST(ID INT)");
        Connection connB = DriverManager.getConnection("jdbc:h2:mem:b");
        Statement statB = connB.createStatement();
        statB.execute("CREATE LINKED TABLE TEST_LINK('', 'jdbc:h2:mem:a', '', '', 'TEST')");
        connA.close();
        // the connection should be closed now
        // (and the table should disappear because the last connection was
        // closed)
        statB.execute("DROP TABLE TEST_LINK");
        connA = DriverManager.getConnection("jdbc:h2:mem:a");
        statA = connA.createStatement();
        // table should not exist now
        statA.execute("CREATE TABLE TEST(ID INT)");
        connA.close();
        connB.close();
    }

    @Test
    public void linkedView() throws Exception {
        Class.forName("org.h2.Driver");
        Connection connA = DriverManager.getConnection("jdbc:h2:mem:a");
        Statement statA = connA.createStatement();
        statA.execute("CREATE TABLE ONE(ID INT)");
        statA.execute("CREATE VIEW TWO AS SELECT * FROM ONE");
        Connection connB = DriverManager.getConnection("jdbc:h2:mem:b");
        Statement statB = connB.createStatement();
        statB.execute("CREATE LINKED TABLE THREE('', 'jdbc:h2:mem:a', '', '', 'TWO')");
        connA.close();
        ResultSet rs = statB.executeQuery("SELECT id from THREE");
        connA.close();
        connB.close();
    }

    @Test
    public void meta() throws Exception {
        Class.forName("org.h2.Driver");
        try (Connection conn = DriverManager.getConnection("jdbc:h2:mem:a")) {
            try (Statement stat = conn.createStatement()) {
                stat.execute("CREATE TABLE ONE(ID INT)");
                stat.execute("CREATE VIEW TWO AS SELECT * FROM ONE");
            }
            DatabaseMetaData metaData = conn.getMetaData();
            try (ResultSet rs = metaData.getColumns(null, null, "TWO", null)) {
                rs.next();
                System.out.println("rs = " + rs.getString("COLUMN_NAME"));
            }
        }
    }
}