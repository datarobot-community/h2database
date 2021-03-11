package com.dullesopen.h2test.features;

import com.dullesopen.h2test.TestConfig;
import com.dullesopen.h2test.Utils;
import org.h2.engine.Constants;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.sql.*;

import static org.testng.Assert.*;

/**
 * Test precision and displaySize
 */
public class SizeTest {
// ------------------------------ FIELDS ------------------------------

    private static final boolean DB2 = false;
    private Connection ca;

// -------------------------- TEST METHODS --------------------------

    @BeforeMethod
    protected void setUp() throws Exception {
        Class.forName("org.h2.Driver");
        ca = DriverManager.getConnection("jdbc:h2:mem:");
    }

    @AfterMethod
    protected void tearDown() throws Exception {
        ca.close();
    }

// -------------------------- OTHER METHODS --------------------------

    @Test
    public void concat() throws Exception {
        Statement sa = ca.createStatement();
        sa.execute("CREATE TABLE one (C CHARACTER(10))");

        ResultSet rs = sa.executeQuery("SELECT C || C FROM one;");
        ResultSetMetaData md = rs.getMetaData();
        assertEquals(md.getPrecision(1), 20);
    }

    @Test
    public void count() throws Exception {
        Statement sa = ca.createStatement();
        sa.execute("CREATE TABLE one (C CHARACTER(12), Y INTEGER)");

        ResultSet rs = sa.executeQuery("SELECT COUNT(C) AS X , Y FROM one GROUP BY Y;");
        ResultSetMetaData md = rs.getMetaData();
        //noinspection ConstantConditions
        assertEquals(md.getPrecision(1), Constants.BUILD_ID > 200 ? 64 : 19);
        //noinspection ConstantConditions
        assertEquals(md.getPrecision(2), Constants.BUILD_ID > 200 ? 32 : 10);
    }

    @Test
    public void createAs() throws Exception {
        Class.forName("org.h2.Driver");

        Connection ca = DriverManager.getConnection("jdbc:h2:mem:", "", "");

        Statement sa = ca.createStatement();
        sa.execute("CREATE TABLE ONE (X NUMBER(12,2), Y FLOAT)");
        sa.execute("CREATE TABLE TWO as SELECT * FROM ONE");
        {
            ResultSet rs = sa.executeQuery("SELECT * FROM ONE;");
            ResultSetMetaData md = rs.getMetaData();
            assertEquals(md.getPrecision(1), 12);
            //noinspection ConstantConditions
            assertEquals(md.getPrecision(2), Constants.BUILD_ID > 200 ? 53 : 17);
            //noinspection ConstantConditions
            assertEquals(md.getColumnType(1), Constants.BUILD_ID > 200 ? Types.NUMERIC : Types.DECIMAL);
            //noinspection ConstantConditions
            assertEquals(md.getColumnType(2), Constants.BUILD_ID > 200 ? Types.FLOAT : Types.DOUBLE);
        }
        {
            ResultSet rs = sa.executeQuery("SELECT * FROM TWO;");
            ResultSetMetaData md = rs.getMetaData();
            assertEquals(md.getPrecision(1), 12); //WRONG SIZE
            //noinspection ConstantConditions
            assertEquals(md.getPrecision(2), Constants.BUILD_ID > 200 ? 53 : 17);
            //noinspection ConstantConditions
            assertEquals(md.getColumnType(1), Constants.BUILD_ID > 200 ? Types.NUMERIC : Types.DECIMAL);
            //noinspection ConstantConditions
            assertEquals(md.getColumnType(2), Constants.BUILD_ID > 200 ? Types.FLOAT : Types.DOUBLE);
        }
        ca.close();
    }

    @Test
    public void createAsOracle() throws Exception {
        if (TestConfig.ORACLE) {

            Connection ora = TestConfig.getOracleConnection();

            Statement sa = ora.createStatement();

            Utils.drop(sa, "DROP TABLE ONE ");
            Utils.drop(sa, "DROP TABLE TWO ");

            sa.execute("CREATE TABLE ONE (X NUMBER(12,2), Y FLOAT)");
            sa.execute("CREATE TABLE TWO as SELECT * FROM ONE");
            {
                ResultSet rs = sa.executeQuery("SELECT * FROM ONE");
                ResultSetMetaData md = rs.getMetaData();
                assertEquals(md.getPrecision(1), 12);
                assertEquals(md.getPrecision(2), 126);
                assertEquals(md.getColumnType(1), Types.NUMERIC);
                assertEquals(md.getColumnType(2), Types.NUMERIC);
            }
            {
                ResultSet rs = sa.executeQuery("SELECT * FROM TWO");
                ResultSetMetaData md = rs.getMetaData();
                assertEquals(md.getPrecision(1), 12);
                assertEquals(md.getPrecision(2), 126);
                assertEquals(md.getColumnType(1), Types.NUMERIC);
                assertEquals(md.getColumnType(2), Types.NUMERIC);
            }
            ora.close();
        }
    }

    @Test
    public void datepart() throws Exception {
        Statement stat = ca.createStatement();

        Utils.drop(stat, "DROP TABLE TEST");

        stat.execute("CREATE TABLE TEST(X INT)");
        stat.execute("INSERT INTO TEST VALUES(1)");

/*
        TODO: what was datepart function is all about?
        ResultSetMetaData meta = stat.executeQuery("select datepart(X) FROM TEST").getMetaData();
        assertEquals(10, meta.getPrecision(1));
*/

        stat.execute("DROP TABLE TEST");
    }

    @Test
    public void function() throws Exception {
        Statement sa = ca.createStatement();
        sa.execute("CREATE TABLE one (C CHARACTER(12))");

        ResultSet rs = sa.executeQuery("SELECT UPPER (C), CHAR(10), CONCAT(C,C,C), HEXTORAW(C), RAWTOHEX(C) FROM one;");
        ResultSetMetaData md = rs.getMetaData();
        assertEquals(md.getPrecision(1), Constants.BUILD_ID > 200 ? 1048576 : 12);
        assertEquals(md.getPrecision(2), 1);
        assertEquals(md.getPrecision(3), 36);
        assertEquals(md.getPrecision(4), 3);
        assertEquals(md.getPrecision(5), 48);
    }

    @Test
    public void number() throws Exception {
        Statement stat = ca.createStatement();

        Utils.drop(stat, "DROP TABLE TEST");

        stat.execute("CREATE TABLE TEST(X INT)");
        stat.execute("INSERT INTO TEST VALUES(1)");

        ResultSetMetaData meta = stat.executeQuery("select abs(X) FROM TEST").getMetaData();
        assertEquals(meta.getPrecision(1), Constants.BUILD_ID > 200 ? 32 : 10);

        stat.execute("DROP TABLE TEST");
    }

    @Test
    public void precision() throws Exception {
        Statement sa = ca.createStatement();
        sa.execute("CREATE TABLE ONE (X NUMBER(12,2))");
        sa.execute("CREATE TABLE TWO (Y NUMBER(12,2))");

        ResultSet rs = sa.executeQuery("SELECT * FROM ONE, TWO;");
        ResultSetMetaData md = rs.getMetaData();
        assertEquals(md.getPrecision(1), 12);
    }

    @Test
    public void substringH2() throws Exception {
        Statement stat = ca.createStatement();

        Utils.drop(stat, "DROP TABLE TEST");

        stat.execute("CREATE TABLE TEST(ID INT, NAME VARCHAR(10))");
        stat.execute("INSERT INTO TEST VALUES(1, 'Hello'), (2, 'WorldPeace')");

        checkPrecision(9, "SELECT SUBSTR(NAME, 2) FROM TEST", stat);
        checkPrecision(10, "SELECT SUBSTR(NAME, ID) FROM TEST", stat);
        checkPrecision(4, "SELECT SUBSTR(NAME, 2, 4) FROM TEST", stat);
        checkPrecision(Constants.BUILD_ID > 200 ? 1 : 0, "SELECT SUBSTR(NAME, 12, 4) FROM TEST", stat);
        checkPrecision(3, "SELECT SUBSTR(NAME, 8, 4) FROM TEST", stat);
        checkPrecision(4, "SELECT SUBSTR(NAME, 7, 4) FROM TEST", stat);
        checkPrecision(8, "SELECT SUBSTR(NAME, 3, ID*0) FROM TEST", stat);

        stat.execute("DROP TABLE TEST");
    }

    private void checkPrecision(int expected, String sql, Statement stat) throws Exception {
        ResultSetMetaData meta = stat.executeQuery(sql).getMetaData();
        assertEquals(expected, meta.getPrecision(1));
        int type = meta.getColumnType(1);
        assertTrue(type != 0);
    }

    @Test
    public void substringOracle() throws Exception {
        if (TestConfig.ORACLE) {
            Connection ora = TestConfig.getOracleConnection();
            Statement stat = ora.createStatement();

            Utils.drop(stat, "DROP TABLE TEST");

            stat.execute("CREATE TABLE TEST(ID INT, NAME VARCHAR(10))");
            stat.execute("INSERT INTO TEST VALUES(1, 'Hello')");
            stat.execute("INSERT INTO TEST VALUES (2, 'WorldPeace')");

            checkPrecision(9, "SELECT SUBSTR(NAME, 2) FROM TEST", stat);
            checkPrecision(10, "SELECT SUBSTR(NAME, ID) FROM TEST", stat);
            checkPrecision(4, "SELECT SUBSTR(NAME, 2, 4) FROM TEST", stat);
            checkPrecision(0, "SELECT SUBSTR(NAME, 12, 4) FROM TEST", stat);
            checkPrecision(3, "SELECT SUBSTR(NAME, 8, 4) FROM TEST", stat);
            checkPrecision(4, "SELECT SUBSTR(NAME, 7, 4) FROM TEST", stat);
            checkPrecision(8, "SELECT SUBSTR(NAME, 3, ID*0) FROM TEST", stat);

            stat.execute("DROP TABLE TEST");
            ora.close();
        }
    }

    @Test(groups = {"teradata"})
    public void teradata() throws Exception {

        if (TestConfig.TERADATA) {

            Connection ca = TestConfig.getTeradataConnection();

            Statement sa = ca.createStatement();
            Utils.drop(sa, "DROP TABLE one ");
            sa.execute("CREATE TABLE one (C CHARACTER(12), D DATE, T TIME, DT TIMESTAMP, B FLOAT , C1 CHAR(1), VC VARCHAR(20) )");

            ResultSet rs = sa.executeQuery("select C, D, T, DT, B, C1, VC FROM ONE");
            ResultSetMetaData md = rs.getMetaData();

            assertEquals(md.getColumnType(1), Types.CHAR);
            assertEquals(md.getColumnType(2), Types.DATE);

            assertEquals(md.getPrecision(1), 12);
            assertEquals(md.getScale(1), 0);
            assertEquals(md.getColumnDisplaySize(1), 12);

            assertEquals(md.getPrecision(2), 10);
            assertEquals(md.getScale(2), 0);
            assertEquals(md.getColumnDisplaySize(2), 10);

            assertEquals(md.getPrecision(3), 15);
            assertEquals(md.getScale(3), 6);
            assertEquals(md.getColumnDisplaySize(3), 15);

            assertEquals(md.getPrecision(4), 26);
            assertEquals(md.getScale(4), 6);
            assertEquals(md.getColumnDisplaySize(4), 26);

            assertEquals(md.getPrecision(5), 15);
            assertEquals(md.getScale(5), 0);
            assertEquals(md.getColumnDisplaySize(5), 22);

            assertEquals(md.getPrecision(6), 1);
            assertEquals(md.getScale(6), 0);
            assertEquals(md.getColumnDisplaySize(6), 1);

            assertEquals(md.getPrecision(7), 20);
            assertEquals(md.getScale(7), 0);
            assertEquals(md.getColumnDisplaySize(7), 20);


            ca.close();
        }
    }

    @Test
    public void testSizesForH2() throws Exception {
        Statement sa = ca.createStatement();

        sa.execute("CREATE TABLE one (C CHARACTER(12), D DATE, T TIME, DT TIMESTAMP)");

        ResultSet rs = sa.executeQuery("SELECT C, D, D, DT FROM one;");
        ResultSetMetaData md = rs.getMetaData();
        assertEquals(md.getPrecision(1), 12);
        //noinspection ConstantConditions
        assertEquals(md.getPrecision(2), Constants.BUILD_ID > 200 ? 10 : 8);
        //noinspection ConstantConditions
        assertEquals(md.getPrecision(3), Constants.BUILD_ID > 200 ? 10 : 8);
        //noinspection ConstantConditions
        assertEquals(md.getPrecision(4), Constants.BUILD_ID > 200 ? 26 : 23);
    }

    @Test(groups = {"db2"})
    public void testSubstringDB2() throws Exception {
        if (DB2) {
            Class.forName("com.ibm.db2.jcc.DB2Driver");
            Connection ca = DriverManager.getConnection("jdbc:db2:sample", "db2admin", "pass4db2");
            Statement stat = ca.createStatement();
            Utils.drop(stat, "DROP TABLE TEST");

            stat.execute("CREATE TABLE TEST(ID INT, NAME VARCHAR(10))");
            stat.execute("INSERT INTO TEST VALUES(1, 'Hello'), (2, 'WorldPeace')");

            checkPrecision(10, "SELECT SUBSTR(NAME, 2) FROM TEST", stat);  // DIFFERENT
            checkPrecision(10, "SELECT SUBSTR(NAME, ID) FROM TEST", stat);
            checkPrecision(4, "SELECT SUBSTR(NAME, 2, 4) FROM TEST", stat);

            try {
                checkPrecision(0, "SELECT SUBSTR(NAME, 12, 4) FROM TEST", stat); // runtime error
                fail("error expected");
            } catch (Exception e) {
            }

            try {
                checkPrecision(3, "SELECT SUBSTR(NAME, 8, 4) FROM TEST", stat);  // runtime error
                fail("error expected");
            } catch (Exception e) {
            }

            checkPrecision(4, "SELECT SUBSTR(NAME, 7, 4) FROM TEST", stat);
            checkPrecision(10, "SELECT SUBSTR(NAME, 3, ID*0) FROM TEST", stat);

            stat.execute("DROP TABLE TEST");
            ca.close();
        }
    }
}
