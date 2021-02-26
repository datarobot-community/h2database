package com.dullesopen.h2test.features;

import org.h2.engine.Constants;
import org.h2.engine.SysProperties;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.sql.*;

import static org.testng.Assert.assertEquals;

/**
 * Test additional functions in H2
 */
public class FunctionTest {
// ------------------------------ FIELDS ------------------------------

    private Connection h2;

// -------------------------- TEST METHODS --------------------------

    @BeforeSuite
    static protected void beforeSuite() throws Exception {
        System.setProperty("h2.dateAddExtended", "true");
    }


    @BeforeMethod
    protected void setUp() throws Exception {
        Class.forName("org.h2.Driver");
        h2 = DriverManager.getConnection("jdbc:h2:mem:");
    }

    @AfterMethod
    protected void tearDown() throws Exception {
        h2.close();
    }

// -------------------------- OTHER METHODS --------------------------

    /**
     * @throws Exception
     */
    @Test
    public void dateadd2() throws Exception {
        Assert.assertTrue(SysProperties.DATE_ADD_EXTENDED);
        Statement statement = h2.createStatement();
        statement.execute("CREATE TABLE ONE (T TIME, D DATE, DT TIMESTAMP)");
        statement.execute("INSERT INTO ONE VALUES (TIME'11:12:13', DATE'2014-09-08', TIMESTAMP'2014-07-06 05:04:03')");
        ResultSet rs;

        rs = statement.executeQuery("select T, max(T), T + 2, T - 3, 4 + T from ONE;");
        for (int i = 1; i <= 5; i++)
            Assert.assertEquals(rs.getMetaData().getColumnType(i), Types.TIME);
        rs.next();
        Assert.assertEquals(rs.getString(1), "11:12:13");
        Assert.assertEquals(rs.getString(2), "11:12:13");
        Assert.assertEquals(rs.getString(3), "11:12:15");
        Assert.assertEquals(rs.getString(4), "11:12:10");
        Assert.assertEquals(rs.getString(5), "11:12:17");

        rs = statement.executeQuery("select D, max(D), D + 2, D - 3, 4 + D from ONE;");

        for (int i = 1; i <= 5; i++)
            Assert.assertEquals(rs.getMetaData().getColumnType(i), Types.DATE);
        rs.next();
        Assert.assertEquals(rs.getString(1), "2014-09-08");
        Assert.assertEquals(rs.getString(2), "2014-09-08");
        Assert.assertEquals(rs.getString(3), "2014-09-10");
        Assert.assertEquals(rs.getString(4), "2014-09-05");
        Assert.assertEquals(rs.getString(5), "2014-09-12");

        rs = statement.executeQuery("select DT, max(DT), DT + 2, DT - 3, 4 + DT from ONE;");

        for (int i = 1; i <= 5; i++)
            Assert.assertEquals(rs.getMetaData().getColumnType(i), Types.TIMESTAMP);
        rs.next();
        Assert.assertEquals(rs.getString(1), "2014-07-06 05:04:03.0");
        Assert.assertEquals(rs.getString(2), "2014-07-06 05:04:03.0");
        Assert.assertEquals(rs.getString(3), "2014-07-06 05:04:05.0");
        Assert.assertEquals(rs.getString(4), "2014-07-06 05:04:00.0");
        Assert.assertEquals(rs.getString(5), "2014-07-06 05:04:07.0");

    }

    /**
     * changed in version: 1.4.181.
     * DATE constant can not be specified with seconds
     */
    @Test
    public void ignoreSeconds() throws Exception {
        Statement statement = h2.createStatement();
        statement.execute("DROP TABLE TS IF EXISTS");
        statement.execute("CREATE TABLE TS (X TIMESTAMP)");
        try {
            statement.execute("INSERT INTO TS VALUES (DATE'2008-01-08 10-11-15')");
            Assert.fail();
        } catch (SQLException e) {
            Assert.assertEquals(e.getErrorCode(), 22007);
            String msg = "Cannot parse \"DATE\" constant \"2008-01-08 10-11-15\"; SQL statement:\n" +
                    "INSERT INTO TS VALUES (DATE'2008-01-08 10-11-15') [22007-196]";
            Assert.assertEquals(e.getMessage(), msg);
        }
    }

    @Test
    public void isnull() throws Exception {
        Statement statement = h2.createStatement();
        statement.execute("CREATE TABLE B(X DOUBLE)");
        statement.execute("INSERT INTO B VALUES(NULL)");
        statement.execute("INSERT INTO B VALUES(1)");


        String s1 = "SELECT is_null(X) from B";
        ResultSet rs = statement.executeQuery(s1);
        rs.next();
        assertEquals(rs.getBoolean(1), true);
        rs.next();
        assertEquals(rs.getBoolean(1), false);
    }

    @Test
    public void version() throws SQLException {
        Statement stat = h2.createStatement();
        String query = "select h2version()";
        ResultSet rs = stat.executeQuery(query);
        Assert.assertTrue(rs.next());
        String version = rs.getString(1);
        assertEquals(Constants.getVersion(), version);
        Assert.assertFalse(rs.next());
        rs.close();
        stat.close();
    }

}
