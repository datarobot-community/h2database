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
