package com.dullesopen.h2test.features;


import com.dullesopen.h2test.Utils;
import org.h2.api.ErrorCode;
import org.h2.engine.SysProperties;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.sql.*;
import java.text.MessageFormat;

import static org.testng.Assert.assertEquals;

public class AliasTest {
// ------------------------------ FIELDS ------------------------------

    private static final String FS = "fs";
    private static String CLASS = AliasTest.class.getName();
    private Connection h2;

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

    @BeforeSuite
    static public void beforeSuite() {
        System.getProperties().put("h2.doubleNanSameAsNull", "true");
    }

// -------------------------- STATIC METHODS --------------------------

    public static double foo(String s, int i) {
        return i;
    }

    public static double foo(String s, int i, double d1, double d2) {
        return i + d1 + d2;
    }

    @SuppressWarnings("UnusedDeclaration")
    public static String fs(String s) {
        return s;
    }

    public static double my(double x) {
        return 10 * x;
    }

    public static double my(double x, int y) {
        return 100 * x + 10 * y;
    }

    @SuppressWarnings("UnusedDeclaration")
    public static double nan(double x) {
        return Double.NaN;
    }

    @SuppressWarnings("UnusedDeclaration")
    public static double mean(double... values) {
        double sum = 0;
        for (double x : values) {
            sum += x;
        }
        return sum / values.length;
    }

    @SuppressWarnings("UnusedDeclaration")
    public static String print(String prefix, double... values) {
        double sum = 0;
        for (double x : values) {
            sum += x;
        }
        return prefix + ": " + (int) (sum / values.length);
    }

    @SuppressWarnings("UnusedDeclaration")
    public static void init(Connection c) {
    }

// -------------------------- OTHER METHODS --------------------------

    @Test
    public void connectionAsFirstArgument() throws Exception {
        Statement statement = h2.createStatement();
        statement.execute(MessageFormat.format("CREATE ALIAS IF NOT EXISTS MY_INIT FOR \"{0}.init\"", CLASS));
        statement.execute("CALL MY_INIT()");
        statement.close();
    }

    @Test
    public void elipses() throws Exception {
        Statement statement = h2.createStatement();
        {
            statement.execute(MessageFormat.format("CREATE ALIAS mean FOR \"{0}.mean\"", CLASS));

            String sql = "select mean(10), mean(10,20), mean (10,20,30) ";
            ResultSet rs = statement.executeQuery(sql);
            rs.next();
            assertEquals(rs.getDouble(1), 10.0);
            assertEquals(rs.getDouble(2), 15.0);
            assertEquals(rs.getDouble(3), 20.0);
        }
        {
            statement.execute(MessageFormat.format("CREATE ALIAS print FOR \"{0}.print\"", CLASS));
            String sql = "select print('A',10), print('BB',10,20), print ('CCC',10,20,30) ";
            ResultSet rs = statement.executeQuery(sql);
            rs.next();
            assertEquals(rs.getString(1), "A: 10");
            assertEquals(rs.getString(2), "BB: 15");
            assertEquals(rs.getString(3), "CCC: 20");
        }
        statement.close();
    }

    @Test
    public void nan() throws Exception {
        {
            Assert.assertTrue(SysProperties.DOUBLE_NAN_SAME_AS_NULL);
            Statement statement = h2.createStatement();
            statement.execute(MessageFormat.format("CREATE ALIAS isnan FOR \"java.lang.Double.isNaN\"", CLASS));

            String sql = "select isnan(NULL) ";
            ResultSet rs = statement.executeQuery(sql);
            rs.next();
            assertEquals(rs.getBoolean(1), true);
            statement.close();
        }
        {
            Assert.assertTrue(SysProperties.DOUBLE_NAN_SAME_AS_NULL);
            Statement statement = h2.createStatement();
            statement.execute(MessageFormat.format("CREATE ALIAS nan FOR \"{0}.nan\"", CLASS));

            String sql = "select nan(10) is null";
            ResultSet rs = statement.executeQuery(sql);
            rs.next();
            assertEquals(rs.getBoolean(1), true);
            statement.close();
        }
    }

    @Test
    public void overload() throws Exception {
        Statement sa = h2.createStatement();
        sa.execute("CREATE ALIAS foo FOR \"" + CLASS + ".foo\"");

        ResultSet rs1 = sa.executeQuery("SELECT foo('a',2)");
        rs1.next();
        assertEquals(rs1.getDouble(1), 2.0);

        ResultSet rs2 = sa.executeQuery("SELECT foo('a',2,3,4)");
        rs2.next();
        assertEquals(rs2.getDouble(1), 9.0);

        ResultSet rs;
        try {
            sa.executeQuery("SELECT foo()");
            Assert.fail();
        } catch (SQLException e) {
            Assert.assertEquals(e.getErrorCode(), ErrorCode.METHOD_NOT_FOUND_1);
            Assert.assertEquals(Utils.truncate(e),
                    "Method \"FOO (com.dullesopen.h2test.features.AliasTest, parameter count: 0)\" not found");
        }

        try {
            sa.executeQuery("SELECT foo('a')");
            Assert.fail();
        } catch (SQLException e) {
            Assert.assertEquals(e.getErrorCode(), ErrorCode.METHOD_NOT_FOUND_1);
            Assert.assertEquals(Utils.truncate(e),
                    "Method \"FOO (com.dullesopen.h2test.features.AliasTest, parameter count: 1)\" not found");
        }

        try {
            sa.executeQuery("SELECT foo(2,'a')");
            Assert.fail();
        } catch (SQLException e) {
            Assert.assertEquals(e.getErrorCode(), ErrorCode.DATA_CONVERSION_ERROR_1);
            Assert.assertEquals(Utils.truncate(e),
                    "Data conversion error converting \"a\"");
        }

        try {
            sa.executeQuery("SELECT foo('a',2,3)");
            Assert.fail();
        } catch (SQLException e) {
            Assert.assertEquals(e.getErrorCode(), ErrorCode.METHOD_NOT_FOUND_1);
            Assert.assertEquals(Utils.truncate(e),
                    "Method \"FOO (com.dullesopen.h2test.features.AliasTest, parameter count: 3)\" not found");
        }

        try {
            sa.executeQuery("SELECT foo('a',2,3,4,5)");
            Assert.fail();
        } catch (SQLException e) {
            Assert.assertEquals(e.getErrorCode(), ErrorCode.METHOD_NOT_FOUND_1);
            Assert.assertEquals(Utils.truncate(e),
                    "Method \"FOO (com.dullesopen.h2test.features.AliasTest, parameter count: 5)\" not found");
        }
    }

    @Test
    public void overloadedFunction() throws Exception {
        Statement statement = h2.createStatement();
        statement.execute("CREATE TABLE foo(a DOUBLE, b INTEGER)");
        statement.execute("INSERT INTO foo values(10,20)");

        statement.execute("CREATE ALIAS my FOR \"" + CLASS + ".my\"");


        String sql = "select my(a), my(a,b) from foo";
        ResultSet rs = statement.executeQuery(sql);
        rs.next();
        assertEquals(rs.getDouble(1), 100.0);
        assertEquals(rs.getDouble(2), 1200.0);
        statement.close();
    }

    @Test
    public void precision() throws Exception {
        Statement statement = h2.createStatement();
        statement.execute("CREATE TABLE foo(a CHAR(6))");
        statement.execute("INSERT INTO foo values('abcdef')");

        statement.execute(MessageFormat.format("CREATE ALIAS my FOR \"{0}.{1}\" PRECISION(ARG,1)", CLASS, FS));
        statement.execute("CREATE TABLE bar as select my(a) from foo"); // throws exception here

        String sql = "select * from bar";
        ResultSet rs = statement.executeQuery(sql);

        assertEquals(rs.getMetaData().getPrecision(1), 6);
        statement.close();
    }
}
