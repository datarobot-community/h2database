package com.dullesopen.h2test.features;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class DivideTest {
// ------------------------------ FIELDS ------------------------------

    private Connection h2;

// -------------------------- TEST METHODS --------------------------

    @BeforeMethod
    protected void setUp() throws Exception {
        Class.forName("org.h2.Driver");
        h2 = DriverManager.getConnection("jdbc:h2:mem:;mode=Carolina");
    }

    @AfterMethod
    protected void tearDown() throws Exception {
        h2.close();
    }

// -------------------------- OTHER METHODS --------------------------

    @Test
    public void noninteger() throws Exception {

        try (Statement sa = h2.createStatement()) {

            sa.execute("CREATE TABLE TAB(ONE INTEGER, TWO INTEGER)");
            sa.execute("INSERT INTO TAB VALUES ( 123, 456)");
            sa.execute("INSERT INTO TAB VALUES ( 222, 333)");
            try (ResultSet rs = sa.executeQuery("select one/two from tab")) {
                rs.next();
                double d = rs.getDouble(1);
                org.testng.Assert.assertEquals(d, 0.26973684210526316);
            }
            try (ResultSet rs = sa.executeQuery("select 1/count(*) from tab")) {
                rs.next();
                double d = rs.getDouble(1);
                org.testng.Assert.assertEquals(d, 0.5);
            }
            try (ResultSet rs = sa.executeQuery("select 1/0 from tab")) {
                rs.next();
                double d = rs.getDouble(1);
                org.testng.Assert.assertEquals(d, Double.POSITIVE_INFINITY);
            }
        }
    }


    @Test
    public void zero() throws Exception {

        try (Statement sa = h2.createStatement()) {

            sa.execute("CREATE TABLE TAB(ONE DOUBLE , TWO DOUBLE)");
            sa.execute("INSERT INTO TAB VALUES ( 0, 0)");
            try (ResultSet rs = sa.executeQuery("select one/two from tab")) {
                rs.next();
                double d = rs.getDouble(1);
                org.testng.Assert.assertEquals(d, 0.0);
            }

            try (ResultSet rs = sa.executeQuery("select one/two as ratio, SUM(CASE WHEN calculated ratio > 1 THEN 1 ELSE 0 END) from tab")) {
                rs.next();
                double d = rs.getDouble(2);
                org.testng.Assert.assertEquals(d, 0.0);
            }
        }
    }

}
