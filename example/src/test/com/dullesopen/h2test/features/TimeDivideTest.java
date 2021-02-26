package com.dullesopen.h2test.features;

import org.h2.engine.SysProperties;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.sql.*;

public class TimeDivideTest {
// ------------------------------ FIELDS ------------------------------

    private Connection h2;

// -------------------------- TEST METHODS --------------------------

    @BeforeSuite
    static protected void beforeSuite() throws Exception {
        System.setProperty("h2.dateTimeDivideAsDouble", "true");
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

    @Test
    public void minute() throws Exception {

        Statement sa = h2.createStatement();

        sa.execute("CREATE TABLE ONE(FIRST TIME, LAST TIME)");
        sa.execute("INSERT INTO ONE VALUES ( TIME '08:09:16', TIME '10:09:11')");
        ResultSet rs = sa.executeQuery("select (last-first)/60.0  as diff  from one");
        rs.next();
        double d = rs.getDouble(1);
        org.testng.Assert.assertEquals(d, 119.91666666666667);
    }


}
