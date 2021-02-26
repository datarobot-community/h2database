package com.dullesopen.h2test.features;

import org.h2.engine.SysProperties;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;


public class CompareTest {
// ------------------------------ FIELDS ------------------------------

    private Connection h2;

// -------------------------- TEST METHODS --------------------------

    @BeforeMethod
    protected void setUp() throws Exception {
        Class.forName("org.h2.Driver");
        h2 = DriverManager.getConnection("jdbc:h2:mem:;MODE=Carolina");
    }

    @AfterMethod
    protected void tearDown() throws Exception {
        h2.close();
    }

// -------------------------- OTHER METHODS --------------------------

    @Test
    public void compare() throws Exception {
        try (Statement sa = h2.createStatement()) {

            sa.execute("CREATE TABLE A(X DOUBLE, Y DOUBLE)");
            sa.execute("INSERT INTO A VALUES (NULL,1)");
            sa.execute("CREATE TABLE B(X DOUBLE, Y DOUBLE)");
            sa.execute("INSERT INTO B VALUES (NULL,NULL)");

            try (ResultSet rs = sa.executeQuery("SELECT COUNT(*) FROM A WHERE X < Y")) {
                rs.next();
                Assert.assertEquals(1, rs.getInt(1));
            }

            try (ResultSet rs = sa.executeQuery("SELECT COUNT(*) FROM A WHERE Y < X")) {
                rs.next();
                Assert.assertEquals(0, rs.getInt(1));
            }

            try (ResultSet rs = sa.executeQuery("SELECT COUNT(*) FROM A WHERE Y > X")) {
                rs.next();
                Assert.assertEquals(1, rs.getInt(1));
            }

            try (ResultSet rs = sa.executeQuery("SELECT COUNT(*) FROM A WHERE X > Y")) {
                rs.next();
                Assert.assertEquals(0, rs.getInt(1));
            }

            try (ResultSet rs = sa.executeQuery("SELECT COUNT(*) FROM A WHERE X <= Y")) {
                rs.next();
                Assert.assertEquals(1, rs.getInt(1));
            }

            try (ResultSet rs = sa.executeQuery("SELECT COUNT(*) FROM A WHERE Y >= X")) {
                rs.next();
                Assert.assertEquals(1, rs.getInt(1));
            }

            try (ResultSet rs = sa.executeQuery("SELECT COUNT(*) FROM A WHERE X <> Y")) {
                rs.next();
                Assert.assertEquals(1, rs.getInt(1));
            }

            try (ResultSet rs = sa.executeQuery("SELECT COUNT(*) FROM A WHERE X = Y")) {
                rs.next();
                Assert.assertEquals(0, rs.getInt(1));
            }

            try (ResultSet rs = sa.executeQuery("SELECT COUNT(*) FROM B WHERE X < Y")) {
                rs.next();
                Assert.assertEquals(0, rs.getInt(1));
            }

            try (ResultSet rs = sa.executeQuery("SELECT COUNT(*) FROM B WHERE Y < X")) {
                rs.next();
                Assert.assertEquals(0, rs.getInt(1));
            }

            try (ResultSet rs = sa.executeQuery("SELECT COUNT(*) FROM B WHERE Y > X")) {
                rs.next();
                Assert.assertEquals(0, rs.getInt(1));
            }

            try (ResultSet rs = sa.executeQuery("SELECT COUNT(*) FROM B WHERE X > Y")) {
                rs.next();
                Assert.assertEquals(0, rs.getInt(1));
            }

            try (ResultSet rs = sa.executeQuery("SELECT COUNT(*) FROM B WHERE X <= Y")) {
                rs.next();
                Assert.assertEquals(1, rs.getInt(1));
            }

            try (ResultSet rs = sa.executeQuery("SELECT COUNT(*) FROM B WHERE Y >= X")) {
                rs.next();
                Assert.assertEquals(1, rs.getInt(1));
            }

            try (ResultSet rs = sa.executeQuery("SELECT COUNT(*) FROM B WHERE X <> Y")) {
                rs.next();
                Assert.assertEquals(0, rs.getInt(1));
            }

            try (ResultSet rs = sa.executeQuery("SELECT COUNT(*) FROM B WHERE X = Y")) {
                rs.next();
                Assert.assertEquals(1, rs.getInt(1));
            }
        }
    }


    @Test
    public void groupBy() throws Exception {

        try (Statement sa = h2.createStatement()) {
            sa.execute("CREATE TABLE ONE(X DOUBLE, Y DOUBLE, Z DOUBLE)");
            sa.execute("INSERT INTO ONE VALUES (NULL,NULL,1)");
            sa.executeQuery("select one.x from one natural join (select x, max(y) from one group by x, y)");
        }
    }

    @Test
    public void when() throws Exception {

        try (Statement sa = h2.createStatement()) {
            sa.execute("CREATE TABLE ONE(X DOUBLE, Y DOUBLE, Z DOUBLE)");
            sa.execute("INSERT INTO ONE VALUES (NULL,NULL,null)");
            sa.executeQuery("select one.x from one natural join (select x, case when y=1 then max(z) end as r from one group by x, y)");
        }
    }

}