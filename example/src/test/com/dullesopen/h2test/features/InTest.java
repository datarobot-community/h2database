package com.dullesopen.h2test.features;


import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.sql.*;

public class InTest {
// ------------------------------ FIELDS ------------------------------

    private Connection h2;

// -------------------------- TEST METHODS --------------------------


    @BeforeMethod
    protected void setUp() throws Exception {
        Class.forName("org.h2.Driver");
        h2 = DriverManager.getConnection("jdbc:h2:mem:;mode=Carolina");

        try (Statement stat = h2.createStatement()) {
            stat.execute("CREATE TABLE ONE ( abc INTEGER)");
            stat.execute("CREATE TABLE TWO ( ID  INTEGER, xyz VARCHAR(3))");
            stat.execute("CREATE TABLE SUB ( klm VARCHAR(3))");

            stat.execute("INSERT INTO  ONE VALUES(123)");

            stat.execute("INSERT INTO  TWO VALUES(11,'abc')");
            stat.execute("INSERT INTO  TWO VALUES(22,null)");
            stat.execute("INSERT INTO  TWO VALUES(33,'def')");

            stat.execute("INSERT INTO  SUB VALUES('abc')");
            stat.execute("INSERT INTO  SUB VALUES(null)");
        }

    }

    @AfterMethod
    protected void tearDown() throws Exception {
        h2.close();
    }

// -------------------------- OTHER METHODS --------------------------

    @Test
    public void numeric() throws Exception {

        try (Statement stat = h2.createStatement()) {
            try (ResultSet rs = stat.executeQuery("SELECT abc FROM ONE WHERE abc not in (NULL,456)")) {
                rs.next();
                Assert.assertEquals(rs.getInt(1), 123);
                Assert.assertFalse(rs.next());
            }
        }

        try (Statement stat = h2.createStatement()) {
            try (ResultSet rs = stat.executeQuery("SELECT abc FROM ONE WHERE abc in (NULL,456)")) {
                Assert.assertFalse(rs.next());
            }
        }

        try (Statement stat = h2.createStatement()) {
            try (ResultSet rs = stat.executeQuery("SELECT abc FROM ONE WHERE null in (NULL,456)")) {
                rs.next();
                Assert.assertEquals(rs.getInt(1), 123);
                Assert.assertFalse(rs.next());
            }
        }

        try (Statement stat = h2.createStatement()) {
            try (ResultSet rs = stat.executeQuery("SELECT abc FROM ONE WHERE null not in (NULL,456)")) {
                Assert.assertFalse(rs.next());
            }
        }
    }

    @Test
    public void character() throws Exception {
        testNullOnly("NULL");
        testNullAndValue("NULL,'abc'");
    }

    @Test
    public void subquery() throws Exception {
        testNullOnly("select klm from sub where klm is null");
        testNullAndValue("select klm from sub");
    }

    private void testNullAndValue(String list) throws SQLException {
        try (Statement stat = h2.createStatement()) {
            try (ResultSet rs = stat.executeQuery("SELECT id FROM TWO WHERE xyz in (" + list + ")")) {
                rs.next();
                Assert.assertEquals(rs.getInt(1), 11);
                rs.next();
                Assert.assertEquals(rs.getInt(1), 22);
                Assert.assertFalse(rs.next());
            }
        }
        try (Statement stat = h2.createStatement()) {
            try (ResultSet rs = stat.executeQuery("SELECT id FROM TWO WHERE xyz not in (" + list + ")")) {
                rs.next();
                Assert.assertEquals(rs.getInt(1), 33);
                Assert.assertFalse(rs.next());
            }
        }
        try (Statement stat = h2.createStatement()) {
            try (ResultSet rs = stat.executeQuery("SELECT id FROM TWO WHERE null in (" + list + ")")) {
                rs.next();
                Assert.assertEquals(rs.getInt(1), 11);
                rs.next();
                Assert.assertEquals(rs.getInt(1), 22);
                rs.next();
                Assert.assertEquals(rs.getInt(1), 33);
                Assert.assertFalse(rs.next());
            }
        }
        try (Statement stat = h2.createStatement()) {
            try (ResultSet rs = stat.executeQuery("SELECT id FROM TWO WHERE null not in (" + list + ")")) {
                Assert.assertFalse(rs.next());
            }
        }
    }

    private void testNullOnly(String list) throws SQLException {
        try (Statement stat = h2.createStatement()) {
            try (ResultSet rs = stat.executeQuery("SELECT id FROM TWO WHERE xyz in (" + list + ")")) {
                rs.next();
                Assert.assertEquals(rs.getInt(1), 22);
                Assert.assertFalse(rs.next());
            }
        }
        try (Statement stat = h2.createStatement()) {
            try (ResultSet rs = stat.executeQuery("SELECT id FROM TWO WHERE xyz not in (" + list + ")")) {
                rs.next();
                Assert.assertEquals(rs.getInt(1), 11);
                rs.next();
                Assert.assertEquals(rs.getInt(1), 33);
                Assert.assertFalse(rs.next());
            }
        }
        try (Statement stat = h2.createStatement()) {
            try (ResultSet rs = stat.executeQuery("SELECT id FROM TWO WHERE null in (" + list + ")")) {
                rs.next();
                Assert.assertEquals(rs.getInt(1), 11);
                rs.next();
                Assert.assertEquals(rs.getInt(1), 22);
                rs.next();
                Assert.assertEquals(rs.getInt(1), 33);
                Assert.assertFalse(rs.next());
            }
        }
        try (Statement stat = h2.createStatement()) {
            try (ResultSet rs = stat.executeQuery("SELECT id FROM TWO WHERE null not in (" + list + ")")) {
                Assert.assertFalse(rs.next());
            }
        }
    }

}


