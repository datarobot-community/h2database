package com.dullesopen.h2test.features;

import org.h2.engine.SysProperties;
import org.h2.jdbc.JdbcResultSetMetaData;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.sql.*;

public class CreateTableTest {
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
    public void create() throws Exception {

        Statement stat = h2.createStatement();

        stat.execute("CREATE TABLE FOO (A INTEGER)");
        stat.execute("INSERT INTO FOO VALUES(10)");
        stat.execute("INSERT INTO FOO VALUES(20)");
        stat.execute("INSERT INTO FOO VALUES(30)");

        stat.execute("CREATE TABLE BAR AS SELECT * FROM FOO");
        Assert.assertEquals(stat.getUpdateCount(), 3);
    }

    @Test
    public void extension() throws Exception {
        Statement stat = h2.createStatement();
        stat.execute("CREATE TABLE FOO (A INTEGER extension 'foo')");
        JdbcResultSetMetaData rs = stat.executeQuery("SELECT * FROM FOO").getMetaData().unwrap(JdbcResultSetMetaData.class);
        Assert.assertEquals(rs.getExtension(1),"foo");
    }
}
