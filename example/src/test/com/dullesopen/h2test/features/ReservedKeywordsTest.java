package com.dullesopen.h2test.features;

import org.h2.store.fs.FileUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.text.MessageFormat;

public class ReservedKeywordsTest {
// ------------------------------ FIELDS ------------------------------

    private Connection connection;

// -------------------------- TEST METHODS --------------------------

    @BeforeSuite
    protected void beforeSuite() throws Exception {
    }

    @BeforeMethod
    protected void setUp() throws Exception {
        Class.forName("org.h2.Driver");
        FileUtils.deleteRecursive("./target/db", false);
        connection = DriverManager.getConnection("jdbc:h2:./target/db/mixed");

    }

    @AfterMethod
    protected void tearDown() throws Exception {
        connection.close();
    }

    @Test
    public void reserved() throws Exception {
        java.sql.Statement statement = connection.createStatement();
        statement.execute("CREATE TABLE \"ALL\" (x INTEGER )");
        statement.execute("CREATE TABLE \"TOP\" (x INTEGER )");
        connection.commit();
        String sql = "select {0}.x from {0} inner join (select {0}.x from {0}) AS B on {0}.x = B.x";
        statement.execute(MessageFormat.format(sql, "\"ALL\""));
        statement.execute(MessageFormat.format(sql, "\"TOP\""));
        statement.close();
        connection.close();
    }


}
