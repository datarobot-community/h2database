package com.dullesopen.h2test.features;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;


public class CompareTest {

    private Connection h2;

    @BeforeMethod
    protected void setUp() throws Exception {
        Class.forName("org.h2.Driver");
        h2 = DriverManager.getConnection("jdbc:h2:mem:");
    }

    @AfterMethod
    protected void tearDown() throws Exception {
        h2.close();
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