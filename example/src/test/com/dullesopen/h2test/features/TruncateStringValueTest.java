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

import static org.testng.Assert.assertEquals;

public class TruncateStringValueTest {
// ------------------------------ FIELDS ------------------------------

    private Connection h2;

// -------------------------- TEST METHODS --------------------------

    @BeforeMethod
    protected void setUp() throws Exception {
        Class.forName("org.h2.Driver");
        h2 = DriverManager.getConnection("jdbc:h2:mem:truncate;MODE=Carolina");
    }

    @AfterMethod
    protected void tearDown() throws Exception {
        h2.close();
    }

// -------------------------- OTHER METHODS --------------------------

    @Test
    public void truncate() throws Exception {

        try (Statement s1 = h2.createStatement()) {
            s1.execute("CREATE TABLE MYTAB (S VARCHAR(5))");
            s1.execute("INSERT INTO MYTAB VALUES('abcdefg')");

            try (ResultSet rs = s1.executeQuery("SELECT S FROM MYTAB")) {
                rs.next();
                assertEquals("abcde", rs.getString("S"));
            }
        }
    }
}
