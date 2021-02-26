package com.dullesopen.h2test.features;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import static org.testng.Assert.assertEquals;

public class CatalogTest {
// ------------------------------ FIELDS ------------------------------

    private Connection h2;

// -------------------------- TEST METHODS --------------------------

    @BeforeSuite
    static protected void beforeSuite() throws Exception {
        System.setProperty("h2.implicitRelativePath", "true");
    }

    @BeforeMethod
    protected void setUp() throws Exception {

        Class.forName("org.h2.Driver");
        Properties info = new Properties();
        info.put("CATALOG", "MYCAT");
        h2 = DriverManager.getConnection("jdbc:h2:file:target/temp", info);
    }

    @AfterMethod
    protected void tearDown() throws Exception {
        h2.close();
    }

// -------------------------- OTHER METHODS --------------------------

    @Test
    public void catalog() throws Exception {

        Statement s1 = h2.createStatement();
        ResultSet rs = h2.getMetaData().getCatalogs();
        rs.next();
        String catalog = rs.getString(1);
        assertEquals(catalog, "MYCAT");
        rs.close();
    }
}
