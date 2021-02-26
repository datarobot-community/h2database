package com.dullesopen.h2test.features;

import org.h2.store.fs.FileUtils;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class ConvertToUpperCaseTest {
// ------------------------------ FIELDS ------------------------------

    private Connection h2;
    private Statement stat;

// -------------------------- TEST METHODS --------------------------

    @BeforeSuite
    protected void beforeSuite() throws Exception {
    }

    @BeforeMethod
    protected void setUp() throws Exception {
        Class.forName("org.h2.Driver");
        FileUtils.deleteRecursive("./target/db",false);
        h2 = DriverManager.getConnection("jdbc:h2:./target/db/upper;DATABASE_TO_UPPER=false");

        stat = h2.createStatement();
    }

    @AfterMethod
    protected void tearDown() throws Exception {
        h2.close();
    }


    @Test
    public void hex() throws Exception {

        Statement stat = h2.createStatement();
        ResultSet rs = stat.executeQuery("SELECT -0x1234567890abcd FROM DUAL");
        rs.next();
        Assert.assertEquals("-5124095575370701", rs.getString(1));

    }


}
