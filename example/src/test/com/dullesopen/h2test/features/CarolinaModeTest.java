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

public class CarolinaModeTest {
// ------------------------------ FIELDS ------------------------------

    private Connection h2;

// -------------------------- TEST METHODS --------------------------

    @BeforeSuite
    protected void beforeSuite() throws Exception {
        System.setProperty("h2.disableThreeValueLogic", "true");
    }


    @BeforeMethod
    protected void setUp() throws Exception {
        Class.forName("org.h2.Driver");
        h2 = DriverManager.getConnection("jdbc:h2:mem:;MODE=Carolina");

        Statement stat = h2.createStatement();
        stat.execute("CREATE TABLE ONE ( X INTEGER)");
        stat.execute("INSERT INTO ONE VALUES(123)");
        stat.execute("CREATE TABLE TWO ( X VARCHAR(6), Y VARCHAR(6))");
        stat.execute("INSERT INTO TWO VALUES('ABC',NULL)");
    }

    @AfterMethod
    protected void tearDown() throws Exception {
        h2.close();
    }

// -------------------------- OTHER METHODS --------------------------

    @Test
    public void invert() throws Exception {

        {
            Statement stat = h2.createStatement();
            ResultSet rs = stat.executeQuery("SELECT X FROM ONE WHERE X not in (NULL,456)");
            rs.next();
            Assert.assertEquals(rs.getInt(1), 123);
            Assert.assertFalse(rs.next());
            stat.close();
        }
        {
            Statement stat = h2.createStatement();
            ResultSet rs = stat.executeQuery("SELECT X FROM ONE WHERE X in (NULL,456)");
            Assert.assertFalse(rs.next());
            stat.close();
        }
        {
            Statement stat = h2.createStatement();
            ResultSet rs = stat.executeQuery("SELECT X FROM ONE WHERE null in (NULL,456)");
            rs.next();
            Assert.assertEquals(rs.getInt(1), 123);
            Assert.assertFalse(rs.next());
            stat.close();
        }
        {
            Statement stat = h2.createStatement();
            ResultSet rs = stat.executeQuery("SELECT X FROM ONE WHERE null not in (NULL,456)");
            Assert.assertFalse(rs.next());
            stat.close();
        }
    }

    @Test
    public void concat() throws Exception {

        {
            Statement stat = h2.createStatement();
            ResultSet rs = stat.executeQuery("SELECT X||Y FROM TWO");
            rs.next();
            Assert.assertEquals(rs.getString(1), "ABC");
            Assert.assertFalse(rs.next());
            stat.close();
        }
        {
            Statement stat = h2.createStatement();
            ResultSet rs = stat.executeQuery("SELECT Y||X FROM TWO");
            rs.next();
            Assert.assertEquals(rs.getString(1), "ABC");
            Assert.assertFalse(rs.next());
            stat.close();
        }
        {
            Statement stat = h2.createStatement();
            ResultSet rs = stat.executeQuery("SELECT X||NULL FROM TWO");
            rs.next();
            Assert.assertEquals(rs.getString(1), "ABC");
            Assert.assertFalse(rs.next());
            stat.close();
        }
    }

}
