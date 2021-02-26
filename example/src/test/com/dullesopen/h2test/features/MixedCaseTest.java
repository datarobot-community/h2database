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

public class MixedCaseTest {
// ------------------------------ FIELDS ------------------------------

    private Connection h2;

// -------------------------- TEST METHODS --------------------------

    @BeforeSuite
    protected void beforeSuite() throws Exception {
    }

    @BeforeMethod
    protected void setUp() throws Exception {
        Class.forName("org.h2.Driver");
        FileUtils.deleteRecursive("./target/db", false);
        h2 = DriverManager.getConnection("jdbc:h2:./target/db/mixed;MIXED_CASE=true;LAZY_QUERY_EXECUTION=1");

        Statement stat = h2.createStatement();
        stat.execute("CREATE TABLE tBl ( cLMn INTEGER)");
        stat.execute("INSERT INTO TbL(ClmN) VALUES(10)");
    }

    @AfterMethod
    protected void tearDown() throws Exception {
        h2.close();
    }

// -------------------------- OTHER METHODS --------------------------

    @Test
    public void alias() throws Exception {
        Statement stat = h2.createStatement();
        ResultSet rs = stat.executeQuery("SELECT clMN+1 AnoTher FROM tBL");
        Assert.assertEquals(rs.getMetaData().getColumnName(1), "AnoTher");
        rs.next();
        Assert.assertEquals(11, rs.getInt("aNoThEr"));
    }

    @Test
    public void create() throws Exception {
        Statement stat = h2.createStatement();
        ResultSet rs = stat.executeQuery("SELECT * FROM tBL");
        Assert.assertEquals("cLMn", rs.getMetaData().getColumnName(1));
    }

    @Test
    public void hex() throws Exception {
        Statement stat = h2.createStatement();
        ResultSet rs = stat.executeQuery("SELECT -0x1234567890abcd FROM DUAL");
        rs.next();
        Assert.assertEquals(rs.getLong(1), -5124095575370701L);
    }

    @Test
    public void meta() throws Exception {
        Assert.assertTrue(h2.getMetaData().storesMixedCaseIdentifiers());
        Assert.assertFalse(h2.getMetaData().storesUpperCaseIdentifiers());
        Assert.assertFalse(h2.getMetaData().storesLowerCaseIdentifiers());
        Assert.assertTrue(h2.getMetaData().supportsMixedCaseIdentifiers());
    }

    @Test
    public void quoted() throws Exception {
        {
            Statement stat = h2.createStatement();
            stat.execute("CREATE TABLE \"Quo-ted\" ( \"cLMn\" INTEGER)");
            stat.execute("INSERT INTO \"Quo-ted\"(\"cLMn\") VALUES(10)");
            ResultSet rs = stat.executeQuery("SELECT \"cLMn\" FROM \"Quo-ted\"");
            Assert.assertEquals("cLMn", rs.getMetaData().getColumnName(1));
            rs.next();
            Assert.assertEquals(10, rs.getInt("CLMN"));
        }
/*
        h2.close();
        h2 = DriverManager.getConnection("jdbc:h2:./target/db/mixed;MIXED_CASE=true");
        {
            Statement stat = h2.createStatement();
            ResultSet rs = stat.executeQuery("SELECT CLmn FROM tBL");
            Assert.assertEquals("cLMn", rs.getMetaData().getColumnName(1));
            rs.next();
            Assert.assertEquals(10, rs.getInt("clmN"));
        }
*/
    }

    @Test
    public void select() throws Exception {
        {
            Statement stat = h2.createStatement();
            ResultSet rs = stat.executeQuery("SELECT CLmn FROM tBL");
            Assert.assertEquals("cLMn", rs.getMetaData().getColumnName(1));
            rs.next();
            Assert.assertEquals(10, rs.getInt("clmN"));
        }
        h2.close();
        h2 = DriverManager.getConnection("jdbc:h2:./target/db/mixed;MIXED_CASE=true");
        {
            Statement stat = h2.createStatement();
            ResultSet rs = stat.executeQuery("SELECT CLmn FROM tBL");
            Assert.assertEquals("cLMn", rs.getMetaData().getColumnName(1));
            rs.next();
            Assert.assertEquals(10, rs.getInt("clmN"));
        }
    }

    @Test
    public void view() throws Exception {
        Statement stat = h2.createStatement();
        stat.execute("CREATE TABLE ONE (x integer)");

        {
            stat.execute("CREATE VIEW TWO as SELECT x+x as TwoX FROM one");
            ResultSet rs = stat.executeQuery("SELECT * FROM TWO");
            Assert.assertEquals(rs.getMetaData().getColumnName(1), "TwoX");
        }
        {
            // table for comparison
            stat.execute("CREATE TABLE THREE as SELECT x+x as TwoX FROM one");
            ResultSet rs = stat.executeQuery("SELECT * FROM THREE");
            Assert.assertEquals(rs.getMetaData().getColumnName(1), "TwoX");
        }
    }

    @Test
    public void union() throws Exception {
        Statement stat = h2.createStatement();
        stat.execute("CREATE TABLE ONE (xYz integer)");

        {
            stat.execute("" +
                    "create table two as" +
                    "  select * from " +
                    "  (select * from one outer union select * from one)");
            ResultSet rs = stat.executeQuery("SELECT * FROM TWO");
            Assert.assertEquals(rs.getMetaData().getColumnName(1), "xYz");
        }
    }
}
