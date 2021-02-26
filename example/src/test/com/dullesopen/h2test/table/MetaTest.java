package com.dullesopen.h2test.table;

import org.h2.store.fs.FileUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.sql.*;

public class MetaTest {
// ------------------------------ FIELDS ------------------------------

    public static final String DIR = "target/meta";

// -------------------------- TEST METHODS --------------------------

    @BeforeClass
    protected void setUp() throws Exception {
        FileUtils.deleteRecursive(DIR, false);
        new File(DIR).mkdirs();

        Class.forName("org.h2.Driver");
        Connection h2 = DriverManager.getConnection("jdbc:h2:mem:");
        Statement stat;
        stat = h2.createStatement();
        stat.execute(Init.schema(DIR));
        stat.execute("CREATE TABLE S.T (ID INT , S VARCHAR(10))");
        h2.close();
    }

    @AfterClass
    protected void tearDown() throws Exception {
        FileUtils.deleteRecursive(DIR, false);
        Assert.assertFalse(new File(DIR).exists());
    }

// -------------------------- OTHER METHODS --------------------------

    @Test
    public void prepare() throws Exception {
        Connection connection = DriverManager.getConnection("jdbc:h2:mem:");
        Statement stat;
        stat = connection.createStatement();
        stat.execute(Init.schema(DIR));
        ResultSet rs = connection.getMetaData().getColumns(null, "S", "T", null);
        Assert.assertTrue(rs.next());
        Assert.assertEquals(rs.getString("COLUMN_NAME"),"ID");
        Assert.assertTrue(rs.next());
        Assert.assertEquals(rs.getString("COLUMN_NAME"),"S");
        Assert.assertFalse(rs.next());
        connection.close();
    }
}
