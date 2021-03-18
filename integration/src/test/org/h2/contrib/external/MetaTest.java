package org.h2.contrib.external;

import org.h2.store.fs.FileUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class MetaTest {
// ------------------------------ FIELDS ------------------------------

    public static final String DIR = "target/meta";

    @BeforeClass
    protected void setUp() throws Exception {
        FileUtils.deleteRecursive(DIR, false);
        new File(DIR).mkdirs();

        Class.forName("org.h2.Driver");
    }

    @AfterClass
    protected void tearDown() throws Exception {
        FileUtils.deleteRecursive(DIR, false);
        Assert.assertFalse(new File(DIR).exists());
    }

// -------------------------- OTHER METHODS --------------------------

    @Test
    public void prepare() throws Exception {
        try (Connection h2 = DriverManager.getConnection("jdbc:h2:mem:")) {
            try (Statement stat = h2.createStatement()) {
                stat.execute(Init.schema(DIR));
                stat.execute("CREATE TABLE S.T (ID INT , S VARCHAR(10))");
            }
        }
        try (Connection connection = DriverManager.getConnection("jdbc:h2:mem:")) {
            try (Statement stat = connection.createStatement()) {
                stat.execute(Init.schema(DIR));
                try (ResultSet rs = connection.getMetaData().getColumns(null, "S", "T", null)) {
                    Assert.assertTrue(rs.next());
                    Assert.assertEquals(rs.getString("COLUMN_NAME"), "ID");
                    Assert.assertTrue(rs.next());
                    Assert.assertEquals(rs.getString("COLUMN_NAME"), "S");
                    Assert.assertFalse(rs.next());
                }
            }
        }
    }
}
