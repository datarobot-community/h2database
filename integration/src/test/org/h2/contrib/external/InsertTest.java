
package org.h2.contrib.external;

import org.h2.contrib.test.Utils;
import org.h2.jdbc.JdbcConnection;
import org.h2.store.fs.FileUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.sql.*;

public class InsertTest {
// ------------------------------ FIELDS ------------------------------

    public static final String DIR = "target/insert";
    private Connection h2;
    private final int SIZE = 100;

// -------------------------- TEST METHODS --------------------------

    @BeforeClass
    protected void setUp() throws Exception {
        FileUtils.deleteRecursive(DIR, false);
        new File(DIR).mkdirs();

        Class.forName("org.h2.Driver");
        h2 = DriverManager.getConnection("jdbc:h2:mem:");

        init();
    }

    void init() throws SQLException {
        Statement stat;
        stat = h2.createStatement();
        stat.execute(Init.schema(DIR));

        stat.execute("CREATE TABLE S.T (ID INT , ZIPNOTE CHAR(1000))");
        stat.execute("CREATE INDEX S.I ON S.T (ID)");
    }

    @AfterClass
    protected void tearDown() throws Exception {
        h2.close();
        FileUtils.deleteRecursive(DIR, false);
        Assert.assertFalse(new File(DIR).exists());
    }

// -------------------------- OTHER METHODS --------------------------

    @Test
    public void prepare() throws Exception {
        populate();
    }

    private void populate() throws SQLException {
        try (Statement statement = h2.createStatement()) {

            StringBuilder buf = new StringBuilder();
            for (int j = 0; j < 900; j++) {
                buf.append("X");
            }

            try (PreparedStatement p = h2.prepareStatement("INSERT INTO S.T VALUES (?,?)")) {
                for (int i = 0; i < SIZE; i++) {
                    p.setString(1, String.valueOf(i));
                    p.setString(2, buf.toString() + String.valueOf(i));
                    p.executeUpdate();
                    if (i % 10000 == 0) {
                        h2.commit();
                    }
                }
            }
        }
    }


}
