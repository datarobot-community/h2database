package org.h2.contrib.external;

import org.h2.store.fs.FileUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.sql.*;

public class DeleteTest {

    public static final String DIR = "target/delete";
    private final int SIZE = 100;

    @BeforeClass
    protected void setUp() throws Exception {
        Class.forName("org.h2.Driver");
    }

    @AfterClass
    protected void tearDown() throws Exception {
        FileUtils.deleteRecursive(DIR, false);
    }

    @Test
    public void verify() throws SQLException {
        try (Connection h2 = DriverManager.getConnection("jdbc:h2:mem:")) {
            FileUtils.deleteRecursive(DIR, false);
            new File(DIR).mkdir();

            try (Statement stat = h2.createStatement()) {
                stat.execute(Init.schema(DIR));
                stat.execute("CREATE TABLE S.T (ID INT , VALUE REAL)");
                stat.execute("CREATE INDEX S.I ON S.T (VALUE)");
            }
            try (PreparedStatement prep = h2.prepareStatement("INSERT INTO S.T VALUES(?, ?)")) {
                for (int i = 0; i < SIZE; i++) {
                    prep.setInt(1, i);
                    prep.setDouble(2, -i * i);
                    prep.executeUpdate();
                }
            }
            try (Statement stat = h2.createStatement()) {
                stat.execute("DELETE FROM S.T WHERE VALUE=-16");
                stat.execute("UPDATE S.T SET VALUE=9 WHERE VALUE=-9");
                try (ResultSet rs = stat.executeQuery("SELECT ID FROM S.T WHERE VALUE=9")) {
                    rs.next();
                    Assert.assertEquals(rs.getInt(1), 3);
                }
            }
        }
    }
}
