package com.dullesopen.h2test.syntax;

import org.h2.store.fs.FileUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;

public class SelectDistinct {
// ------------------------------ FIELDS ------------------------------

    private static final String DIR = "target/db";
    private Connection conn;

// -------------------------- TEST METHODS --------------------------

    @BeforeMethod
    protected void setUp() throws Exception {
        Class.forName("org.h2.Driver");
        FileUtils.deleteRecursive(DIR, false);
        new File(DIR).mkdirs();
    }

    @AfterMethod
    protected void tearDown() throws Exception {
        conn.close();
        FileUtils.deleteRecursive(DIR, false);
    }

// -------------------------- OTHER METHODS --------------------------

    @Test
    public void bug401() throws Exception {
        conn = DriverManager.getConnection("jdbc:h2:file:./target/db/distinct;MAX_MEMORY_ROWS=10");
        final int ROWS = 20;
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE ONE(S1 VARCHAR(255), S2 VARCHAR(255))");
        PreparedStatement prep = conn.prepareStatement("insert into one values(?,?)");
        for (int row = 0; row < ROWS; row++) {
            prep.setString(1, "abc");
            prep.setString(2, "def" + row);
            prep.execute();
        }
        stat.execute("CREATE TABLE TWO AS SELECT DISTINCT * FROM ONE  ORDER BY S1");

    }
}