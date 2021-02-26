package com.dullesopen.h2test.syntax;

import com.dullesopen.h2test.Utils;
import org.h2.api.ErrorCode;
import org.h2.store.fs.FileUtils;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import static org.testng.Assert.assertEquals;

public class DeleteTest {
// ------------------------------ FIELDS ------------------------------

    private static final String DIR = "target/db";

// -------------------------- TEST METHODS --------------------------

    @BeforeMethod
    protected void setUp() throws Exception {
        Class.forName("org.h2.Driver");
        FileUtils.deleteRecursive(DIR, false);
        new File(DIR).mkdirs();
    }

    @AfterMethod
    protected void tearDown() throws Exception {
        File file = new File(DIR);
        FileUtils.deleteRecursive(DIR, false);
    }

// -------------------------- OTHER METHODS --------------------------

    @Test
    public void deleteView() throws Exception {
        Connection ca = DriverManager.getConnection("jdbc:h2:file:./target/db/view");
        Statement statement = ca.createStatement();
        statement.execute("CREATE TABLE A(VAR CHAR(6))");
        statement.execute("CREATE VIEW B AS (SELECT * FROM A)");
        statement.execute("DROP TABLE A CASCADE");
        ca.close();

        Connection cb = DriverManager.getConnection("jdbc:h2:file:./target/db/view");
        statement = cb.createStatement();
        try {
            statement.execute("SELECT * FROM B");
            Assert.fail("did not drop");
        } catch (SQLException e) {
            Assert.assertEquals(e.getErrorCode(), ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1);
            assertEquals(Utils.truncate(e), "Table \"B\" not found");
        }
        cb.close();
    }

    @Test
    public void star() throws Exception {
        Class.forName("org.h2.Driver");
        Connection ca = DriverManager.getConnection("jdbc:h2:mem:");
        Statement statement = ca.createStatement();
        statement.execute("CREATE TABLE A(VAR CHAR(6))");


        String s1 = "DELETE from A";
        statement.execute(s1);

        ca.close();
    }
}
