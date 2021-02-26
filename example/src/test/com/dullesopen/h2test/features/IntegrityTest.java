package com.dullesopen.h2test.features;

import org.h2.store.fs.FileUtils;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;


public class IntegrityTest {
// ------------------------------ FIELDS ------------------------------

    final String DIR = "target/h2/integrity";

// -------------------------- TEST METHODS --------------------------

    @BeforeClass
    protected void setUp() throws Exception {
        FileUtils.deleteRecursive(DIR, false);
        new File(DIR).mkdirs();
    }

    @AfterMethod
    protected void tearDown() throws Exception {
    }

// -------------------------- OTHER METHODS --------------------------

    @Test(enabled = false, description = "seems that this test is not valid anymore after some changes in the database")
    public void testSchema() throws Exception {
        Class.forName("org.h2.Driver");

        Connection stale = DriverManager.getConnection("jdbc:h2:mem:stale", "linkuser", "linkpass");

        {
            Statement statement = stale.createStatement();
            statement.execute("CREATE SCHEMA S");
            statement.execute("CREATE TABLE S.ONE (X INT)");
            statement.close();
        }


        {
            Connection active = DriverManager.getConnection("jdbc:h2:file:./" + DIR + "/db");
            Statement statement = active.createStatement();
            statement.execute("CREATE SCHEMA LS LINKED ('org.h2.Driver','jdbc:h2:mem:stale','linkuser','linkpass','S')");
            statement.executeQuery("SELECT * FROM LS.ONE");
            statement.close();
            active.close();
        }

        // make the original linked schema unavailable

        stale.close();
        stale = DriverManager.getConnection("jdbc:h2:mem:stale", "linkuser", "newpass");

        {
            Connection active = DriverManager.getConnection("jdbc:h2:file:./" + DIR + "/db");
            Statement statement = active.createStatement();
            // verify that linked schema was dropped
            try {
                statement.execute("DROP SCHEMA LS");
                Assert.fail("exception not thrown");
            } catch (SQLException e) {
            }
            statement.close();
            active.close();
        }
        stale.close();
    }

    @Test
    public void testTable() throws Exception {
        Class.forName("org.h2.Driver");

        Connection stale = DriverManager.getConnection("jdbc:h2:mem:stale", "linkuser", "linkpass");

        {
            Statement statement = stale.createStatement();
            statement.execute("CREATE TABLE ONE (X INT)");
            statement.close();
        }


        {
            Connection active = DriverManager.getConnection("jdbc:h2:file:./" + DIR + "/db");
            Statement statement = active.createStatement();
            statement.execute("CREATE LINKED  TABLE two ('org.h2.Driver','jdbc:h2:mem:stale','linkuser','linkpass','ONE')");
            statement.close();
            active.close();
        }

        // now the linked table is uselesss
        // Unfortunately you can not clean it from database

        stale.close();
        stale = DriverManager.getConnection("jdbc:h2:mem:stale", "linkuser", "newpass");
        {
            Connection active = DriverManager.getConnection("jdbc:h2:file:./" + DIR + "/db");
            Statement statement = active.createStatement();
            try {
                statement.execute("SELECT * FROM two");
                Assert.fail("exception not thrown");
            } catch (SQLException e) {
            }
            statement.close();
            active.close();
        }
        stale.close();
    }
}
