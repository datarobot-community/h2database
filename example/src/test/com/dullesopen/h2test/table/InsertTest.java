
package com.dullesopen.h2test.table;

import com.dullesopen.h2test.TestConfig;
import com.dullesopen.h2test.Utils;
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

    /**
     * This test illustrates that INSERT statement does not work with Teradata in FASTLOAD mode
     *
     * @throws Exception
     */
    @Test(groups = {"teradata"}, enabled = false)
    public void teradata() throws Exception {


        int size = 100;

        try (Statement stat = h2.createStatement()) {

            stat.execute("CREATE TABLE S.SOURCE (ID INT , TEXT VARCHAR(6))");

            try (PreparedStatement p = h2.prepareStatement("INSERT INTO S.SOURCE VALUES (?,?)")) {
                for (int i = size; i < 2 * size; i++) {
                    p.setInt(1, i);
                    p.setString(2, String.valueOf(i));
                    p.executeUpdate();
                    if (i % 10000 == 0) {
                        h2.commit();
                    }
                }
            }
        }

        try (Connection tera = TestConfig.getTeradataConnectionFastLoad()) {

            ((JdbcConnection) h2).addExternalConnection("tera", tera);

            try (Statement sb = h2.createStatement()) {
                sb.execute("CREATE SCHEMA TERA LINKED ('tera','')");
            }

            try (Statement stat = tera.createStatement()) {
                Utils.drop(stat, "DROP TABLE DEST");
                stat.execute("CREATE TABLE DEST (ID INT , TEXT VARCHAR(6))");
            }

            try (Statement stat = h2.createStatement()) {
                stat.execute("INSERT INTO TERA.DEST SELECT * FROM S.SOURCE");
                Assert.fail();
            } catch (org.h2.jdbc.JdbcSQLException e) {
                SQLException cause = (SQLException) e.getCause();
                Assert.assertEquals(cause.getSQLState(), "HY000");
                Assert.assertEquals(cause.getErrorCode(), 1093);
                String message = cause.getMessage();
                message = message.replaceAll(" \\[TeraJDBC \\d\\d.\\d\\d.\\d\\d.\\d\\d]", "");
                Assert.assertEquals(message, "[Teradata JDBC Driver] [Error 1093] [SQLState HY000] This method is not implemented");
            }
        }
    }


}
