package com.dullesopen.h2test.syntax;

import org.h2.store.fs.FileUtils;
import org.testng.Assert;
import org.testng.annotations.*;

import java.io.File;
import java.sql.*;

public class IndexTest {
// ------------------------------ FIELDS ------------------------------

    static final String DIR = "target/data/";
    private Connection connection;

    // -------------------------- STATIC METHODS --------------------------
    @BeforeMethod
    protected void setUp() throws Exception {
        Class.forName("org.h2.Driver");
        File dir = new File(DIR);
        if (dir.exists())
            FileUtils.deleteRecursive(DIR, false);
        dir.mkdirs();
        connection = DriverManager.getConnection("jdbc:h2:mem:");
    }

    @AfterMethod
    protected void tearDown() throws Exception {
        connection.close();
        FileUtils.deleteRecursive(DIR, false);
    }

// -------------------------- OTHER METHODS --------------------------

    /**
     * @throws Exception
     */
    @Test
    public void cancel() throws Exception {
        Connection connection = DriverManager.getConnection("jdbc:h2:mem:");
        final java.sql.Statement statement = connection.createStatement();
        final int N = 20000;
        create(N, connection, false);
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    ResultSet rs = statement.executeQuery("SELECT count(*) FROM mylib.one, mylib.two where one.x=two.y");
                    rs.next();
                    int count = rs.getInt(1);
                    Assert.assertEquals(N - 1, count);
                    rs.close();
                    Assert.fail("cancel failed");
                } catch (SQLException e) {
                    Assert.assertEquals("57014", e.getSQLState());
                }
            }
        }).start();
        Thread.sleep(100);
        statement.cancel();
        statement.close();
    }

    private void create(int n, Connection connection, boolean index) throws SQLException {
        final Statement statement = connection.createStatement();
        statement.execute("CREATE SCHEMA mylib");
        statement.execute("CREATE TABLE mylib.one (x INTEGER)");
        statement.execute("CREATE TABLE mylib.two (y DOUBLE PRECISION)");

        java.sql.PreparedStatement p1 = connection.prepareStatement("INSERT INTO mylib.one (x) VALUES (?)");
        java.sql.PreparedStatement p2 = connection.prepareStatement("INSERT INTO mylib.two (y) VALUES (?)");
        for (int i = 0; i < n; i++) {
            p1.setInt(1, i);
            p1.executeUpdate();
            p2.setInt(1, n - i);
            p2.executeUpdate();
        }

        statement.execute("commit");
        if (index) {
            statement.execute("DROP INDEX I IF EXISTS");
            statement.execute("CREATE INDEX I ON mylib.one(x)");
        }
        statement.close();
    }

    // -------------------------- TEST METHODS --------------------------

}
