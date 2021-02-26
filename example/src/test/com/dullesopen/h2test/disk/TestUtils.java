package com.dullesopen.h2test.disk;

import com.dullesopen.h2.external.DiskSchemaFactory;
import org.h2.store.fs.FileUtils;
import org.testng.Assert;

import java.io.File;
import java.sql.*;
import java.text.MessageFormat;

/**
 * Misc util methods used by test classes
 */
class TestUtils {
// ------------------------------ FIELDS ------------------------------

    static final String URL = "jdbc:h2:mem:";
    static final String DIR = "target/external";

// -------------------------- STATIC METHODS --------------------------

    static void setUpConnection() throws Exception {
        Class.forName("org.h2.Driver");
        File dir = new File(DIR);
        if (dir.exists())
            FileUtils.deleteRecursive(DIR, false);
        dir.mkdirs();
    }

    static void tearDownConnection() throws Exception {
        FileUtils.deleteRecursive(DIR, false);
    }

    /**
     * Check the all temporary files are closed on connection close and the directory can be removed
     */
    public static void cleanClose() throws Exception {
        int N = 100;
        {
            Connection connection = DriverManager.getConnection(URL);
            create(N, connection, true);
            connection.close();
        }
        {
            // verify that all files can be safely removed and nothing is opened
            FileUtils.deleteRecursive(DIR, false);
            File dir = new File(DIR);
            Assert.assertFalse(dir.exists());
            dir.mkdirs();
        }
    }

    static void create(int n, Connection connection, boolean index) throws SQLException {
        connection.setAutoCommit(false);
        final Statement statement = connection.createStatement();
        schemas(statement, true);
        statement.execute("CREATE TABLE mylib.one (X INTEGER, Y VARCHAR, Z DOUBLE PRECISION)");
        statement.execute("CREATE TABLE mylib.two (y DOUBLE PRECISION)");

        PreparedStatement p1 = connection.prepareStatement("INSERT INTO mylib.one (X,Y,Z) VALUES (?,?,?)");
        PreparedStatement p2 = connection.prepareStatement("INSERT INTO mylib.two (y) VALUES (?)");
        for (int i = 1; i <= n; i++) {
            p1.setInt(1, i);
            p1.setString(2, "ABC" + i);
            p1.setDouble(3, 1. / i);
            p1.executeUpdate();
            p2.setInt(1, n - i);
            p2.executeUpdate();
        }

        statement.execute("commit");
        if (index) {
            statement.execute("DROP INDEX MYLIB.ONE$I IF EXISTS");
            statement.execute("DROP INDEX MYLIB.TWO$I IF EXISTS");
            statement.execute("CREATE INDEX I ON MYLIB.ONE(X)");
            statement.execute("CREATE INDEX MYLIB.I ON MYLIB.TWO(Y)");
            statement.execute("DROP INDEX MYLIB.ONE$I");
            statement.execute("DROP INDEX MYLIB.TWO$I");
            statement.execute("CREATE INDEX I ON MYLIB.ONE(X)");
            statement.execute("CREATE INDEX MYLIB.I ON MYLIB.TWO(Y)");
        }
        statement.close();
    }

    static void schemas(Statement statement, boolean append) throws SQLException {
        String schema = MessageFormat.format("CREATE SCHEMA MYLIB EXTERNAL (''{0}'',''dir={1};append={2}'')",
                DiskSchemaFactory.class.getName(), DIR, append);
        statement.execute(schema);
    }
}
