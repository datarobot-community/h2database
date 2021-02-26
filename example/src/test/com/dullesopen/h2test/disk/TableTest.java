package com.dullesopen.h2test.disk;


import com.dullesopen.h2test.Utils;
import org.h2.api.ErrorCode;
import org.h2.store.fs.FileUtils;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.sql.*;


public class TableTest {
    // ------------------------------ FIELDS ------------------------------

    @BeforeMethod
    public void setUpConnection() throws Exception {
        TestUtils.setUpConnection();
    }

    @AfterMethod
    public void tearDownConnection() throws Exception {
        TestUtils.tearDownConnection();
    }

// -------------------------- OTHER METHODS --------------------------

    /**
     * Check the all temporary files are closed on connection close and the directory can be removed
     */
    @Test
    public void cleanClose() throws Exception {
        int N = 100;
        {
            Connection connection = DriverManager.getConnection(TestUtils.URL + ";LAZY_QUERY_EXECUTION=1");
            TestUtils.create(N, connection, true);
            connection.close();
        }
        {
            // verify that all files can be safely removed and nothing is opened
            FileUtils.deleteRecursive(TestUtils.DIR, false);
            File dir = new File(TestUtils.DIR);
            Assert.assertFalse(dir.exists());
            dir.mkdirs();
        }
    }


    /**
     * check the tables can be flushed from memory on commit and will be picked later on demand by SELECT query
     *
     * @throws Exception
     */
    @Test
    public void reuse() throws Exception {
        int N = 100;
        {
            // recreate table again and check that it is ready for testing
            Connection connection = DriverManager.getConnection(TestUtils.URL);
            TestUtils.create(N, connection, true);
            connection.close();
        }
        {
            // actually read the indices and check
            Connection connection = DriverManager.getConnection(TestUtils.URL);
            Statement statement = connection.createStatement();
            TestUtils.schemas(statement, false);
            ResultSet rs = statement.executeQuery("SELECT * FROM MYLIB.ONE");
            int i = 0;
            while (rs.next()) {
                i++;
                Assert.assertEquals(rs.getInt(1), i);
                Assert.assertEquals(rs.getDouble(3), 1. / i);
            }
            Assert.assertEquals(i, N);
            statement.close();
            connection.close();
        }
    }

    /**
     * remove the creating of temporary data set on disk
     */
    @Test (enabled = false)
    public void read() throws Exception {
        int N = 1000000;
        {
            // recreate table again and check that it is ready for testing
            Connection connection = DriverManager.getConnection(TestUtils.URL);
            connection.setAutoCommit(false);
            final Statement statement = connection.createStatement();
            TestUtils.schemas(statement, true);
            statement.execute("CREATE TABLE mylib.one (X INTEGER, Y VARCHAR, Z DOUBLE PRECISION)");

            PreparedStatement p1 = connection.prepareStatement("INSERT INTO mylib.one (X,Y,Z) VALUES (?,?,?)");
            for (int i = 1; i <= N; i++) {
                p1.setInt(1, i);
                p1.setString(2, "ABC" + i);
                p1.setDouble(3, 1. / i);
                p1.executeUpdate();
            }

            statement.close();
            connection.close();
        }
        for (int c = 1; c < 10; c++) {
            // actually read the indices and check
            Connection connection = DriverManager.getConnection(TestUtils.URL);
            Statement statement = connection.createStatement();
            TestUtils.schemas(statement, false);
            ResultSet rs = statement.executeQuery("SELECT * FROM MYLIB.ONE");
            int i = 0;
            while (rs.next()) {
                i++;
            }
            Assert.assertEquals(i, N);
            statement.close();
            connection.close();
        }
    }

    /**
     * check the tables can be flushed from memory on commit and will be picked later on demand by SELECT query
     */

    @Test
    public void doubleInsertWithAutoCommit() throws Exception {
        // recreate table again and check that it is ready for testing
        Connection connection = DriverManager.getConnection(TestUtils.URL);
        final Statement statement = connection.createStatement();
        TestUtils.schemas(statement, false);
        statement.execute("CREATE TABLE mylib.one (X INTEGER)");
        statement.execute("INSERT INTO mylib.one (X) VALUES (1)");
        try {
            statement.execute("INSERT INTO mylib.one (X) VALUES (1)");
        } catch (SQLException e) {
            Assert.assertEquals(e.getErrorCode(), ErrorCode.TABLE_IS_READ_ONLY);
            Assert.assertEquals(Utils.truncate(e), "The table \"ONE\" is opened in READ ONLY mode.");
        }
        statement.close();
        connection.close();
    }

    /**
     * check the tables can be flushed from memory on commit and will be picked later on demand by SELECT query
     */

    @Test
    public void doubleInsertWithExplicitCommit() throws Exception {
        int N = 100;
        {
            // recreate table again and check that it is ready for testing
            Connection connection = DriverManager.getConnection(TestUtils.URL);
            connection.setAutoCommit(false);
            final Statement statement = connection.createStatement();
            TestUtils.schemas(statement, false);
            statement.execute("CREATE TABLE mylib.one (X INTEGER)");
            statement.execute("INSERT INTO mylib.one (X) VALUES (1)");
            statement.execute("INSERT INTO mylib.one (X) VALUES (1)");
            connection.commit();
            ResultSet rs = statement.executeQuery("SELECT * FROM mylib.one");
            Assert.assertTrue(rs.next());
            Assert.assertTrue(rs.next());
            Assert.assertFalse(rs.next());
            statement.close();
            connection.close();
        }
    }

    @Test
    public void insertWithSelectWithoutCommit() throws Exception {
        Connection connection = DriverManager.getConnection(TestUtils.URL);
        connection.setAutoCommit(false);
        Statement statement = connection.createStatement();
        TestUtils.schemas(statement, false);
        statement.execute("CREATE TABLE mylib.one (X INTEGER)");
        statement.execute("INSERT INTO mylib.one (X) VALUES (1)");
        try {
            statement.executeQuery("SELECT * FROM mylib.one");
        } catch (SQLException e) {
            Assert.assertEquals(e.getErrorCode(), ErrorCode.TABLE_IS_INSERT_ONLY);
            Assert.assertEquals(Utils.truncate(e), "Can not execute query on table \"ONE\" with uncommitted data. Execute COMMIT first.");
        }
        statement.close();
        connection.close();
    }

    @Test
    public void batchUpdate() throws Exception {
        Connection connection = DriverManager.getConnection(TestUtils.URL);
        connection.setAutoCommit(false);
        Statement statement = connection.createStatement();
        TestUtils.schemas(statement, false);
        statement.execute("CREATE TABLE mylib.one (X VARCHAR(20))");

        PreparedStatement p = connection.prepareStatement("INSERT INTO MYLIB.ONE VALUES (?)");
        p.setInt(1, 10);
        p.executeUpdate();
        //statement.execute("DROP TABLE mylib.one");
        p.setInt(1, 20);
        try {
            p.executeUpdate();
        } catch (SQLException e) {
            Assert.assertEquals(e.getErrorCode(), ErrorCode.TABLE_IS_READ_ONLY);
            Assert.assertEquals(Utils.truncate(e), "The table \"ONE\" is opened in READ ONLY mode.");
        }

        statement.close();
        connection.close();
    }

    @Test
    public void deleteWithViewCascade() throws Exception {
        Connection connection = DriverManager.getConnection(TestUtils.URL);
        Statement statement = connection.createStatement();
        TestUtils.schemas(statement, true);
        connection.setAutoCommit(false);
        statement.execute("CREATE TABLE mylib.one (x INT)");
        statement.execute("CREATE VIEW mylib.two AS SELECT * FROM mylib.one");
        statement.execute("DROP TABLE mylib.one CASCADE");
        try {
            statement.executeQuery("SELECT * FROM  mylib.two");
            Assert.fail();
        } catch (SQLException e) {
            Assert.assertEquals(e.getErrorCode(), ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1);
        }
        statement.close();
        connection.close();
    }

    @Test
    public void alterTableExternal() throws Exception {
        Connection connection = DriverManager.getConnection(TestUtils.URL);
        Statement statement = connection.createStatement();
        TestUtils.schemas(statement, true);
        statement.execute("CREATE TABLE MYLIB.ONE (x INT, Y INT, z INT)");
        statement.execute("ALTER TABLE MYLIB.ONE DROP x");
        statement.close();
        connection.close();
    }

    @Test
    public void alterTableH2() throws Exception {
        Connection connection = DriverManager.getConnection(TestUtils.URL);
        Statement statement = connection.createStatement();
        TestUtils.schemas(statement, true);
        statement.execute("CREATE TABLE ONE (x INT, Y INT, z INT)");
        statement.execute("ALTER TABLE ONE DROP x");
        statement.close();
        connection.close();
    }

    @Test
    public void alterTableRename() throws Exception {
        {
            Connection connection = DriverManager.getConnection(TestUtils.URL);
            Statement statement = connection.createStatement();
            TestUtils.schemas(statement, true);
            statement.execute("CREATE TABLE MYLIB.ONE (x INT, Y INT, z INT)");
            connection.commit();
            statement.execute("ALTER TABLE MYLIB.ONE RENAME TO MYLIB.TWO");
            statement.execute("SELECT count(*) FROM MYLIB.TWO");
            connection.commit();
            statement.execute("SELECT count(*) FROM MYLIB.TWO");
            statement.close();
            connection.close();
        }
        {
            Connection connection = DriverManager.getConnection(TestUtils.URL);
            Statement statement = connection.createStatement();
            TestUtils.schemas(statement, true);
            statement.execute("SELECT count(*) FROM MYLIB.TWO");
            statement.close();
            connection.close();
        }
    }

    @Test(enabled = false)
    public void alterTableAddColumn() throws Exception {
        {
            Connection connection = DriverManager.getConnection(TestUtils.URL);
            Statement statement = connection.createStatement();
            TestUtils.schemas(statement, true);
            System.out.println("======================================================");
            statement.execute("CREATE TABLE MYLIB.ONE (x INT, Y INT)");
            connection.commit();
            statement.execute("ALTER TABLE MYLIB.ONE ADD C INT");
            statement.execute("SELECT count(*) FROM MYLIB.TWO");
            connection.commit();
            statement.execute("SELECT count(*) FROM MYLIB.TWO");
            statement.close();
            connection.close();
        }
    }

    @Test
    public void alterTableAddColumnNative() throws Exception {
        {
            Connection connection = DriverManager.getConnection("jdbc:h2:mem:");
            Statement statement = connection.createStatement();
            statement.execute("CREATE SCHEMA MYLIB");
            System.out.println("======================================================");
            statement.execute("CREATE TABLE MYLIB.ONE (x INT, Y INT)");
            connection.commit();
            statement.execute("ALTER TABLE MYLIB.ONE ADD C INT");
            statement.execute("SELECT count(c) FROM MYLIB.ONE");
            statement.close();
            connection.close();
        }
    }

    @Test
    public void alterTableRenameIndex() throws Exception {
        {
            Connection connection = DriverManager.getConnection(TestUtils.URL);
            Statement statement = connection.createStatement();
            TestUtils.schemas(statement, true);
            statement.execute("CREATE TABLE MYLIB.ONE (x INT, Y INT, z INT)");
            statement.execute("CREATE INDEX XYZ ON MYLIB.ONE (x)");
            statement.execute("CREATE INDEX RST ON MYLIB.ONE (y)");
            connection.commit();
            statement.execute("ALTER TABLE MYLIB.ONE RENAME TO MYLIB.TWO");
            statement.execute("SELECT count(*) FROM MYLIB.TWO");
            statement.execute("DROP INDEX MYLIB.TWO$RST");
            connection.commit();
            statement.execute("SELECT count(*) FROM MYLIB.TWO");
            statement.close();
            connection.close();
        }
        {
            Connection connection = DriverManager.getConnection(TestUtils.URL);
            Statement statement = connection.createStatement();
            TestUtils.schemas(statement, true);
            statement.execute("SELECT count(*) FROM MYLIB.TWO");
            statement.execute("DROP INDEX MYLIB.TWO$XYZ");
            statement.close();
            connection.close();
        }
    }

    @Test
    public void createTwiceTheSameTable() throws Exception {
        Connection connection = DriverManager.getConnection(TestUtils.URL);
        java.sql.Statement statement = connection.createStatement();
        TestUtils.schemas(statement, false);
        statement.execute("CREATE TABLE mylib.one (x INTEGER)");
        try {
            statement.execute("CREATE TABLE mylib.one (x INTEGER)");
        } catch (SQLException e) {
            Assert.assertEquals(e.getErrorCode(), ErrorCode.TABLE_OR_VIEW_ALREADY_EXISTS_1);
            Assert.assertEquals(e.getMessage(),
                    "Table \"ONE\" already exists; SQL statement:\n" +
                            "CREATE TABLE mylib.one (x INTEGER) [42101-196]");
        }

        statement.close();
        connection.close();
    }

}