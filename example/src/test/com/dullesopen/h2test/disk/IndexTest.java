package com.dullesopen.h2test.disk;


import org.h2.store.fs.FileUtils;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.sql.*;


public class IndexTest {

    @BeforeMethod
    public void setUpConnection() throws Exception {
        TestUtils.setUpConnection();
    }

    @AfterMethod
    public void tearDownConnection() throws Exception {
        TestUtils.tearDownConnection();
    }

// -------------------------- OTHER METHODS --------------------------

    @Test
    public void bug() throws Exception {
        int N = 10000;

        {
            Connection connection = DriverManager.getConnection(TestUtils.URL);
            TestUtils.create(N, connection, true);
            connection.close();
        }
        java.sql.Connection connection = DriverManager.getConnection(TestUtils.URL);
        java.sql.Statement statement = connection.createStatement();
        TestUtils.schemas(statement, true);
        java.sql.DatabaseMetaData metaData = connection.getMetaData();
        ResultSet tables = metaData.getTables(null, null, null, new String[]{"TABLE"});
        int t = 0;
        while (tables.next()) {
            t++;
        }
        Assert.assertEquals(t, 2);
        connection.setAutoCommit(true);

        {
            statement.setMaxRows(100);
            statement.setFetchSize(100);
            ResultSet rs = statement.executeQuery("select t1.x from mylib.one t1 \n" +
                    "join mylib.two t2 on t1.x = t2.y;");
            int n = 0;
            while (rs.next()) {
                n++;
            }
            rs.close();
        }
        statement.close();
        connection.close();
    }

    @Test
    public void bug1() throws Exception {
        int N = 10000;
        {
            Connection connection = DriverManager.getConnection(TestUtils.URL);
            connection.setAutoCommit(false);
            final Statement statement = connection.createStatement();
            TestUtils.schemas(statement, true);
            statement.execute("CREATE TABLE mylib.VEHICLE (CASENUM INTEGER)");
            java.sql.PreparedStatement p1 = connection.prepareStatement("INSERT INTO mylib.VEHICLE (CASENUM) VALUES (?)");
            for (int i = 0; i < N; i++) {
                p1.setInt(1, i);
                p1.executeUpdate();
            }
            connection.commit();
            statement.close();
            connection.close();
        }

        {
            java.sql.Connection connection = DriverManager.getConnection(TestUtils.URL);
            java.sql.Statement statement = connection.createStatement();
            TestUtils.schemas(statement, true);
            statement.execute("SET SCHEMA mylib");
            statement.execute("DROP INDEX VEHICLE$I IF EXISTS");
            statement.execute("CREATE INDEX I ON VEHICLE (CASENUM)");
            connection.close();
        }
        {
            java.sql.Connection connection = DriverManager.getConnection(TestUtils.URL);
            java.sql.Statement statement = connection.createStatement();
            TestUtils.schemas(statement, true);
            java.sql.DatabaseMetaData metaData = connection.getMetaData();

            {
                ResultSet rs = metaData.getTables(null, null, null, new String[]{"TABLE"});
                rs.next();
                Assert.assertEquals(rs.getString("TABLE_NAME"), "VEHICLE");
                Assert.assertFalse(rs.next());
                rs.close();
            }
            connection.setAutoCommit(true);

            if (false) {
                statement.setMaxRows(100);
                statement.setFetchSize(100);
            }
            ResultSet rs = statement.executeQuery("select t1.CASENUM from mylib.VEHICLE t1 \n" +
                    "join mylib.VEHICLE t2 on t1.CASENUM = t2.CASENUM;");
            int n = 0;
            while (rs.next()) {
                n++;
            }
            Assert.assertEquals(n, N);
            rs.close();

            statement.close();
            connection.close();
        }
        FileUtils.deleteRecursive(TestUtils.DIR, false);
    }

    @Test
    public void drop() throws Exception {
        {
            Connection connection = DriverManager.getConnection(TestUtils.URL);
            connection.setAutoCommit(false);
            final Statement statement = connection.createStatement();
            TestUtils.schemas(statement, true);
            statement.execute("CREATE TABLE mylib.one (X INTEGER)");
            connection.commit();
            statement.execute("CREATE INDEX I ON MYLIB.ONE(X)");
            statement.close();
            connection.close();
        }
        {
            Connection connection = DriverManager.getConnection(TestUtils.URL);
            connection.setAutoCommit(false);
            final Statement statement = connection.createStatement();
            TestUtils.schemas(statement, true);
            statement.execute("DROP INDEX MYLIB.ONE$I");
            connection.close();
        }
        {
            Connection connection = DriverManager.getConnection(TestUtils.URL);
            final Statement statement = connection.createStatement();
            TestUtils.schemas(statement, true);
            statement.execute("CREATE INDEX I ON MYLIB.ONE(X)");
            statement.execute("DROP INDEX MYLIB.ONE$I");
            statement.execute("CREATE INDEX I ON MYLIB.ONE(X)");
            statement.execute("DROP INDEX MYLIB.ONE$I");
            connection.close();
        }
    }

    @Test
    public void dropTableWithIndex() throws Exception {
        {
            Connection connection = DriverManager.getConnection(TestUtils.URL);
            connection.setAutoCommit(false);
            final Statement statement = connection.createStatement();
            TestUtils.schemas(statement, true);
            statement.execute("CREATE TABLE mylib.VEHICLE (CASENUM INTEGER)");
            statement.execute("INSERT INTO mylib.VEHICLE (CASENUM) VALUES (1)");
            connection.commit();
            statement.close();
            connection.close();
        }
        {
            java.sql.Connection connection = DriverManager.getConnection(TestUtils.URL);
            java.sql.Statement statement = connection.createStatement();
            TestUtils.schemas(statement, true);
            statement.execute("SET SCHEMA mylib");
            Assert.assertFalse(new File(TestUtils.DIR + "/" + "vehicle.table.index").exists());
            statement.execute("CREATE INDEX I ON VEHICLE (CASENUM)");
            Assert.assertTrue(new File(TestUtils.DIR + "/" + "vehicle.table").exists());
            Assert.assertTrue(new File(TestUtils.DIR + "/" + "vehicle.table.index").exists());
            statement.execute("DROP TABLE MYLIB.VEHICLE IF EXISTS CASCADE");
            Assert.assertFalse(new File(TestUtils.DIR + "/" + "vehicle.table").exists());
            Assert.assertFalse(new File(TestUtils.DIR + "/" + "vehicle.table.index").exists());

            statement.close();
            connection.close();
        }
    }

    @Test
    public void dropTableWithIndexAfterClose() throws Exception {
        {
            Connection connection = DriverManager.getConnection(TestUtils.URL);
            connection.setAutoCommit(false);
            final Statement statement = connection.createStatement();
            TestUtils.schemas(statement, true);
            statement.execute("CREATE TABLE mylib.VEHICLE (CASENUM INTEGER)");
            statement.execute("INSERT INTO mylib.VEHICLE (CASENUM) VALUES (1)");
            connection.commit();
            statement.execute("SET SCHEMA mylib");
            Assert.assertFalse(new File(TestUtils.DIR + "/" + "vehicle.table.index").exists());
            statement.execute("CREATE INDEX I ON VEHICLE (CASENUM)");
            Assert.assertTrue(new File(TestUtils.DIR + "/" + "vehicle.table").exists());
            Assert.assertTrue(new File(TestUtils.DIR + "/" + "vehicle.table.index").exists());
            statement.close();
            connection.close();
        }
        {
            java.sql.Connection connection = DriverManager.getConnection(TestUtils.URL);
            java.sql.Statement statement = connection.createStatement();
            TestUtils.schemas(statement, true);
            statement.execute("DROP TABLE MYLIB.VEHICLE IF EXISTS CASCADE");
            Assert.assertFalse(new File(TestUtils.DIR + "/" + "vehicle.table").exists());
            Assert.assertFalse(new File(TestUtils.DIR + "/" + "vehicle.table.index").exists());

            statement.close();
            connection.close();
        }
    }

    @Test
    public void dropTableWithIndexAfterCommit() throws Exception {
        Connection connection = DriverManager.getConnection(TestUtils.URL);
        final Statement statement = connection.createStatement();
        TestUtils.schemas(statement, true);
        statement.execute("CREATE TABLE MYLIB.ONE(ZIP CHAR(7), ZIPNOTE CHAR(1000))");
        statement.execute("CREATE INDEX ZIP ON MYLIB.ONE(ZIP)");
        //Assert.assertTrue(new File(BLUE + "/" + "a.car3indx").exists());
        statement.execute("commit");
        statement.execute("DROP TABLE MYLIB.ONE");
        //Assert.assertFalse(new File(BLUE + "/" + "a.car3indx").exists());
    }

    @Test
    public void reuse() throws Exception {
        int N = 10000;
        {
            Connection connection = DriverManager.getConnection(TestUtils.URL);
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
            TestUtils.schemas(statement, true);
            merge(N, statement);
            statement.close();
            connection.close();
        }
    }

    private void merge(int n, Statement statement) throws SQLException {
        ResultSet rs = statement.executeQuery("SELECT count(*) FROM mylib.one, mylib.two where one.X=two.y");
        rs.next();
        int count = rs.getInt(1);
        Assert.assertEquals(n - 1, count);
        rs.close();
    }

}
