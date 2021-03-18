package org.h2.contrib.external;

import org.h2.store.fs.FileUtils;
import org.testng.Assert;
import org.testng.annotations.*;

import java.io.File;
import java.sql.*;

public class IndexTest {

    public static final String DIR = "target/index";
    private int size;
    private Connection h2;

    @BeforeClass
    protected void setUp() throws Exception {
        FileUtils.deleteRecursive(DIR, false);
        new File(DIR).mkdirs();

        Class.forName("org.h2.Driver");
        h2 = DriverManager.getConnection("jdbc:h2:mem:");

        size = 100;
        init();
    }

    @BeforeMethod
    public void setUpConnection() throws Exception {
        TestUtils.setUpConnection();
    }

    @AfterMethod
    public void tearDownConnection() throws Exception {
        TestUtils.tearDownConnection();
    }

    void init() throws SQLException {
        Statement stat;
        stat = h2.createStatement();
        stat.execute(Init.schema(DIR));

        stat.execute("CREATE TABLE S.T (ID INT , VALUE REAL)");
        stat.execute("CREATE INDEX S.I ON S.T (VALUE)");

        PreparedStatement prep = h2.prepareStatement("INSERT INTO S.T VALUES(?, ?)");
        for (int i = 0; i < size; i++) {
            prep.setInt(1, i);
            prep.setDouble(2, -i * i);
            prep.executeUpdate();
        }
        prep.close();
    }

    @AfterClass
    protected void tearDown() throws Exception {
        h2.close();
        FileUtils.deleteRecursive(DIR, false);
        Assert.assertFalse(new File(DIR).exists());
    }

// -------------------------- OTHER METHODS --------------------------

    @Test
    public void condition() throws SQLException {
        Statement stat = h2.createStatement();
        ResultSet rs = stat.executeQuery(
                "SELECT COUNT(*) FROM S.T WHERE VALUE < -100.001 AND VALUE > -400.001");
        rs.next();
        Assert.assertEquals(rs.getInt(1), 10);
    }

    @Test
    public void count() throws SQLException {
        Statement stat = h2.createStatement();
        ResultSet rs = stat.executeQuery("SELECT COUNT(*) FROM S.T");
        rs.next();
        Assert.assertEquals(rs.getInt(1), size);
    }

    @Test
    public void find() throws SQLException {
        Statement stat = h2.createStatement();
        ResultSet rs = stat.executeQuery("select id from s.t where value=-1600");
        rs.next();
        Assert.assertEquals(rs.getInt(1), 40);
    }

    @Test
    public void recreate() throws Exception {
        Statement statement = h2.createStatement();
        statement.execute("DROP INDEX S.T$I IF EXISTS");
        statement.execute("CREATE INDEX S.I ON S.T (VALUE)");
        statement.close();
    }

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
        try (Connection connection = DriverManager.getConnection(TestUtils.URL)) {
            TestUtils.create(N, connection, true);
        }

        // verify that all files can be safely removed and nothing is opened
        FileUtils.deleteRecursive(TestUtils.DIR, false);
        File dir = new File(TestUtils.DIR);
        Assert.assertFalse(dir.exists());
        dir.mkdirs();

        // recreate table again and check that it is ready for testing
        try (Connection connection = DriverManager.getConnection(TestUtils.URL)) {
            TestUtils.create(N, connection, true);
        }
        // actually read the indices and check
        try (Connection connection = DriverManager.getConnection(TestUtils.URL)) {
            try (Statement statement = connection.createStatement()) {
                TestUtils.schemas(statement, true);
                merge(N, statement);
            }
        }
    }

    private void merge(int n, Statement statement) throws SQLException {
        String sql = "SELECT count(*) FROM mylib.one, mylib.two where one.X=two.y";
        try (ResultSet rs = statement.executeQuery(sql)) {
            rs.next();
            int count = rs.getInt(1);
            Assert.assertEquals(n - 1, count);
        }
    }


}
