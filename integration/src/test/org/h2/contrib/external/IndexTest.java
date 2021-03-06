package org.h2.contrib.external;

import org.h2.store.fs.FileUtils;
import org.testng.Assert;
import org.testng.annotations.*;

import java.io.File;
import java.sql.*;

public class IndexTest {
// ------------------------------ FIELDS ------------------------------

    public static final String DIR = "target/index";
    private int size;
    private Connection h2;

// -------------------------- TEST METHODS --------------------------

    @BeforeClass
    protected void setUp() throws Exception {
        FileUtils.deleteRecursive(DIR, false);
        new File(DIR).mkdirs();
        
        Class.forName("org.h2.Driver");
        h2 = DriverManager.getConnection("jdbc:h2:mem:");

        size = 100;
        init();
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
}
