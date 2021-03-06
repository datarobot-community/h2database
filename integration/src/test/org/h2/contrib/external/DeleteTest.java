package org.h2.contrib.external;

import org.h2.store.fs.FileUtils;
import org.testng.Assert;
import org.testng.annotations.*;

import java.io.File;
import java.sql.*;

public class DeleteTest {
// ------------------------------ FIELDS ------------------------------

    public static final String DIR = "target/delete";
    private final int SIZE=100;
    private Connection h2;

// -------------------------- TEST METHODS --------------------------

    @BeforeClass
    protected void setUp() throws Exception {
        Class.forName("org.h2.Driver");
        h2 = DriverManager.getConnection("jdbc:h2:mem:");
        FileUtils.deleteRecursive(DIR,false);
        new File(DIR).mkdir();

        init();
    }

    void init() throws SQLException {
        Statement stat;
        stat = h2.createStatement();
        stat.execute(Init.schema(DIR));

        stat.execute("CREATE TABLE S.T (ID INT , VALUE REAL)");
        stat.execute("CREATE INDEX S.I ON S.T (VALUE)");

        PreparedStatement prep = h2.prepareStatement("INSERT INTO S.T VALUES(?, ?)");
        for (int i = 0; i < SIZE; i++) {
            prep.setInt(1, i);
            prep.setDouble(2, -i * i);
            prep.executeUpdate();
        }
        prep.close();
    }

    @AfterClass
    protected void afterClass() throws Exception {
        h2.close();
        new File("target/delete.db").delete();
    }

// -------------------------- OTHER METHODS --------------------------

    @Test
    public void verify() throws SQLException {
        Statement stat = h2.createStatement();
        stat.execute("DELETE FROM S.T WHERE VALUE=-16");
        stat.execute("UPDATE S.T SET VALUE=9 WHERE VALUE=-9");
        ResultSet rs = stat.executeQuery("SELECT ID FROM S.T WHERE VALUE=9");
        rs.next();
        Assert.assertEquals(rs.getInt(1), 3);
        stat.close();
    }
}
