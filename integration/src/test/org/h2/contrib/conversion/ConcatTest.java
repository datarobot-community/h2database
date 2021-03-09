package org.h2.contrib.conversion;

import org.h2.jdbc.JdbcConnection;
import org.h2.store.fs.FileUtils;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class ConcatTest {
// ------------------------------ FIELDS ------------------------------

    private JdbcConnection h2;

// -------------------------- TEST METHODS --------------------------

    @BeforeMethod
    protected void setUp() throws Exception {
        Class.forName("org.h2.Driver");
        FileUtils.deleteRecursive("./target/db", false);
        h2 = (JdbcConnection) DriverManager.getConnection("jdbc:h2:./target/db/concat;MODE=Carolina");

        try (Statement stat = h2.createStatement()) {
            h2.setUserDefinedConversion(new ConversionTest.DoubleToDot());
            stat.execute("CREATE TABLE one ( abc double, def double)");
            stat.execute("INSERT INTO one(abc,def) VALUES(10.,20.)");
        }
    }

    @AfterMethod
    protected void tearDown() throws Exception {
        h2.close();
    }

// -------------------------- OTHER METHODS --------------------------

    /**
     * No user defined conversion for concat operation
     */
    @Test
    public void dot() throws Exception {
        try (Statement stat = h2.createStatement()) {
            ResultSet rs = stat.executeQuery("SELECT concat(abc,'/',def) cat FROM one");
            int prec = rs.getMetaData().getPrecision(1);
            rs.next();
            Assert.assertEquals(rs.getString("cat"), "10/20");
            Assert.assertEquals(prec, 200);
        }
    }
}
