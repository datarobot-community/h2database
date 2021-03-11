package org.h2.mode;

import org.h2.contrib.conversion.ConversionTest;
import org.h2.jdbc.JdbcConnection;
import org.h2.store.fs.FileUtils;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Mode.concatReturnSize
 */

public class ConcatReturnSizeTest {
// ------------------------------ FIELDS ------------------------------

    @BeforeClass
    protected void setUp() throws Exception {
        Class.forName("org.h2.Driver");
    }

    /**
     * No user defined conversion for concat operation
     */
    @Test
    public void dot() throws Exception {
        try (JdbcConnection h2 = (JdbcConnection) DriverManager.getConnection("jdbc:h2:mem:;mode=Carolina")) {
            try (Statement stat = h2.createStatement()) {
                h2.setUdfArgumentConverter(new ConversionTest.DoubleToDot());
                stat.execute("CREATE TABLE one ( abc double, def double)");
                stat.execute("INSERT INTO one(abc,def) VALUES(10.,20.)");
            }
            try (Statement stat = h2.createStatement()) {
                ResultSet rs = stat.executeQuery("SELECT concat(abc,'/',def) cat FROM one");
                int prec = rs.getMetaData().getPrecision(1);
                rs.next();
                Assert.assertEquals(rs.getString("cat"), "10/20");
                Assert.assertEquals(prec, 200);
            }
        }
    }}
