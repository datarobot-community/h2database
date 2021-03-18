package org.h2.mode;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Mode.allowZeroDivide
 */

public class AllowZeroDivideTest {

    @BeforeClass
    protected void setUp() throws Exception {
        Class.forName("org.h2.Driver");
    }

    @Test
    public void zero() throws Exception {

        try (Connection h2 = DriverManager.getConnection("jdbc:h2:mem:;mode=Carolina")) {
            try (Statement sa = h2.createStatement()) {

                sa.execute("CREATE TABLE TAB(ONE DOUBLE , TWO DOUBLE)");
                sa.execute("INSERT INTO TAB VALUES ( 0, 0)");
                try (ResultSet rs = sa.executeQuery("select one/two from tab")) {
                    rs.next();
                    double d = rs.getDouble(1);
                    org.testng.Assert.assertEquals(d, 0.0);
                }

                try (ResultSet rs = sa.executeQuery("select one/two as ratio, SUM(CASE WHEN calculated ratio > 1 THEN 1 ELSE 0 END) from tab")) {
                    rs.next();
                    double d = rs.getDouble(2);
                    org.testng.Assert.assertEquals(d, 0.0);
                }
            }
        }
    }

}
