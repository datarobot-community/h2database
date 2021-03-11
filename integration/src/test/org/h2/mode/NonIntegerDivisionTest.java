package org.h2.mode;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Mode.nonIntegerDivision
 */

public class NonIntegerDivisionTest {

    @BeforeClass
    protected void setUp() throws Exception {
        Class.forName("org.h2.Driver");
    }

    @Test
    public void noninteger() throws Exception {

        try (Connection h2 = DriverManager.getConnection("jdbc:h2:mem:;mode=Carolina")) {
            try (Statement sa = h2.createStatement()) {

                sa.execute("CREATE TABLE TAB(ONE INTEGER, TWO INTEGER)");
                sa.execute("INSERT INTO TAB VALUES ( 123, 456)");
                sa.execute("INSERT INTO TAB VALUES ( 222, 333)");
                try (ResultSet rs = sa.executeQuery("select one/two from tab")) {
                    rs.next();
                    double d = rs.getDouble(1);
                    org.testng.Assert.assertEquals(d, 0.26973684210526316);
                }
                try (ResultSet rs = sa.executeQuery("select 1/count(*) from tab")) {
                    rs.next();
                    double d = rs.getDouble(1);
                    org.testng.Assert.assertEquals(d, 0.5);
                }
                try (ResultSet rs = sa.executeQuery("select 1/0 from tab")) {
                    rs.next();
                    double d = rs.getDouble(1);
                    org.testng.Assert.assertEquals(d, Double.POSITIVE_INFINITY);
                }
            }
        }
    }
}
