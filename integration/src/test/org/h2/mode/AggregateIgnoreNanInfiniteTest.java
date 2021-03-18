package org.h2.mode;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Mode.aggregateIgnoreNanInfinite
 */
public class AggregateIgnoreNanInfiniteTest {

    @BeforeClass
    protected void setUp() throws Exception {
        Class.forName("org.h2.Driver");
    }

    @Test
    public void sumWithMissing() throws Exception {
        try (Connection h2 = DriverManager.getConnection("jdbc:h2:mem:;mode=Carolina")) {
            try (Statement statement = h2.createStatement()) {
                statement.execute("DROP TABLE FOO IF EXISTS");
                statement.execute("CREATE TABLE FOO (X DOUBLE PRECISION)");
                statement.execute("insert into FOO values(0.0)");
                statement.execute("insert into FOO values(1.0)");
                statement.execute("insert into FOO values(2.718281828459045235)");
                try (ResultSet rs = statement.executeQuery("select sum(log(X)) from FOO")) {
                    rs.next();
                    Assert.assertEquals(rs.getDouble(1), 1.0);
                }
                try (ResultSet rs = statement.executeQuery("select min(log(X)) from FOO")) {
                    rs.next();
                    Assert.assertEquals(rs.getDouble(1), Double.NEGATIVE_INFINITY);
                }
                try (ResultSet rs = statement.executeQuery("select max(log(X)) from FOO")) {
                    rs.next();
                    Assert.assertEquals(rs.getDouble(1), 1.0);
                }
            }
        }
    }
}
