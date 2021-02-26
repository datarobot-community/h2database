package com.dullesopen.h2test.syntax;

import org.h2.tools.SimpleResultSet;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.sql.*;

public class LazyTest {
// ------------------------------ FIELDS ------------------------------

    private Connection conn;

// -------------------------- TEST METHODS --------------------------

    @BeforeMethod
    protected void setUp() throws Exception {
        Class.forName("org.h2.Driver");
        conn = DriverManager.getConnection("jdbc:h2:mem:;LAZY_QUERY_EXECUTION=1");
    }

    @AfterMethod
    protected void tearDown() throws Exception {
        conn.close();
    }

    // -------------------------- OTHER METHODS --------------------------
    @Test(enabled = false)
    public void conversionError() throws Exception {
        Statement statement = conn.createStatement();
        statement.execute("CREATE TABLE B(S VARCHAR(10))");
        statement.execute("INSERT INTO B VALUES ('ABC')");

        String s1 = "SELECT S+1 from B";
        try {
            ResultSet rs = statement.executeQuery(s1);
            rs.next();
            Assert.fail();
        } catch (SQLException e) {
            System.out.println("e = " + e.getMessage());
            Assert.assertTrue(e.getMessage().contains("SELECT S+1 from B"));
        }
    }

    @Test
    public void subQueryCache() throws Exception {
        Statement stat = conn.createStatement();
        stat.execute("create alias \"ONCE\" DETERMINISTIC for \"" + this.getClass().getName() + ".once()\"");
        String s1 = "select 123 from SYSTEM_RANGE(1, 10) where x in (select id from once())";
        try (ResultSet rs = stat.executeQuery(s1)) {
            int n = 0;
            while (rs.next()) {
                n++;
            }
            System.out.println("n = " + n);
        }
    }

    static int count = 0;

    @SuppressWarnings("unused")
    public static SimpleResultSet once() {
        // called twice within parser and then once during execution
        if (count > 3)
            throw new IllegalArgumentException("called twice, query is not cached");
        count++;
        System.out.println("count = " + count);
        SimpleResultSet rs = new SimpleResultSet();
        rs.addColumn("ID", Types.INTEGER, 10, 0);
        for (int i = 1; i <= 10; i++)
            rs.addRow(i);
        return rs;
    }

    @Test
    public void subquery() throws Exception {
        Statement stat = conn.createStatement();
        final int ROWS = 100000;
        stat.execute("CREATE TABLE ONE(X INTEGER , Y INTEGER )");
        PreparedStatement prep = conn.prepareStatement("insert into one values(?,?)");
        for (int row = 0; row < ROWS; row++) {
            prep.setInt(1, row / 100);
            prep.setInt(2, row);
            prep.execute();
        }
        String s1 = "SELECT COUNT (*) from one where x in ( select y from one where y < 100)";
        ResultSet rs = stat.executeQuery(s1);
        rs.next();
        Assert.assertEquals(rs.getInt(1), 10000);
    }

    //@Test
    public void join() throws Exception {
        try (Statement stat = conn.createStatement()) {
            final int ROWS = 100000;
            stat.execute("CREATE TABLE ONE(X INTEGER , Y INTEGER )");
            stat.execute("CREATE TABLE TWO(S INTEGER , R INTEGER )");
            try (PreparedStatement prep = conn.prepareStatement("insert into one values(?,?)")) {
                for (int row = 0; row < ROWS; row++) {
                    prep.setInt(1, row / 100);
                    prep.setInt(2, row);
                    prep.execute();
                }
            }
            try (PreparedStatement prep = conn.prepareStatement("insert into two values(?,?)")) {
                for (int row = 0; row < ROWS; row++) {
                    prep.setInt(1, row / 100);
                    prep.setInt(2, row);
                    prep.execute();
                }
            }
            stat.execute("CREATE INDEX ON TWO(R)");

            String s1 = "SELECT COUNT (*) from " +
                    "( select x , y from one where x >= 0 ) A ," +
                    "( select s , r from two where s >= 0 ) B where A.Y=B.R";
            String s2 = "SELECT COUNT (*) from " +
                    "one A ," +
                    "two B where A.Y=B.R";

            long t0 = System.currentTimeMillis();
            try (ResultSet rs = stat.executeQuery("EXPLAIN PLAN FOR " + s1)) {
                while (rs.next()) {
                    String plan = rs.getString(1);
                    System.out.println(plan);
                }
            }
            try (ResultSet rs = stat.executeQuery(s1)) {
                rs.next();
                Assert.assertEquals(rs.getInt(1), ROWS);
            }
            long t1 = System.currentTimeMillis();
            System.out.println("t1 = " + (t1 - t0) / 1000.);

        }
    }

}



