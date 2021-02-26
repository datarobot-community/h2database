package com.dullesopen.h2test.table;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.sql.*;

@Test
public class JoinTest {
// ------------------------------ FIELDS ------------------------------

    public static final String FILE = "target/join.db";
    private Connection h2;


// -------------------------- TEST METHODS --------------------------

    @BeforeClass
    protected void setUp() throws Exception {
        Class.forName("org.h2.Driver");
        new File(FILE).delete();

        Class.forName("org.h2.Driver");
        h2 = DriverManager.getConnection("jdbc:h2:mem:");

    }

    @AfterClass
    protected void tearDown() throws Exception {
        h2.close();
    }

// -------------------------- OTHER METHODS --------------------------

    @Test
    public void join() throws Exception {
        Statement statement = h2.createStatement();

        // in H2 names are valid across schema

        run(statement, 1000);
        run(statement, 1000);
        run(statement, 10000);
        if (false) {
            run(statement, 100000);
            run(statement, 200000);
            run(statement, 400000);
            run(statement, 1500000);
        }
        statement.close();
    }

    private void run(Statement statement, int size) throws SQLException {
        statement.execute("drop table test if exists");
        statement.execute("drop table two if exists");
        statement.execute("create table test (" +
                "id int, " +
                "manager int, " +
                "name varchar(10), " +
                "dept int)");
        PreparedStatement ps = h2.prepareStatement("INSERT INTO TEST VALUES ( ?,?,?,?)");
        for (int i = 0; i < size; i++) {
            ps.setInt(1, i);
            if (i > 10)
                ps.setInt(2, i / 10);
            else
                ps.setNull(2, Types.INTEGER);
            ps.setString(3, "N" + i);
            ps.setInt(4, i / 100);
            ps.execute();
        }
        statement.execute("create index x on  test(id)");
        statement.execute("create index y on  test(manager)");
        statement.execute("create index z on  test(dept)");


        Timer timer = new Timer();
        statement.execute("" +
                "create table two as select t.*, r.name as boss " +
                "from test t left outer join test r " +
                "on t.manager = r.id and t.dept = t.dept " +
                "order by boss");
        ResultSet rs = statement.executeQuery("select count (*) from two");
        rs.next();
        Assert.assertEquals(size, rs.getInt(1));
        timer.report("merge with size=" + size + ": ");
    }

}
