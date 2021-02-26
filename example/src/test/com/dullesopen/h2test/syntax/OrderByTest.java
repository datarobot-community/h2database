package com.dullesopen.h2test.syntax;


import com.dullesopen.h2test.TestConfig;
import com.dullesopen.h2test.Utils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.sql.*;

public class OrderByTest {
// ------------------------------ FIELDS ------------------------------


// -------------------------- TEST METHODS --------------------------

    @BeforeClass
    public void setUp() throws Exception {
        Class.forName("org.h2.Driver");
    }

// -------------------------- OTHER METHODS --------------------------


    private void info(String type, Connection connection, String order, String prefix, String suffix) throws SQLException {
        Statement statement = connection.createStatement();
        Utils.drop(statement, "DROP TABLE ONE");
        Utils.drop(statement, "DROP TABLE TWO");
        statement.execute("CREATE TABLE ONE (x int, y int, z int)");
        statement.execute("CREATE TABLE TWO AS " + prefix + "SELECT X+Y as A , X*Y as B from one " + order + suffix);
        {
            ResultSet rs = statement.executeQuery("SELECT X+Y , X*Y as b from one order by b, 1 , 2");
            rs.next();
            rs.close();
        }
        {
            ResultSet rs = statement.executeQuery("SELECT max(z) from one GROUP by (X+Y) order by (X+Y)");
            rs.next();
            rs.close();
        }
        {
            ResultSet rs = statement.executeQuery("SELECT distinct sum(X*X) from one GROUP by (X)");
            rs.next();
            rs.close();
        }
        {
            ResultSet rs = statement.executeQuery("SELECT * from one order by 1, 2, 3");
            rs.next();
            rs.close();
        }
        statement.close();

    }

    @Test
    public void h2() throws SQLException {
        String url = "jdbc:h2:mem:";
        Connection linked = DriverManager.getConnection(url, null, null);
        info("H2", linked, "order by 1 , 2", "", "");
    }

    @Test
    public void oracle() throws Exception {
        if (TestConfig.ORACLE) {
            Connection linked =TestConfig.getOracleConnection();
            info("Oracle", linked, "order by 1 , 2", "", "");
        }
    }

    @Test(groups = {"teradata"})
    public void teradata() throws Exception {
        if (TestConfig.TERADATA) {
            Connection linked = TestConfig.getTeradataConnection();
            info("Teradata", linked, "", " (", ") WITH DATA");
        }
    }
}