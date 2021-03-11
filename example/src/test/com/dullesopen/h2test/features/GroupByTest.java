package com.dullesopen.h2test.features;

import com.dullesopen.h2test.Utils;
import org.h2.api.ErrorCode;
import org.h2.store.fs.FileUtils;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.sql.*;

import static org.testng.Assert.assertEquals;


public class GroupByTest {
// ------------------------------ FIELDS ------------------------------

    private Connection h2;

// -------------------------- TEST METHODS --------------------------

    @BeforeMethod
    protected void setUp() throws Exception {
        Class.forName("org.h2.Driver");
        FileUtils.deleteRecursive("./target/db", false);
        h2 = DriverManager.getConnection("jdbc:h2:./target/db/order");

        Statement stat = h2.createStatement();
        stat.execute("CREATE TABLE ONE ( X INTEGER, Y INTEGER, Z INTEGER)");
        stat.execute("INSERT INTO ONE(X,Y,Z) VALUES(  30,  100, 130)");
        stat.execute("INSERT INTO ONE(X,Y,Z) VALUES(  40,  300, 140)");
        stat.execute("INSERT INTO ONE(X,Y,Z) VALUES(  40,  310, 140 )");
        stat.execute("INSERT INTO ONE(X,Y,Z) VALUES(  10,  140, 210)");
        stat.execute("INSERT INTO ONE(X,Y,Z) VALUES(  30,  200, 130)");
        stat.execute("INSERT INTO ONE(X,Y,Z) VALUES(  20,  130, 120)");
        stat.execute("INSERT INTO ONE(X,Y,Z) VALUES(  10,  160, 210 )");
        stat.execute("INSERT INTO ONE(X,Y,Z) VALUES(  20,  170, 120 )");
        stat.execute("INSERT INTO ONE(X,Y,Z) VALUES(  12,  200, 112)");
        stat.execute("INSERT INTO ONE(X,Y,Z) VALUES(  50, 1200, 150)");

        stat.execute("CREATE TABLE TWO ( X INTEGER, Y INTEGER, Z INTEGER)");
        stat.execute("INSERT INTO TWO(X,Y,Z) VALUES(  11,  101, 1001)");
        stat.execute("INSERT INTO TWO(X,Y,Z) VALUES(  11,  102, 1001)");
        stat.execute("INSERT INTO TWO(X,Y,Z) VALUES(  11,  103, 1002)");
        stat.execute("INSERT INTO TWO(X,Y,Z) VALUES(  11,  104, 1002)");
        stat.execute("INSERT INTO TWO(X,Y,Z) VALUES(  11,  105, 1003)");
    }

    @AfterMethod
    protected void tearDown() throws Exception {
        h2.close();
    }

// -------------------------- OTHER METHODS --------------------------

    @Test
    public void order() throws Exception {

        Statement stat = h2.createStatement();
        ResultSet rs = stat.executeQuery("SELECT X, AVG(Y) AS M FROM ONE GROUP BY X ORDER BY X");

        int row = 0;
        while (rs.next()) {
            int i = rs.getInt(1);
            int j = rs.getInt(2);
            if (false) System.out.println(i + "/" + j);
            row++;
        }
    }

    @Test
    public void orderAndGroup() throws Exception {

        Statement stat = h2.createStatement();
        ResultSet rs = stat.executeQuery("SELECT X, AVG(Y) AS M FROM ONE GROUP BY Z, X ORDER BY M, Z, X");

        int[][] expected = new int[][]{{20, 150}, {30, 150}, {10, 150}, {12, 200}, {40, 305}, {50, 1200},};

        int row = 0;
        while (rs.next()) {
            int i = rs.getInt(1);
            int j = rs.getInt(2);
            assertEquals(expected[row], new int[]{i, j});
            row++;
        }
    }

    @Test
    public void multiple() throws Exception {

        try {
            Statement stat = h2.createStatement();
            stat.executeQuery("SELECT X, Y AS M FROM ONE GROUP BY X");
        } catch (SQLException e) {
            Assert.assertEquals(e.getErrorCode(), ErrorCode.MUST_GROUP_BY_COLUMN_1);
            Assert.assertEquals(Utils.truncate(e), "Column \"Y\" must be in the GROUP BY list");
        }
    }

    @Test
    public void missingColumn() throws Exception {

        Statement stat = h2.createStatement();
        ResultSet rs = stat.executeQuery("SELECT MIN(Y) AS M FROM ONE GROUP BY X ORDER BY X");
        rs.next();

    }

    @Test
    public void distinct() throws Exception {

        Statement stat = h2.createStatement();
        ResultSet rs = stat.executeQuery("SELECT DISTINCT X FROM ONE GROUP BY Y, X ORDER BY X");

        int[] expected = new int[]{10, 12, 20, 30, 40, 50,};

        int row = 0;
        while (rs.next()) {
            assertEquals(expected[row], rs.getInt(1));
            row++;
        }

        try {
            stat.executeQuery("SELECT DISTINCT X FROM ONE GROUP BY X, Y ORDER BY Y, X");
            Assert.fail();
        } catch (SQLException e) {
            Assert.assertEquals(e.getErrorCode(), ErrorCode.ORDER_BY_NOT_IN_RESULT);
            Assert.assertEquals(Utils.truncate(e), "Order by expression \"Y\" must be in the result list in this case");
        }

        stat.execute("CREATE TABLE ANOTHER AS SELECT DISTINCT X FROM ONE GROUP BY X,Y");
    }

}
