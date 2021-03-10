package org.h2.contrib.misc;

import org.h2.api.ErrorCode;
import org.h2.contrib.test.Utils;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.sql.*;

/**
 * https://bitbucket.org/dullesresearch/h2database/issues/7/group-by-column-number
 */

public class GroupByColumnNumberTest {
// ------------------------------ FIELDS ------------------------------

    private Connection h2;

// -------------------------- TEST METHODS --------------------------

    @BeforeMethod
    protected void setUp() throws Exception {
        Class.forName("org.h2.Driver");
        h2 = DriverManager.getConnection("jdbc:h2:mem:;MODE=Carolina");

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

    @Test
    public void byIndex() throws Exception {

        try (Statement stat = h2.createStatement()) {
            try (ResultSet rs = stat.executeQuery("SELECT X, SUM(Y), Z AS M FROM TWO GROUP BY 1, 3 ORDER BY 1, 3")) {

                int row = 0;
                while (rs.next()) {
                    int x = rs.getInt(1);
                    int y = rs.getInt(2);
                    int z = rs.getInt(3);
                    switch (row) {
                        case 0:
                            Assert.assertEquals(x, 11);
                            Assert.assertEquals(y, 203);
                            Assert.assertEquals(z, 1001);
                            break;
                        case 1:
                            Assert.assertEquals(x, 11);
                            Assert.assertEquals(y, 207);
                            Assert.assertEquals(z, 1002);
                            break;
                        case 2:
                            Assert.assertEquals(x, 11);
                            Assert.assertEquals(y, 105);
                            Assert.assertEquals(z, 1003);
                            break;
                    }
                    row++;
                }
                Assert.assertEquals(row, 3);
            }
        }
    }

    @Test
    public void byIndexStar() throws Exception {

        try (Statement stat = h2.createStatement()) {
            try (ResultSet rs = stat.executeQuery("SELECT * FROM TWO GROUP BY 1, 2, 3")) {
                int row = 0;
                while (rs.next()) {
                    row++;
                }
                Assert.assertEquals(row, 5);
            }
        }
    }

    @Test
    public void invalidIndex() throws Exception {
        try (Statement stat = h2.createStatement()) {
            try (ResultSet ignored = stat.executeQuery("SELECT X, SUM(Y) FROM TWO GROUP BY 1, 3")) {
                Assert.fail();
            } catch (SQLException e) {
                Assert.assertEquals(e.getErrorCode(), ErrorCode.GROUP_BY_INVALID_INDEX);
                Assert.assertEquals(Utils.truncate(e), "GROUP BY \"3\" is not valid. Select list contains \"2\" columns");
            }
        }
    }
}