package org.h2.contrib.external;


import org.h2.api.ErrorCode;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.sql.*;


public class ViewTest {
// ------------------------------ FIELDS ------------------------------

    @BeforeMethod
    public void setUpConnection() throws Exception {
        TestUtils.setUpConnection();
    }

    @AfterMethod
    public void tearDownConnection() throws Exception {
        TestUtils.tearDownConnection();
    }

    @Test
    public void doubleView() throws Exception {
        Connection connection = DriverManager.getConnection(TestUtils.URL);
        TestUtils.create(10, connection, true);
        try (Statement statement = connection.createStatement()) {
            statement.execute("CREATE VIEW mylib.vone as select * from mylib.one");
            statement.execute("FLUSH SCHEMAS");
            try (ResultSet rs = statement.executeQuery("SELECT * FROM mylib.vone")) {
                while (rs.next()) {
                }
            }
            connection.commit();
            statement.execute("FLUSH SCHEMAS");
            statement.execute("CREATE VIEW mylib.vtwo as select * from mylib.vone");
            statement.execute("FLUSH SCHEMAS");
            try (ResultSet rs = statement.executeQuery("SELECT * FROM mylib.vtwo")) {
                int n = 0;
                while (rs.next()) {
                    n++;
                }
                Assert.assertEquals(n, 10);
            }
        }
        connection.close();
    }

    @Test
    public void deleteViewWithCascade() throws Exception {
        try (Connection connection = DriverManager.getConnection(TestUtils.URL)) {
            TestUtils.create(10, connection, false);
            try (Statement stat = connection.createStatement()) {
                stat.execute("CREATE TABLE MYLIB.A (x INT)");
                stat.execute("CREATE VIEW MYLIB.B AS SELECT * FROM MYLIB.A");
                stat.execute("DROP TABLE MYLIB.A CASCADE");
                try {
                    stat.executeQuery("SELECT * FROM  MYLIB.B");
                    Assert.fail();
                } catch (SQLException e) {
                    Assert.assertEquals(e.getErrorCode(), ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1);
                }
            }
        }
    }
}