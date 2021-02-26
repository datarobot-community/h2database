package com.dullesopen.h2test.features;

import com.dullesopen.h2test.Utils;
import org.h2.api.ErrorCode;
import org.h2.store.fs.FileUtils;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.sql.*;


public class CalculatedAliasTest {
// ------------------------------ FIELDS ------------------------------

    private Connection h2;

// -------------------------- TEST METHODS --------------------------

    @BeforeMethod
    protected void setUp() throws Exception {
        Class.forName("org.h2.Driver");
        FileUtils.deleteRecursive("./target/db", false);
        h2 = DriverManager.getConnection("jdbc:h2:./target/db/calculated;MODE=Carolina");
    }

    @AfterMethod
    protected void tearDown() throws Exception {
        h2.close();
    }

    @Test
    public void calculated() throws Exception {
        try (Statement statement = h2.createStatement()) {
            //Utils.drop(statement, "DROP TABLE A");
            statement.execute("CREATE TABLE A(X INT , Y INT)");
            statement.execute("INSERT INTO A VALUES(1,2)");
            statement.execute("CREATE TABLE C(X INT , R INT)");
            statement.execute("INSERT INTO C VALUES(1,5)");
            try (ResultSet rs = statement.executeQuery("SELECT X + Y AS Z, 2 * CALCULATED Z FROM A")) {
                rs.next();
                Assert.assertEquals(rs.getInt(1), 3);
                Assert.assertEquals(rs.getInt(2), 6);
            }
            try (ResultSet rs = statement.executeQuery("SELECT X + Y AS Z FROM A WHERE CALCULATED Z > 0")) {
                rs.next();
                Assert.assertEquals(rs.getInt(1), 3);
            }
            try (ResultSet rs = statement.executeQuery("SELECT X + Y AS Z FROM A WHERE CALCULATED Z < 0")) {
                Assert.assertFalse(rs.next());
            }
            try (ResultSet rs = statement.executeQuery("SELECT Y + R AS Z, 2 * CALCULATED Z FROM A LEFT OUTER JOIN C ON A.X = C.X")) {
                rs.next();
                Assert.assertEquals(rs.getInt(1), 7);
            }
        }
    }

    @Test
    public void errors() throws Exception {
        try (Statement statement = h2.createStatement()) {
            statement.execute("CREATE TABLE B(X INT , Y INT)");
            statement.execute("INSERT INTO B VALUES(1,2)");
            try {
                statement.executeQuery("SELECT CALCULATED Z , X + Y AS Z FROM B");
            } catch (SQLException e) {
                Assert.assertEquals(e.getErrorCode(), ErrorCode.CALCULATED_ALIAS_NOT_FOUND);
                Assert.assertEquals(Utils.truncate(e), "CALCULATED column \"Z\" not defined before usage");
            }
            try {
                statement.executeQuery("SELECT CALCULATED W , X + Y AS Z FROM B");
            } catch (SQLException e) {
                Assert.assertEquals(e.getErrorCode(), ErrorCode.CALCULATED_ALIAS_NOT_FOUND);
                Assert.assertEquals(Utils.truncate(e), "CALCULATED column \"W\" not defined before usage");
            }

        }
    }
}
