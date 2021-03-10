package com.dullesopen.h2test.features;

import com.dullesopen.h2test.TestConfig;
import com.dullesopen.h2test.Utils;
import org.h2.api.ErrorCode;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.sql.*;

/**
 * Aggregate function with boolean values
 */
public class AggregateTest {
// ------------------------------ FIELDS ------------------------------

    private Connection h2;

// -------------------------- TEST METHODS --------------------------

    @BeforeMethod
    protected void setUp() throws Exception {
        Class.forName("org.h2.Driver");
        h2 = DriverManager.getConnection("jdbc:h2:mem:;mode=Carolina");
    }

    @AfterMethod
    protected void tearDown() throws Exception {
        h2.close();
    }

// -------------------------- OTHER METHODS --------------------------

    /**
     * H2 does not allow sum on boolean values
     *
     * @throws Exception
     */
    @Test
    public void sumWithBoolean() throws Exception {
        Statement statement = h2.createStatement();
        statement.execute("DROP TABLE FOO IF EXISTS");
        statement.execute("CREATE TABLE FOO (B BOOLEAN)");
        statement.execute("insert into FOO values(true)");
        statement.execute("insert into FOO values(true)");
        statement.execute("insert into FOO values(false)");
        statement.execute("insert into FOO values(true)");
        {
            ResultSet rs = statement.executeQuery("select sum(B) from FOO");
            rs.next();
            Assert.assertEquals(rs.getDouble(1), 3.0);
        }
        {
            try {
                ResultSet rs = statement.executeQuery("select avg(B) from FOO");
                Assert.fail();
                Assert.assertEquals(rs.getDouble(1), 0.75);
            } catch (SQLException e) {
                int code = ErrorCode.SUM_OR_AVG_ON_WRONG_DATATYPE_1;
                Assert.assertEquals(e.getErrorCode(), code);
                Assert.assertEquals(Utils.truncate(e), "SUM or AVG on wrong data type for \"AVG(B)\"");
            }
        }
    }

    @Test
    public void sumWithMissing() throws Exception {
        Statement statement = h2.createStatement();
        statement.execute("DROP TABLE FOO IF EXISTS");
        statement.execute("CREATE TABLE FOO (X DOUBLE PRECISION)");
        statement.execute("insert into FOO values(0.0)");
        statement.execute("insert into FOO values(1.0)");
        statement.execute("insert into FOO values(2.718281828459045235)");
        {
            ResultSet rs = statement.executeQuery("select sum(log(X)) from FOO");
            rs.next();
            Assert.assertEquals(rs.getDouble(1), 1.0);
        }
        {
            ResultSet rs = statement.executeQuery("select min(log(X)) from FOO");
            rs.next();
            Assert.assertEquals(rs.getDouble(1), Double.NEGATIVE_INFINITY);
        }
        {
            ResultSet rs = statement.executeQuery("select max(log(X)) from FOO");
            rs.next();
            Assert.assertEquals(rs.getDouble(1), 1.0);
        }
    }

    @Test
    public void sumWithMissingOracleBinaryFloat() throws Exception {
        if (TestConfig.ORACLE) {
            Connection ora = TestConfig.getOracleConnection();
            Statement statement = ora.createStatement();
            try {
                statement.execute("DROP TABLE FOO");
            } catch (SQLException e) {
            }
            statement.execute("create table FOO as select TO_BINARY_FLOAT(0) X from DUAL");
            statement.execute("insert into FOO values(1.0)");
            statement.execute("insert into FOO values(2.718281828459045235)");
            {
                ResultSet rs = statement.executeQuery("select sum(ln(X)) from FOO");
                rs.next();
                Assert.assertEquals(rs.getDouble(1), Double.NEGATIVE_INFINITY);
            }
            statement.executeUpdate("insert into FOO values( to_binary_float('INF'))");
            {
                ResultSet rs = statement.executeQuery("select sum(X) from FOO");
                rs.next();
                Assert.assertEquals(rs.getDouble(1), Double.POSITIVE_INFINITY);
            }
            statement.close();
            ora.close();
        }
    }
}
