package com.dullesopen.h2test.features;


import com.dullesopen.h2test.TestConfig;
import com.dullesopen.h2test.Utils;
import org.testng.Assert;
import org.testng.annotations.*;

import java.sql.*;

public class ResolverTest {
    // ------------------------------ FIELDS ------------------------------

    private Connection h2;
    private Connection oracle;
    private Statement stat;
    private Connection teradata;

// -------------------------- TEST METHODS --------------------------

    @BeforeMethod
    protected void setUp() throws Exception {
        Class.forName("org.h2.Driver");
        h2 = DriverManager.getConnection("jdbc:h2:mem:;MIXED_CASE=true");
        stat = h2.createStatement();
    }

    @AfterMethod
    protected void tearDown() throws Exception {
        h2.close();
    }

    @BeforeClass
    protected void beforeClass() throws Exception {
        oracle = TestConfig.getOracleConnection();
        if (oracle != null) {
            oracle.setAutoCommit(false);
        }
        teradata = TestConfig.getTeradataConnection();

    }

    @AfterClass
    protected void afterClass() throws Exception {
        if (oracle != null) {
            oracle.close();
        }
        if (teradata != null) {
            teradata.close();
        }
    }

// -------------------------- OTHER METHODS --------------------------

    @Test
    public void useTableInsteadOfAlias() throws Exception {
        Statement statement = h2.createStatement();
        statement.execute("CREATE TABLE foo(a CHAR(6))");
        statement.execute("INSERT INTO foo values('abcdef')");

        String sql = "select foo.a from foo as f";
        ResultSet rs = null;
        try {
            statement.executeQuery(sql);
            Assert.fail();
        } catch (SQLException e) {
            Assert.assertEquals(e.getErrorCode(), 42122);
            Assert.assertEquals(Utils.truncate(e), "Column \"FOO.A\" not found");
        }
        statement.close();
    }

    @Test
    public void useTableInsteadOfAliasOracle() throws Exception {
        if (TestConfig.ORACLE) {
            Statement statement = oracle.createStatement();
            Utils.drop(statement, "drop table one");
            statement.execute("create table one(a integer)");
            statement.execute("insert into one values(10)");

            String sql = "select foo.a from one f";
            try {
                statement.executeQuery(sql);
                Assert.fail();
            } catch (SQLException e) {
                Assert.assertEquals(e.getErrorCode(), 904);
                Assert.assertEquals(e.getMessage().trim(), "ORA-00904: \"FOO\".\"A\": invalid identifier");
            }
            statement.close();
        }
    }

    @Test(groups = {"Teradata"})
    public void useTableInsteadOfAliasTeradata() throws Exception {
        if (TestConfig.TERADATA) {

            Statement statement = teradata.createStatement();

            Utils.drop(statement, "drop table one");
            statement.execute("create table one(a integer)");
            statement.execute("insert into one values(10)");

            try {
                statement.executeQuery("select foo.a from one f");
                Assert.fail();
            } catch (SQLException e) {
                Assert.assertEquals(e.getErrorCode(), 3807);
                Assert.assertEquals(Utils.teradata(e), "[SQLState 42S02] Object 'foo' does not exist.");
            }

            statement.close();
        }
    }
}
