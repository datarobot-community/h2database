package com.dullesopen.h2test.syntax;

import com.dullesopen.h2test.TestConfig;
import com.dullesopen.h2test.Utils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.sql.*;

public class GroupByTest {
// ------------------------------ FIELDS ------------------------------

    private Connection ca;
    private Connection oracle;
    private Connection teradata;

    @BeforeClass
    protected void setUp() throws Exception {
        Class.forName("org.h2.Driver");
        ca = DriverManager.getConnection("jdbc:h2:mem:");
        oracle = TestConfig.getOracleConnection();
        teradata = TestConfig.getTeradataConnection();
    }

    @AfterClass
    protected void tearDown() throws Exception {
        ca.close();
        if (oracle != null) {
            oracle.close();
        }
        if (teradata != null) {
            teradata.close();
        }
    }

    @Test
    public void groupByIndexOracle() throws Exception {
        if (TestConfig.ORACLE) {
            try (Statement statement = oracle.createStatement()) {
                Utils.drop(statement, "DROP TABLE A");
                statement.execute("CREATE TABLE A(X INT , Y INT, Z INT)");
                statement.execute("INSERT INTO A VALUES(1,2,3)");
                try {
                    statement.executeQuery("SELECT X, SUM(Y), Z FROM A GROUP BY 1 , 3");
                } catch (SQLException e) {
                    Assert.assertEquals("ORA-00979: not a GROUP BY expression\n", e.getMessage());
                }
            }
        }
    }

    @Test
    public void groupByIndexTeradata() throws Exception {
        if (TestConfig.TERADATA) {
            try (Statement statement = teradata.createStatement()) {
                Utils.drop(statement, "DROP TABLE A");
                statement.execute("CREATE TABLE A(X INT , Y INT, Z INT)");
                statement.execute("INSERT INTO A VALUES(1,2,3)");
                ResultSet rs = statement.executeQuery("SELECT X, SUM(Y), Z FROM A GROUP BY 1 , 3");
            }
        }
    }
}