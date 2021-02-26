package com.dullesopen.h2test.syntax;

import com.dullesopen.h2test.TestConfig;
import com.dullesopen.h2test.Utils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.sql.*;

public class AlterTableTest {
// ------------------------------ FIELDS ------------------------------

    private Connection ca;
    private Connection oracle;

    @BeforeClass
    protected void setUp() throws Exception {
        Class.forName("org.h2.Driver");
        ca = DriverManager.getConnection("jdbc:h2:mem:");
        oracle = TestConfig.getOracleConnection();
    }

    @AfterClass
    protected void tearDown() throws Exception {
        ca.close();
        if (oracle != null) {
            oracle.close();
        }
    }

// -------------------------- OTHER METHODS --------------------------

    @Test
    public void dropColumnH2() throws Exception {
        Statement statement = ca.createStatement();
        statement.execute("CREATE TABLE A(X CHAR(6), Y CHAR(3), Z CHAR(10))");

        String s1 = "ALTER TABLE A DROP COLUMN X;";
        statement.execute(s1);
    }

    @Test
    public void dropTwoColumnH2() throws Exception {
        Statement statement = ca.createStatement();
        statement.execute("CREATE TABLE B(X CHAR(6), Y CHAR(3), Z CHAR(10))");

        String s1 = "ALTER TABLE B DROP COLUMN X;ALTER TABLE B DROP COLUMN Y;";
        statement.execute(s1);
        ResultSet rs = statement.executeQuery("select * from B");
        ResultSetMetaData meta = rs.getMetaData();
        Assert.assertEquals(meta.getColumnCount(), 1);
    }

    @Test
    public void dropColumnOracle() throws Exception {
        if (TestConfig.ORACLE) {
            Statement statement = oracle.createStatement();
            Utils.drop(statement, "DROP TABLE A");
            statement.execute("CREATE TABLE A(X CHAR(6), Y CHAR(3), Z CHAR(10))");
            statement.execute("ALTER TABLE A DROP COLUMN X");
        }
    }
}