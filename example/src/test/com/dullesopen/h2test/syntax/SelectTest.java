package com.dullesopen.h2test.syntax;

import com.dullesopen.h2test.Utils;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import static org.testng.Assert.fail;

public class SelectTest {
// ------------------------------ FIELDS ------------------------------

    private Connection h2;

// -------------------------- TEST METHODS --------------------------

    @BeforeMethod
    protected void setUp() throws Exception {
        Class.forName("org.h2.Driver");
        h2 = DriverManager.getConnection("jdbc:h2:mem:");
    }

    @AfterMethod
    protected void tearDown() throws Exception {
        h2.close();
    }

// -------------------------- OTHER METHODS --------------------------

    /**
     * Obsolete test:
     * Ancient release of H2 did not include original sql statement
     *
     * @throws Exception
     */
    @Test
    public void missingColumn() throws Exception {
        Statement statement = h2.createStatement();
        statement.execute("CREATE TABLE B(X DOUBLE)");

        String s1 = "SELECT X, Y from B";
        try {
            statement.executeQuery(s1);
            Assert.fail();
        } catch (SQLException e) {
            Assert.assertTrue(e.getMessage().contains("SELECT X, Y from B"));
        }
    }

    @Test
    public void star() throws Exception {
        Statement statement = h2.createStatement();
        statement.execute("CREATE TABLE A(VAR CHAR(6))");


        String s1 = "SELECT DISTINCT COUNT(*) from A";
        statement.executeQuery(s1);

        try {
            String s2 = "SELECT UNIQUE COUNT(*) from A";
            statement.executeQuery(s2);
            fail();
        } catch (SQLException e) {
            Assert.assertEquals(e.getErrorCode(), 42001);
            Assert.assertEquals(Utils.truncate(e),
                    "Syntax error in SQL statement \"SELECT UNIQUE[*] COUNT(*) FROM A \"; expected \"TOP, LIMIT, DISTINCT, ALL, *, NOT, EXISTS, INTERSECTS, SELECT, FROM, WITH\"");
        }
    }
}
