package org.h2.contrib.misc;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.testng.Assert.assertEquals;

/**
 * Test function IS_NULL
 */
public class FunctionIsNullTest {
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

    @Test
    public void isnull() throws Exception {
        Statement statement = h2.createStatement();
        statement.execute("CREATE TABLE B(X DOUBLE)");
        statement.execute("INSERT INTO B VALUES(NULL)");
        statement.execute("INSERT INTO B VALUES(1)");


        String s1 = "SELECT is_null(X) from B";
        ResultSet rs = statement.executeQuery(s1);
        rs.next();
        assertEquals(rs.getBoolean(1), true);
        rs.next();
        assertEquals(rs.getBoolean(1), false);
    }

}
