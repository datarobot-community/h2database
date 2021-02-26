package com.dullesopen.h2test.syntax;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

public class CreateIndexTest {
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
    public void grammar() throws Exception {
        Statement statement = h2.createStatement();

        // in H2 names are valid across schema

        statement.execute("DROP SCHEMA S IF EXISTS");
        statement.execute("CREATE SCHEMA S");
        statement.execute("DROP TABLE S.ONE IF EXISTS");
        statement.execute("CREATE TABLE S.ONE (A INT, B INT)");
        statement.execute("CREATE INDEX X ON  S.ONE(A)");
        statement.execute("CREATE INDEX Y ON  S.ONE(A, B)");
        statement.execute("DROP INDEX S.X IF EXISTS");
        statement.execute("DROP INDEX S.Y");

        statement.close();
    }
}
