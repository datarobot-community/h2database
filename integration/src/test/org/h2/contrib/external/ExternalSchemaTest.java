package org.h2.contrib.external;


import org.h2.jdbc.JdbcConnection;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.MessageFormat;


public class ExternalSchemaTest {
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
    public void extended() throws Exception {
        JdbcConnection conn2 = (JdbcConnection) DriverManager.getConnection("jdbc:h2:mem:memory", "sa2", "def");
        conn2.attachExternalContext(new DemoContext());
        Statement stat;
        stat = conn2.createStatement();
        stat.execute(MessageFormat.format("CREATE schema DEMO EXTERNAL (''{0}'','''')", DemoSchemaFactory.class.getName()));
        try {
            stat.execute("CREATE TABLE DEMO.T (ID INT)");
            Assert.fail();
        } catch (SQLException e) {
            UnsupportedOperationException cause = (UnsupportedOperationException) e.getCause();
            Assert.assertEquals(cause.getMessage(), "DEMO");
        }
    }

    static class DemoContext {

    }
}
