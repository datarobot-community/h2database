package org.h2.contrib.external.demo;


import org.h2.jdbc.JdbcConnection;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.MessageFormat;


public class SimpleSchemaTest {

    @BeforeClass
    protected void setUp() throws Exception {
        Class.forName("org.h2.Driver");
    }

// -------------------------- OTHER METHODS --------------------------

    @Test
    public void extended() throws Exception {
        try (JdbcConnection h2 = (JdbcConnection) DriverManager.getConnection("jdbc:h2:mem:memory",
                "sa2", "def")) {
            h2.setClientContext(new DemoContext("Uniq-1234"));
            try (Statement stat = h2.createStatement()) {
                stat.execute(MessageFormat.format("CREATE schema DEMO EXTERNAL (''{0}'','''')", DemoSchemaFactory.class.getName()));
                try {
                    stat.execute("CREATE TABLE DEMO.T (ID INT)");
                    Assert.fail();
                } catch (SQLException e) {
                    UnsupportedOperationException cause = (UnsupportedOperationException) e.getCause();
                    Assert.assertEquals(cause.getMessage(), "Simple failed for: Uniq-1234");
                }
            }
        }
    }

}
