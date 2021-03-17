package org.h2.contrib;

import org.h2.jdbc.JdbcResultSetMetaData;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class ColumnMetaExtensionTest {

    @BeforeMethod
    protected void setUp() throws Exception {
        Class.forName("org.h2.Driver");
    }

    @Test
    public void extension() throws Exception {
        try (Connection h2 = DriverManager.getConnection("jdbc:h2:mem:")) {
            try (Statement stat = h2.createStatement()) {
                stat.execute("CREATE TABLE FOO (A INTEGER extension 'foo')");
                try (ResultSet rs = stat.executeQuery("SELECT * FROM FOO")) {
                    JdbcResultSetMetaData meta = rs.getMetaData().unwrap(JdbcResultSetMetaData.class);
                    Assert.assertEquals(meta.getColumnMetaExtension(1), "foo");
                }
            }
        }
    }
}
