package org.h2.contrib.link;

import org.h2.contrib.test.Utils;
import org.h2.jdbc.JdbcConnection;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class TableLinkTest {

    @BeforeMethod
    protected void setUp() throws Exception {
        Class.forName("org.h2.Driver");
    }

    @Test
    public void noCreateThroughLink() throws Exception {
        try (Connection ca = DriverManager.getConnection("jdbc:h2:mem:one", "linkuser", "linkpass")) {

            try (Connection cb = DriverManager.getConnection("jdbc:h2:mem:two")) {
                try (Statement sb = cb.createStatement()) {
                    Utils.drop(sb, "DROP SCHEMA db2");
                    cb.unwrap(JdbcConnection.class).addExternalConnection("one", ca);
                    sb.execute("CREATE SCHEMA db2 LINKED ('one','')");
                    try {
                        sb.execute("CREATE TABLE db2.testtable(BNUM FLOAT)");
                        Assert.fail("surprise: create table through link is working now?");
                    } catch (SQLException ignored) {
                    }
                }
            }
        }
    }

    @Test
    public void find() throws Exception {
        try (Connection ca = DriverManager.getConnection("jdbc:h2:mem:db2", "linkuser", "linkpass")) {

            try (Connection cb = DriverManager.getConnection("jdbc:h2:mem:two")) {
                try (Statement sb = cb.createStatement()) {
                    cb.unwrap(JdbcConnection.class).addExternalConnection("db2", ca);
                    sb.execute("CREATE SCHEMA db2 LINKED ('db2','')");
                    try (Statement sa = ca.createStatement()) {
                        sa.execute("CREATE TABLE testtable(BNUM FLOAT)");
                        sb.execute("INSERT INTO db2.testtable VALUES(10)");
                    }
                }
            }
        }
    }
}
