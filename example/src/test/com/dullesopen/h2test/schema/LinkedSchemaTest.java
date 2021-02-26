package com.dullesopen.h2test.schema;

import com.dullesopen.h2.external.ExternalQueryExecutionReporter;
import org.h2.jdbc.JdbcConnection;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.testng.collections.Lists;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.testng.Assert.assertEquals;

/**
 * Created by Pavel on 6/16/2015.
 */
public class LinkedSchemaTest {


    @Test
    public void linkedNamedConnection() throws Exception {
        Class.forName("org.h2.Driver");
        Connection ca = DriverManager.getConnection("jdbc:h2:mem:", "linkuser", "linkpass");
        JdbcConnection cb = (JdbcConnection) DriverManager.getConnection("jdbc:h2:mem:");

        cb.addExternalConnection("#one#", ca);
        Statement sa = ca.createStatement();
        sa.execute("CREATE TABLE A (B INT)");
        sa.execute("INSERT INTO A VALUES (123456)");

        Statement sb = cb.createStatement();
        sb.execute("CREATE SCHEMA db2 LINKED ('#one#','')");
        ResultSet rs = sb.executeQuery("SELECT * FROM db2.A ");
        rs.next();
        assertEquals(rs.getInt(1), 123456);
        sb.close();

        // validate that sa connection was not closed
        ResultSet rsa = sa.executeQuery("SELECT * FROM A ");
        rsa.next();
        assertEquals(rsa.getInt(1), 123456);
        sa.close();
    }

    @Test
    public void doNotCloseOnError() throws Exception {
        Class.forName("org.h2.Driver");
        Connection ca = DriverManager.getConnection("jdbc:h2:mem:", "linkuser", "linkpass");
        JdbcConnection cb = (JdbcConnection) DriverManager.getConnection("jdbc:h2:mem:");

        cb.addExternalConnection("#one#", ca);
        Statement sa = ca.createStatement();
        sa.execute("CREATE TABLE A (B INT)");
        sa.execute("INSERT INTO A VALUES (123456)");

        Statement sb = cb.createStatement();
        sb.execute("CREATE SCHEMA db2 LINKED ('#one#','')");
        try {
            sb.executeQuery("SELECT * FROM db2.unknown ");
            Assert.fail();
        } catch (SQLException e) {
        }

        ResultSet rs = sb.executeQuery("SELECT * FROM db2.A ");
        rs.next();
        assertEquals(rs.getInt(1), 123456);

        sa.close();
        sb.close();
    }

    @Test
    public void removeLinkedConnection() throws Exception {

        Class.forName("org.h2.Driver");
        Connection ca = DriverManager.getConnection("jdbc:h2:mem:", "linkuser", "linkpass");
        JdbcConnection cb = (JdbcConnection) DriverManager.getConnection("jdbc:h2:mem:");

        cb.addExternalConnection("#one#", ca);
        Statement sa = ca.createStatement();
        sa.execute("CREATE TABLE A (B INT)");
        sa.execute("INSERT INTO A VALUES (123456)");
        cb.removeExternalConnection("#one#");

        Statement sb = cb.createStatement();
        sb.execute("CREATE SCHEMA db2 LINKED ('#one#','')");
        try {
            sb.execute("SELECT * FROM db2.ONE");
            Assert.fail();
        } catch (SQLException e) {
            Assert.assertEquals(e.getMessage(), "External connection \"#one#\" not defined.; SQL statement:\n" +
                    "SELECT * FROM db2.ONE [99005-196]");
        }
        sa.close();
        sb.close();

    }


    @Test
    public void report() throws Exception {
        Class.forName("org.h2.Driver");
        Connection rdbm = DriverManager.getConnection("jdbc:h2:mem:", "linkuser", "linkpass");
        JdbcConnection h2 = (JdbcConnection) DriverManager.getConnection("jdbc:h2:mem:");

        h2.addExternalConnection("#one#", rdbm);
        final List<String> messages = new ArrayList<String>();
        h2.addExternalQueryExecutionReporter(new ExternalQueryExecutionReporter() {
            @Override
            public void report(Action action, String schema, String sql, Connection connection) {
                String msg = action + " : " + schema + " : " + sql;
                System.out.println("msg = " + msg);
                messages.add(msg);
            }
        });

        Statement sa = rdbm.createStatement();
        sa.execute("CREATE TABLE A (B INT)");
        sa.execute("INSERT INTO A VALUES (123456)");
        {
            Statement sb = h2.createStatement();
            sb.execute("CREATE SCHEMA db2 LINKED ('#one#','')");
            sb.close();
        }
        {
            Statement sb = h2.createStatement();
            ResultSet rs = sb.executeQuery("SELECT T.B FROM db2.A T,db2.A U  NATURAL JOIN db2.A R");
            rs.next();
        }
        sa.execute("DROP TABLE A");
        sa.execute("CREATE TABLE A (C VARCHAR(10), D  VARCHAR(20))");
        sa.execute("INSERT INTO A VALUES ('abc','xyz')");

        {
            Statement sb = h2.createStatement();
            sb.execute("FLUSH SCHEMAS");
            ResultSet rs = sb.executeQuery("SELECT * FROM db2.A ");
            rs.next();
        }
        sa.close();

        Assert.assertEquals(messages, Lists.newArrayList(
                "PREPARE : DB2 : SELECT * FROM PUBLIC.A T",
                "EXECUTE : DB2 : SELECT * FROM PUBLIC.A T",
                "PREPARE : DB2 : SELECT * FROM PUBLIC.A T WHERE B>=? AND B<=?",
                "EXECUTE : DB2 : SELECT * FROM PUBLIC.A T WHERE B>=? AND B<=?",
                "PREPARE : DB2 : SELECT * FROM PUBLIC.A T",
                "EXECUTE : DB2 : SELECT * FROM PUBLIC.A T",
                "PREPARE : DB2 : SELECT * FROM PUBLIC.A T",
                "EXECUTE : DB2 : SELECT * FROM PUBLIC.A T"
        ));
    }


}
