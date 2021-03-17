package org.h2.contrib.link;

import org.h2.jdbc.JdbcConnection;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.testng.collections.Lists;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Pavel on 10/7/2016.
 */
public class LinkedQueryTest {

    @Test
    public void linkedView() throws Exception {

        Class.forName("org.h2.Driver");
        Connection ca = DriverManager.getConnection("jdbc:h2:mem:", "linkuser", "linkpass");
        JdbcConnection cb = (JdbcConnection) DriverManager.getConnection("jdbc:h2:mem:");

        cb.addExternalConnection("one", ca);
        Statement sa = ca.createStatement();
        sa.execute("CREATE TABLE A (B INT)");
        sa.execute("INSERT INTO A VALUES (123456)");
        sa.execute("INSERT INTO A VALUES (456)");

        Statement sb = cb.createStatement();
        sb.execute("CREATE LINKED VIEW v WITH EXTERNAL CONNECTION ('one','select * from a where b=456')");
        ResultSet rs = sb.executeQuery("SELECT * FROM v");
        rs.next();
        Assert.assertEquals(rs.getInt("b"), 456);
        Assert.assertFalse(rs.next());
        rs.close();
        sa.close();
        sb.close();

    }

    @Test
    public void reporter() throws Exception {

        Class.forName("org.h2.Driver");
        Connection ca = DriverManager.getConnection("jdbc:h2:mem:", "linkuser", "linkpass");
        JdbcConnection cb = (JdbcConnection) DriverManager.getConnection("jdbc:h2:mem:");

        cb.addExternalConnection("one", ca);
        final List<String> messages = new ArrayList<>();
        cb.addLinkedQueryExecutionReporter((action, schema, sql, connection) -> {
            String msg = action + " : " + schema + " : " + sql;
            System.out.println("msg = " + msg);
            messages.add(msg);
        });
        Statement sa = ca.createStatement();
        sa.execute("CREATE TABLE A (B INT)");
        sa.execute("INSERT INTO A VALUES (123456)");
        sa.execute("INSERT INTO A VALUES (456)");

        {
            Statement sb = cb.createStatement();
            sb.execute("CREATE LINKED VIEW v WITH EXTERNAL CONNECTION ('one','select * from a where b=456')");
            ResultSet rs = sb.executeQuery("SELECT * FROM v");
            rs.next();
            rs.close();
            sb.close();
        }
        {
            Statement sb = cb.createStatement();
            ResultSet rs = sb.executeQuery("SELECT * FROM v");
            rs.next();
            rs.close();
            sb.close();
        }
        sa.close();
        Assert.assertEquals(messages, Lists.newArrayList(
                "PREPARE : PUBLIC : select * from a where b=456",
                "PREPARE : PUBLIC : select * from a where b=456",
                "EXECUTE : PUBLIC : select * from a where b=456",
                "REUSE : PUBLIC : select * from a where b=456",
                "EXECUTE : PUBLIC : select * from a where b=456"
                ));

    }

    @Test
    public void wrongColumn() throws Exception {

        Class.forName("org.h2.Driver");
        Connection ca = DriverManager.getConnection("jdbc:h2:mem:", "linkuser", "linkpass");
        JdbcConnection cb = (JdbcConnection) DriverManager.getConnection("jdbc:h2:mem:");

        cb.addExternalConnection("one", ca);
        Statement sa = ca.createStatement();
        sa.execute("CREATE TABLE A (B INT)");

        Statement sb = cb.createStatement();
        try {
            sb.execute("CREATE LINKED VIEW v WITH EXTERNAL CONNECTION ('one','select * from a where c=456')");
        } catch (SQLException e) {
            Assert.assertEquals(e.getMessage(),
                    "Column \"C\" not found; SQL statement:\n" +
                            "select * from a where c=456 [42122-196]");
        }
        sa.close();
        sb.close();
    }

    @Test
    public void noExternalConnection() throws Exception {

        Class.forName("org.h2.Driver");
        JdbcConnection cb = (JdbcConnection) DriverManager.getConnection("jdbc:h2:mem:");

        Statement sb = cb.createStatement();
        try {
            sb.execute("CREATE LINKED VIEW v WITH EXTERNAL CONNECTION ('two','select * from a where c=456')");
        } catch (SQLException e) {
            Assert.assertEquals(e.getMessage(),
                    "External connection \"two\" not defined.; SQL statement:\n" +
                            "CREATE LINKED VIEW v WITH EXTERNAL CONNECTION ('two','select * from a where c=456') [99005-196]");
        }
        sb.close();
    }


}
