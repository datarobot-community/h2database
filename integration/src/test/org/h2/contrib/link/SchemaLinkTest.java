package org.h2.contrib.link;

import org.h2.contrib.external.TestConfig;
import org.h2.jdbc.JdbcConnection;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.sql.*;
import java.text.MessageFormat;
import java.util.Properties;

public class SchemaLinkTest {
// -------------------------- TEST METHODS --------------------------

    @BeforeClass
    protected void setUp() throws Exception {
        Class.forName("org.h2.Driver");
    }

// -------------------------- OTHER METHODS --------------------------

    /**
     * H2 has strange table build in table dual similar to Oracle
     *
     * @throws Exception
     */
    @Test
    public void dual() throws Exception {

        Connection link = DriverManager.getConnection("jdbc:h2:mem:dual", "linkuser", "linkpass");
        Statement sa = link.createStatement();
        ResultSet rs = sa.executeQuery("select \"X\" from dual;");
        rs.getMetaData().getColumnName(1);

        Connection main = DriverManager.getConnection("jdbc:h2:mem:main", "mainuser", "mainpass");
        Statement sm = main.createStatement();
        sm.execute("create linked table test_link('org.h2.Driver', 'jdbc:h2:mem:dual', 'linkuser', 'linkpass', 'DUAL')");
    }

    @Test
    public void testCloseOracleCursors() throws Exception {

        if (TestConfig.ORACLE) {
            int N = 1 /*1000 for stress testing*/;

            JdbcConnection conn = new JdbcConnection("jdbc:h2:mem:", new Properties());
            Connection oracle = TestConfig.getOracleConnection();
            {
                Statement statement = oracle.createStatement();
                try {
                    statement.execute("DROP TABLE ONE");
                } catch (SQLException e) {
                }
                statement.execute("CREATE TABLE ONE (I INTEGER)");
                statement.close();
            }


            conn.addExternalConnection("MYORA", oracle);
            for (int i = 0; i < N; i++) {
                System.out.println("i = " + i);
                {
                    Statement stat = conn.createStatement();
                    stat.execute("DROP SCHEMA ORA IF EXISTS");
                    stat.execute("CREATE SCHEMA ORA LINKED ('MYORA','')");
                    stat.close();
                }
                {
                    Statement stat = conn.createStatement();
                    ResultSet rs = stat.executeQuery("SELECT * FROM ORA.ONE");
                    rs.close();
                    stat.close();
                }
            }
        }
    }

    @Test(enabled = false)
    public void extended() throws Exception {

        Connection link = DriverManager.getConnection("jdbc:h2:mem:linked", "linkuser", "linkpass");

        Statement sl = link.createStatement();
        sl.execute("CREATE TABLE FOO ( X INT)");
        sl.execute("CREATE SCHEMA S ");
        sl.execute("CREATE TABLE S.BAR (X INT)");

        Connection test = DriverManager.getConnection("jdbc:h2:mem:", "", "");
        Statement statement = test.createStatement();


        String schema = MessageFormat.format("CREATE SCHEMA L LINKED (''{0}'', ''{1}'', ''{2}'', ''{3}'', '''');",
                "org.h2.Driver", "jdbc:h2:mem:linked", "linkuser", "linkpass");

        statement.execute(schema);
        schema = MessageFormat.format("CREATE SCHEMA LS LINKED (''{0}'', ''{1}'', ''{2}'', ''{3}'', ''{4}'');",
                "org.h2.Driver", "jdbc:h2:mem:linked", "linkuser", "linkpass", "S");

        statement.execute(schema);

        statement.executeQuery("SELECT * FROM  L.FOO");
        statement.executeQuery("SELECT * FROM  LS.BAR");

        statement.execute("DROP TABLE L.FOO");
        statement.execute("DROP TABLE LS.BAR");

        statement.execute("DROP TABLE L.NOTFOUND IF EXISTS");
        statement.execute("DROP TABLE LS.NOTFOUND IF EXISTS");


        test.close();
        link.close();
    }
}
