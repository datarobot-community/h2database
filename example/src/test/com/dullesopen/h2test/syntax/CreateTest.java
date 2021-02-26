package com.dullesopen.h2test.syntax;

import com.dullesopen.h2test.Utils;
import org.h2.api.ErrorCode;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.sql.*;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class CreateTest {
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
    public void create() throws Exception {
        Statement s1 = h2.createStatement();

        s1.execute("CREATE TABLE FOO (A CHAR(10))");
        s1.execute("CREATE TABLE BAR AS SELECT * FROM FOO");

        ResultSet rs = s1.executeQuery("SELECT * FROM BAR");
        ResultSetMetaData md = rs.getMetaData();
        int precision = md.getPrecision(1);
        assertEquals(precision, 10);
    }

    @Test
    public void memory() throws Exception {

        Statement statement = h2.createStatement();
        statement.execute("DROP TABLE one if exists");
        statement.execute("DROP TABLE two if exists");
        statement.execute("DROP TABLE three if exists");
        statement.execute("CREATE TABLE one(id INT )");

        PreparedStatement ins = h2.prepareStatement("insert into one values(?)");
        int N = 100;

        for (int i = 0; i < N; i++) {
            for (int j = 0; j < 100; j++) {
                ins.setInt(1, i * 100 + j);
                ins.addBatch();
            }
            ins.executeBatch();
        }

        String sql = "create table two as select id from one group by id";
        statement.execute(sql);
        statement.execute("create table three as select id from one group by id order by id");
        statement.getUpdateCount();
    }

    @Test
    public void twoTables() throws Exception {
        Statement statement = h2.createStatement();
        statement.execute("CREATE TABLE one(A CHAR(1), B CHAR(6))");


        String select = "select * from\n" +
                "(select A as X from one where B='K' ),\n" +
                "(select A as Y from one where B='L' )";
        ResultSet rs = statement.executeQuery(select);
        ResultSetMetaData meta = rs.getMetaData();
        assertEquals(meta.getColumnCount(), 2);
        assertEquals(meta.getColumnName(1), "X");
        assertEquals(meta.getColumnName(2), "Y");


        String s2 = "create table matrix as " + select;
        statement.execute(s2);
    }

    @Test
    public void fullOuterJoinAsUnion() throws Exception {

        Statement statement = h2.createStatement();

        statement.execute("CREATE TABLE one(id INT , gr INT)");

        PreparedStatement ins = h2.prepareStatement("insert into one values(?,?)");
        int N = 10;
        int M = 10;

        for (int i = 0; i < N; i++) {
            for (int j = 0; j < M; j++) {
                ins.setInt(1, i * M + j);
                ins.setInt(2, i);
                ins.addBatch();
            }
            ins.executeBatch();
        }
        statement.execute("CREATE INDEX IND ON one(id , gr)");

        // Trying to replace full outer join as union of left and right outer join

        String sql = "create table two as " +
                " select a.id, a.gr from one a left outer join one b on a.id=b.id and a.gr=b.gr " +
                " union " +
                " select a.id, a.gr from one a right outer join one b on a.id=b.id and a.gr=b.gr ";

        statement.execute(sql);
    }

    @Test
    public void unique() throws Exception {
        Statement s1 = h2.createStatement();
        s1.execute("CREATE TABLE A(VAR CHAR(6) UNIQUE)");

        Statement s2 = h2.createStatement();
        try {
            s2.execute("CREATE TABLE B(VAR CHAR(6) DISTINCT)");
            fail("distinct is illegal here");
        } catch (SQLException e) {
            assertEquals(e.getErrorCode(), ErrorCode.SYNTAX_ERROR_2);
            assertEquals(Utils.truncate(e),
                    "Syntax error in SQL statement \"CREATE TABLE B(VAR CHAR(6) DISTINCT[*])\"; expected \"FOR, UNSIGNED, INVISIBLE, VISIBLE, NOT, NULL, AS, DEFAULT, GENERATED, NOT, NULL, AUTO_INCREMENT, BIGSERIAL, SERIAL, IDENTITY, NULL_TO_DEFAULT, SEQUENCE, SELECTIVITY, COMMENT, EXTENSION, CONSTRAINT, PRIMARY, UNIQUE, NOT, NULL, CHECK, REFERENCES, ,, )\"");
        }
    }

    /**
     * H2 and Oracle do not allow aliases, SAS does ??? or does not???
     *
     * @throws Exception
     */
    @Test
    public void join() throws Exception {
        Statement sa = h2.createStatement();

        sa.execute("CREATE SCHEMA MFE");
        sa.execute("CREATE TABLE MFE.MOVIES (TITLE CHAR(30), RATING INT)");
        sa.execute("CREATE TABLE MFE.ACTORS (TITLE CHAR(30), actor_leading CHAR(30))");
        String sql = "SELECT M.title, M.rating, A.actor_leading\n" +
                "FROM MFE.MOVIES M, MFE.ACTORS A\n" +
                "WHERE MOVIES.title = ACTORS.title ";
        try {
            sa.executeQuery(sql);
            Assert.fail();
        } catch (SQLException e) {
            Assert.assertEquals(e.getErrorCode(),ErrorCode.COLUMN_NOT_FOUND_1);
            Assert.assertEquals(Utils.truncate(e),"Column \"MOVIES.TITLE\" not found");
        }
    }


}
