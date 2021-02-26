package com.dullesopen.h2test.syntax;

import com.dullesopen.h2test.TestConfig;
import com.dullesopen.h2test.Utils;
import org.h2.api.ErrorCode;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.sql.*;

import static org.testng.Assert.assertEquals;

/**
 * Test miscellaneous UNIONs and their support across multiple database
 */
public class Union {
// ------------------------------ FIELDS ------------------------------

    private Connection h2;

// -------------------------- TEST METHODS --------------------------

    @BeforeMethod
    protected void setUp() throws Exception {
        Class.forName("org.h2.Driver");
        h2 = DriverManager.getConnection("jdbc:h2:mem:syntax");
    }

    @AfterMethod
    protected void tearDown() throws Exception {
        h2.close();
    }

// -------------------------- OTHER METHODS --------------------------

    @Test
    public void caseH2() throws Exception {
        Statement statement = h2.createStatement();
        statement.execute("CREATE TABLE A(X FLOAT)");
        statement.executeQuery("SELECT X , CASE (x) when 1 then 2 else 3 end FROM A");
    }

    /**
     * It used to be a bug in H2 that single case in CASE expression did not work. This is an illustration for Thomas
     * that single case works in Oracle
     *
     * @throws Exception
     */
    @Test
    public void caseOracle() throws Exception {
        if (TestConfig.ORACLE) {
            Connection ora = TestConfig.getOracleConnection();

            Statement statement = ora.createStatement();
            Utils.drop(statement, "DROP TABLE AC");

            statement.execute("CREATE TABLE AC(X NUMBER)");
            statement.executeQuery("SELECT X , CASE (x) when 1 then 2 else 3 end FROM AC ");
            statement.execute("DROP TABLE AC");
            ora.close();
        }
    }

    /**
     * FULL OUTER JOIN is not supported by H2.
     *
     * @throws Exception
     */
    @Test
    public void fullOuterJoin() throws Exception {
        Class.forName("org.h2.Driver");
        Connection ca = DriverManager.getConnection("jdbc:h2:mem:");

        Statement statement = ca.createStatement();

        {
            String s2 = "CREATE table foo ( author char(13), title char(50))";
            statement.execute(s2);

            String s3 = "CREATE table bar ( leader char(6), subject char(20))";
            statement.execute(s3);

            String s4 = "select author, title, subject " +
                    "from foo full outer join bar " +
                    "on author = leader";

            try {
                statement.executeQuery(s4);
                Assert.fail("Surprise: FULL OUTER JOIN is now working in H2?");
            } catch (SQLException e) {
            }
        }
        ca.close();
    }

    @Test
    public void joined() throws Exception {
        Class.forName("org.h2.Driver");
        Connection ca = DriverManager.getConnection("jdbc:h2:mem:u4");

        Statement statement = ca.createStatement();
        statement.execute("CREATE TABLE A(X INT , Y INT)");
        statement.executeQuery("SELECT * FROM (SELECT X FROM A)");
    }

    @Test
    public void unboundConstraintH2() throws Exception {
        Statement statement = h2.createStatement();
        try {
            statement.execute("CREATE TABLE A(X INT CHECK (X>0 AND Y<1), Y INT)");
            Assert.fail("Surprise: unbound constrain is supported by H2?");
        } catch (SQLException e) {
            Assert.assertEquals(e.getErrorCode(), ErrorCode.COLUMN_NOT_FOUND_1);
            Assert.assertEquals(Utils.truncate(e), "Column \"Y\" not found");
        }
    }

    /**
     * Oracle does not support unbound constrains unlike SAS
     *
     * @throws Exception
     */
    @Test
    public void unboundConstraintOracle() throws Exception {
        if (TestConfig.ORACLE) {
            Connection ca = TestConfig.getOracleConnection();

            Statement sa = ca.createStatement();
            Utils.drop(sa, "DROP TABLE AX");
            sa.execute("CREATE TABLE AX(X INT, Y INT, CONSTRAINT XX CHECK( X * Y > 0))"); // OK
            Utils.drop(sa, "DROP TABLE AX");

            Utils.drop(sa, "DROP TABLE BX");
            sa.execute("CREATE TABLE BX(X INT, Y INT CONSTRAINT XY CHECK ( Y * Y > 0))"); // OK
            Utils.drop(sa, "DROP TABLE BX");

            try {
                sa.execute("CREATE TABLE BX(X INT, Y INT CONSTRAINT XZ CHECK ( X * Y > 0))");
                Assert.fail("Surprise: unbound constrain is supported by Oracle?");
            } catch (SQLException e) {
                Assert.assertEquals(e.getErrorCode(), 2438);
                Assert.assertEquals(e.getMessage().trim(), "ORA-02438: Column check constraint cannot reference other columns");
            }
            ca.close();
        }
    }

    @Test
    public void union() throws Exception {
        Class.forName("org.h2.Driver");
        Connection ca = DriverManager.getConnection("jdbc:h2:mem:u5");

        Statement statement = ca.createStatement();
        String s16 = "CREATE table stock (\n" +
                "ISBN char(13),\n" +
                "title char(50),\n" +
                "category char(20),\n" +
                "prodCode char(6),\n" +
                "price number)";
        statement.execute(s16);

        String s14 = "CREATE table orders (\n" +
                "prodCode char(6),\n" +
                "invoice char(10))";
        statement.execute(s14);

        // OK
        String s1 = "SELECT DISTINCT isbn, stock.title, category\n" +
                "FROM stock\n" +
                "JOIN orders\n" +
                "ON stock.prodcode = orders.prodcode\n" +
                "ORDER BY stock.title";
        ResultSet rs1 = statement.executeQuery(s1);

        // FAILS
        String s2 = "SELECT DISTINCT isbn, title, category\n" +
                "FROM stock\n" +
                "JOIN orders\n" +
                "ON stock.prodcode = orders.prodcode\n" +
                "ORDER BY stock.title";
        ResultSet rs2 = statement.executeQuery(s2);
        ca.close();
    }

    /**
     * Oracle does not supports aliases in UNION statements
     *
     * @throws Exception
     */

    @Test
    public void unionAliasOracle() throws Exception {
        if (TestConfig.ORACLE) {
            Connection ca = TestConfig.getOracleConnection();

            Statement statement = ca.createStatement();
            Utils.drop(statement, "DROP TABLE stock");
            Utils.drop(statement, "DROP TABLE orders");

            String s16 = "CREATE table stock (\n" +
                    "ISBN char(13),\n" +
                    "title char(50),\n" +
                    "category char(20),\n" +
                    "prodCode char(6),\n" +
                    "price number)";
            statement.execute(s16);

            String s14 = "CREATE table orders (\n" +
                    "prodCode char(6),\n" +
                    "ord_date date,\n" +
                    "quantity number,\n" +
                    "totsale number,\n" +
                    "currency char(3),\n" +
                    "delCode char(3),\n" +
                    "clientNo number,\n" +
                    "invoice char(10))";
            statement.execute(s14);

            String s = "SELECT client.company ,\n" +
                    "order2.clientno ,\n" +
                    "order1.prodcode ,\n" +
                    "stock.title\n" +
                    "FROM stock JOIN orders AS order1\n" +
                    "ON stock.prodcode=order1.prodcode, " +
                    "client JOIN orders AS order2\n" +
                    "ON order2.clientno = client.clientno\n" +
                    "AND order1.prodcode = order2.prodcode\n" +
                    "AND order1.clientno = order2.clientno\n" +
                    "WHERE stock.category = 'general'";

            try {
                statement.executeQuery(s);
                Assert.fail("surprise: FULL JOIN is now working in H2?");
            } catch (SQLException e) {
            }
            ca.close();
        }
    }

    @Test
    public void unionFourTables() throws Exception {
        Class.forName("org.h2.Driver");
        Connection ca = DriverManager.getConnection("jdbc:h2:mem:u6");

        Statement statement = ca.createStatement();
        statement.execute("CREATE TABLE A ( X INT , A1 INT )");
        statement.execute("CREATE TABLE B ( X INT , B1 INT )");
        statement.execute("CREATE TABLE C ( X INT , Y INT, C1 INT )");
        statement.execute("CREATE TABLE D ( X INT , Y INT, C2 INT )");

        String s1 = "SELECT * FROM A JOIN B ON A.x=B.x, C JOIN D ON C.X=D.X and C.Y=D.y";
        ResultSet rs = statement.executeQuery(s1);
        ResultSetMetaData md = rs.getMetaData();
        assertEquals(md.getColumnCount(), 10);
        assertEquals(md.getColumnName(1), "X");
        assertEquals(md.getColumnName(2), "A1");
        assertEquals(md.getColumnName(3), "X");
        assertEquals(md.getColumnName(4), "B1");
        assertEquals(md.getColumnName(5), "X");
        assertEquals(md.getColumnName(6), "Y");
        assertEquals(md.getColumnName(7), "C1");
        assertEquals(md.getColumnName(8), "X");
        assertEquals(md.getColumnName(9), "Y");
        assertEquals(md.getColumnName(10), "C2");
        ca.close();
    }

    /**
     * FULL JOIN is not supported by H2
     *
     * @throws Exception
     */
    @Test
    public void unionH2() throws Exception {
        Class.forName("org.h2.Driver");
        Connection ca = DriverManager.getConnection("jdbc:h2:mem:u7");

        Statement statement = ca.createStatement();

        String s16 = "CREATE table stock (\n" +
                "ISBN char(13),\n" +
                "title char(50),\n" +
                "category char(20),\n" +
                "prodCode char(6),\n" +
                "price number)";
        statement.execute(s16);

        String s14 = "CREATE table orders (\n" +
                "prodCode char(6),\n" +
                "ord_date date,\n" +
                "quantity number,\n" +
                "totsale number,\n" +
                "currency char(3),\n" +
                "delCode char(3),\n" +
                "clientNo number,\n" +
                "invoice char(10))";
        statement.execute(s14);

        String s11 = "CREATE table client (\n" +
                "clientNo number,\n" +
                "company char(40),\n" +
                "cont_fst char(10),\n" +
                "cont_lst char(15),\n" +
                "dept char(25),\n" +
                "phone char(25),\n" +
                "fax char(25) )";
        statement.execute(s11);

        String s = "SELECT client.company ,\n" +
                "order2.clientno ,\n" +
                "order1.prodcode ,\n" +
                "stock.title\n" +
                "FROM stock JOIN orders AS order1\n" +
                "ON stock.prodcode=order1.prodcode, " +
                "client JOIN orders AS order2\n" +
                "ON order2.clientno = client.clientno\n" +
                "AND order1.prodcode = order2.prodcode\n" +
                "AND order1.clientno = order2.clientno\n" +
                "WHERE stock.category = 'general'";

        String full = "SELECT stock.title ,\n" +
                "sum(orders.quantity) ,\n" +
                "sum(orders.totsale)\n" +
                "FROM stock FULL JOIN orders\n" +
                "ON stock.prodcode = orders.prodcode\n" +
                "WHERE stock.category IN ('' , 'general' )\n" +
                "GROUP BY stock.title";


        statement.executeQuery(s);

        try {
            statement.executeQuery(full);
            Assert.fail("FULL JOIN is now working in H2?");
        } catch (SQLException e) {
        }
        ca.close();
    }

    /*@Test(groups = "hsql")*/
    public void unionHsqldb() throws Exception {
        Class.forName("org.hsqldb.jdbcDriver");
        Connection ca = DriverManager.getConnection("jdbc:hsqldb:directoryname/filename", "sa", "");

        Statement statement = ca.createStatement();
        Utils.drop(statement, "DROP TABLE stock");

        Utils.drop(statement, "DROP TABLE orders");

        String s16 = "CREATE table stock (\n" +
                "ISBN char(13),\n" +
                "title char(50),\n" +
                "category char(20),\n" +
                "prodCode char(6),\n" +
                "price numeric)";
        statement.execute(s16);

        String s14 = "CREATE table orders (\n" +
                "prodCode char(6),\n" +
                "ord_date date,\n" +
                "quantity numeric,\n" +
                "totsale numeric,\n" +
                "currency char(3),\n" +
                "delCode char(3),\n" +
                "clientNo numeric,\n" +
                "invoice char(10))";
        statement.execute(s14);

        String s11 = "CREATE table client (\n" +
                "clientNo numeric,\n" +
                "company char(40),\n" +
                "cont_fst char(10),\n" +
                "cont_lst char(15),\n" +
                "dept char(25),\n" +
                "phone char(25),\n" +
                "fax char(25) )";
        statement.execute(s11);

        String s = "SELECT client.company ,\n" +
                "order2.clientno ,\n" +
                "order1.prodcode ,\n" +
                "stock.title\n" +
                "FROM stock JOIN orders AS order1\n" +
                "ON stock.prodcode=order1.prodcode, " +
                "client JOIN orders AS order2\n" +
                "ON order2.clientno = client.clientno\n" +
                "AND order1.prodcode = order2.prodcode\n" +
                "AND order1.clientno = order2.clientno\n" +
                "WHERE stock.category = 'general'";

        statement.executeQuery(s);

        String s1 = "SELECT stock.title ,\n" +
                "sum(orders.quantity) ,\n" +
                "sum(orders.totsale)\n" +
                "FROM stock FULL JOIN orders\n" +
                "ON stock.prodcode = orders.prodcode\n" +
                "WHERE stock.category IN ('' , 'general' )\n" +
                "GROUP BY stock.title";

        statement.executeQuery(s1);
        ca.close();
    }

    /**
     * this test demonstrates that H2 does not support OUTER UNION
     *
     * @throws Exception
     */
    public void where() throws Exception {
        Class.forName("org.h2.Driver");
        Connection ca = DriverManager.getConnection("jdbc:h2:mem:u1");

        Statement statement = ca.createStatement();
        statement.execute("CREATE TABLE A(X FLOAT)");
        statement.execute("CREATE TABLE B(X FLOAT)");
        // Should be legal?
        // see for example: http://www.xlinesoft.com/interactive_sql_tutorial/UNION_and_Outer_Joins.htm
        statement.executeQuery("SELECT * FROM A OUTER UNION SELECT * FROM A");
        statement.executeQuery("(SELECT * FROM A WHERE X BETWEEN 1 AND 2) OUTER UNION (SELECT * FROM A)");
        statement.executeQuery("SELECT * FROM A WHERE X BETWEEN 1 AND 2 OUTER UNION SELECT * FROM A");
        ca.close();
    }
}
