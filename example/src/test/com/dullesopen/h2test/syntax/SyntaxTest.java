package com.dullesopen.h2test.syntax;

import com.dullesopen.h2test.TestConfig;
import com.dullesopen.h2test.Utils;
import org.h2.api.ErrorCode;
import org.testng.Assert;
import org.testng.annotations.*;

import java.sql.*;
import java.text.MessageFormat;

import static org.testng.Assert.assertEquals;


public class SyntaxTest {
    // ------------------------------ FIELDS ------------------------------

    private Connection h2;
    private Connection oracle;
    private Connection sqlsvr;
    private Connection teradata;

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

    @AfterClass
    protected void afterClass() throws Exception {
        if (oracle != null) {
            oracle.close();
        }
        if (sqlsvr != null) {
            sqlsvr.close();
        }
        if (teradata != null) {
            teradata.close();
        }
    }

    @BeforeClass
    protected void beforeClass() throws Exception {
        oracle = TestConfig.getOracleConnection();
        teradata = TestConfig.getTeradataConnection();
        if (TestConfig.MSSQL) {
            String driver = "net.sourceforge.jtds.jdbc.Driver";
            Class.forName(driver);

            String user = "user";
            String password = "password";
            String url = "jdbc:jtds:sqlserver://localhost";

            sqlsvr = DriverManager.getConnection(url, user, password);
        }
    }

// -------------------------- OTHER METHODS --------------------------

    @Test
    public void dropColumnH2() throws Exception {
        Statement sa = h2.createStatement();
        Utils.drop(sa, "DROP TABLE A");
        sa.execute("CREATE TABLE A(X INT, Y INT)");
        sa.execute("ALTER TABLE A DROP COLUMN Y");
        sa.execute("DROP TABLE A");
    }

    @Test
    public void whereColumnAlias() throws Exception {
        Statement sa = h2.createStatement();
        Utils.drop(sa, "DROP TABLE T");
        sa.execute("CREATE TABLE T(X INT, Y INT)");
        try {
            sa.execute("SELECT T.X AS B FROM T WHERE B=0");
            Assert.fail();
        } catch (SQLException e) {
            assertEquals(e.getErrorCode(), ErrorCode.COLUMN_NOT_FOUND_1);
            assertEquals(Utils.truncate(e), "Column \"B\" not found");
        }
        sa.execute("DROP TABLE T");
    }

    @Test
    public void whereColumnAliasOracle() throws Exception {
        if (TestConfig.ORACLE) {
            Statement sa = oracle.createStatement();
            Utils.drop(sa, "DROP TABLE T");
            sa.execute("CREATE TABLE T(X INT, Y INT)");
            try {
                sa.executeQuery("SELECT T.X AS B FROM T WHERE B=0");
                Assert.fail();
            } catch (SQLException e) {
                Assert.assertEquals(e.getErrorCode(), 904);
                Assert.assertEquals(e.getMessage().trim(), "ORA-00904: \"B\": invalid identifier");
            }
            sa.execute("DROP TABLE T");
        }
    }


    @Test
    public void whereColumnAliasTeradata() throws Exception {
        if (TestConfig.TERADATA) {
            Statement sa = teradata.createStatement();
            Utils.drop(sa, "DROP TABLE T");
            sa.execute("CREATE TABLE T(X INT)");
            sa.execute("INSERT INTO T VALUES(12)");
            ResultSet rs = sa.executeQuery("SELECT T.X AS B FROM T WHERE B=12");
            rs.next();
            rs.close();
            Assert.assertEquals(rs.getInt(1), 12);
            sa.execute("DROP TABLE T");
            sa.close();
        }
    }


    @Test
    public void dropColumnOralce() throws Exception {
        if (TestConfig.ORACLE) {
            Statement sa = oracle.createStatement();
            Utils.drop(sa, "DROP TABLE A");
            sa.execute("CREATE TABLE A(X INT, Y INT)");
            sa.executeQuery("ALTER TABLE A DROP COLUMN Y");
            sa.execute("DROP TABLE A");
        }
    }

    /**
     * @throws Exception
     */
    @Test
    public void groupByNumber() throws Exception {
        Statement sa = h2.createStatement();
        sa.execute("CREATE TABLE A(X NUMBER, Y NUMBER)");
        sa.execute("INSERT INTO  A VALUES (10, 20);");
        ResultSet rs = sa.executeQuery("SELECT X + Y AS Z FROM A  GROUP BY Z");
        rs.next();
        Assert.assertEquals(rs.getDouble(1), 30.0);

        sa.execute("DROP TABLE A");
    }

    @Test
    public void indexOracle() throws Exception {
        if (TestConfig.ORACLE) {
            Statement statement = oracle.createStatement();

            drop(statement, "ONE");
            statement.execute("CREATE TABLE ONE (A INT, B INT)");
            statement.execute("CREATE INDEX X ON  ONE(A)");
            // oracle DOES NOT REQUIRES table name for index
            statement.execute("DROP INDEX X");
            statement.execute("DROP TABLE ONE");

            statement.close();
        }
    }

    private void drop(Statement sa, String name) {
        try {
            sa.execute("DROP TABLE " + name);
        } catch (SQLException e) {
        }
    }

    @Test
    public void indexSqlsvr() throws Exception {
        if (TestConfig.MSSQL) {
            Statement statement = sqlsvr.createStatement();

            drop(statement, "ONE");
            statement.execute("CREATE TABLE ONE (A INT, B INT)");
            statement.execute("CREATE INDEX X ON  ONE(A)");
            // sql server REQUIRES table name for index
            statement.execute("DROP INDEX ONE.X");
            statement.execute("DROP TABLE ONE");

            statement.close();
        }
    }

    @Test
    public void indexTeradata() throws Exception {
        if (TestConfig.TERADATA) {
            Statement statement = teradata.createStatement();

            drop(statement, "ONE");
            statement.execute("CREATE TABLE ONE (A INT, B INT)");
            // In Teradata you can not create index for a single column because it seems Teradata does it by default,
            // at least two columns should be specified
            statement.execute("CREATE INDEX X (A,B) ON ONE");
            // Teradata  DOES REQUIRES table name for index
            statement.execute("DROP INDEX X ON ONE");
            statement.execute("DROP TABLE ONE");

            statement.close();
        }
    }

    /**
     *
     */
    @Test
    public void minAsRegularFunctionH2() throws Exception {
        Statement sa = h2.createStatement();
        sa.execute("CREATE TABLE ONE (X INT, Y INT)");
        ResultSet rs = sa.executeQuery(
                "select MIN(X), Y FROM ONE GROUP BY Y");
        rs.next();

        try {
            sa.executeQuery("select MIN(X,Y) FROM ONE");
            Assert.fail();
        } catch (SQLException e) {
            Assert.assertEquals(e.getErrorCode(), ErrorCode.SYNTAX_ERROR_2);
            Assert.assertEquals(Utils.truncate(e),
                    "Syntax error in SQL statement \"SELECT MIN(X,[*]Y) FROM ONE \"; expected \"., (, [, ::, *, /, %, +, -, ||, ~, !~, NOT, LIKE, ILIKE, REGEXP, IS, IN, BETWEEN, AND, OR, )\"");
        }
        sa.execute("DROP TABLE ONE");
    }

    /**
     *
     */
    @Test
    public void minAsRegularFunctionOracle() throws Exception {
        if (TestConfig.ORACLE) {
            Statement sa = oracle.createStatement();
            drop(sa, "DROP TABLE ONE");
            sa.execute("CREATE TABLE ONE (X INT, Y INT)");
            ResultSet rs = sa.executeQuery(
                    "select MIN(X), Y FROM ONE GROUP BY Y");
            rs.next();
            try {
                sa.executeQuery("select MIN(X,Y) FROM ONE");
                Assert.fail();
            } catch (SQLException e) {
                Assert.assertEquals(e.getErrorCode(), 909);
                Assert.assertEquals(e.getMessage().trim(), "ORA-00909: invalid number of arguments");
            }
            sa.execute("DROP TABLE ONE");
        }
    }

    /**
     *
     */
    @Test
    public void minAsRegularFunctionSqlsvr() throws Exception {
        if (TestConfig.MSSQL) {
            Statement sa = sqlsvr.createStatement();
            drop(sa, "DROP TABLE ONE");
            sa.execute("CREATE TABLE ONE (X INT, Y INT)");
            ResultSet rs = sa.executeQuery(
                    "select MIN(X), Y FROM ONE GROUP BY Y");
            rs.next();
            rs = sa.executeQuery(
                    "select MIN(X,Y) FROM ONE");
            rs.next();
            sa.execute("DROP TABLE ONE");
        }
    }

    /**
     *
     */
    @Test
    public void missingColumnH2() throws Exception {
        Statement sa = h2.createStatement();
        sa.execute("CREATE TABLE SALES (COMPANY VARCHAR(10), COST NUMBER)");
        String sql = "    select company\n" +
                "from ( select company, sum(cost) as totcost\n" +
                "from sales\n" +
                "group by company\n" +
                ") having totcost = max(totcost)";
        ResultSet rs = null;
        try {
            sa.executeQuery(sql);
            Assert.fail();
        } catch (SQLException e) {
            Assert.assertEquals(e.getErrorCode(), ErrorCode.MUST_GROUP_BY_COLUMN_1);
            Assert.assertEquals(Utils.truncate(e), "Column \"COMPANY\" must be in the GROUP BY list");
        }

        sa.execute("DROP TABLE SALES");
    }

    /**
     * demonstrates the error message generated by Oracle
     */
    @Test
    public void missingColumnOracle() throws Exception {
        if (TestConfig.ORACLE) {
            Statement sa = oracle.createStatement();
            Utils.drop(sa, "DROP TABLE SALES");
            sa.execute("CREATE TABLE SALES (COMPANY VARCHAR(10), COST NUMBER)");
            String sql = "    select company\n" +
                    "from ( select company, sum(cost) as totcost\n" +
                    "from sales\n" +
                    "group by company\n" +
                    ") having totcost = max(totcost)";
            try {
                sa.executeQuery(sql);
                Assert.fail();
            } catch (SQLException e) {
                Assert.assertEquals(e.getErrorCode(), 979);
                Assert.assertEquals(e.getMessage().trim(), "ORA-00979: not a GROUP BY expression");
            }


            sa.execute("DROP TABLE SALES");
        }
    }

    /**
     * priority on NOT operator when used as part of combined expression NOT EXISTS
     */
    @Test
    public void notExists() throws Exception {
        if (TestConfig.ORACLE) {
            Statement sa = oracle.createStatement();
            Utils.drop(sa, "DROP TABLE ONE");
            Utils.drop(sa, "DROP TABLE TWO");
            sa.execute("CREATE TABLE ONE (X INT)");
            sa.execute("INSERT INTO ONE VALUES (10)");
            sa.execute("INSERT INTO ONE VALUES (20)");
            sa.execute("CREATE TABLE TWO (Z INT)");
            sa.execute("INSERT INTO  TWO VALUES (1)");
            sa.execute("INSERT INTO  TWO VALUES (2)");
            ResultSet rs = sa.executeQuery(
                    "SELECT X FROM ONE WHERE 1=1 AND NOT EXISTS ( SELECT Z FROM TWO WHERE Z=1 ) OR EXISTS ( SELECT Z FROM TWO  WHERE Z=2)");
            rs.next();
            Assert.assertEquals(rs.getDouble(1), 10.0);
            rs.next();
            Assert.assertEquals(rs.getDouble(1), 20.0);
            Assert.assertFalse(rs.next());

            rs = sa.executeQuery(
                    "SELECT X FROM ONE WHERE 1=1 AND NOT ( EXISTS ( SELECT Z FROM TWO WHERE Z=1 ) OR EXISTS ( SELECT Z FROM TWO  WHERE Z=2) )");
            Assert.assertFalse(rs.next());

            rs = sa.executeQuery(
                    "SELECT X FROM ONE WHERE NOT X=10 OR X=20");
            rs.next();
            Assert.assertEquals(rs.getDouble(1), 20.0);
            Assert.assertFalse(rs.next());

            rs = sa.executeQuery(
                    "SELECT X FROM ONE WHERE ( NOT X=10 ) OR X=20 ");
            rs.next();
            Assert.assertEquals(rs.getDouble(1), 20.0);
            Assert.assertFalse(rs.next());

            rs = sa.executeQuery(
                    "SELECT X FROM ONE WHERE NOT (X=10 OR X=20 )");
            Assert.assertFalse(rs.next());


            sa.execute("DROP TABLE ONE");
            sa.execute("DROP TABLE TWO");
        }
    }

    @Test
    public void quotedIdentifier() throws Exception {
        if (TestConfig.ORACLE) {
            Statement sa = oracle.createStatement();
            Utils.drop(sa, "DROP TABLE A");
            sa.execute("CREATE TABLE A(X INT)");
            ResultSet rs = null;
            try {
                sa.executeQuery("SELECT * FROM \"a\"");
                Assert.fail("Oracle uses upper case for internal representation");
            } catch (SQLException e) {
            }
            rs = sa.executeQuery("SELECT * FROM \"A\"");
            rs.next();
            sa.execute("DROP TABLE A");
        }
    }

    /**
     * convert boolean expression to number by using iff function.
     * TODO: investigate using CASEWHEN function
     *
     * @throws Exception
     */
    @Test
    public void testAvgBooleanWorkAround() throws Exception {
        Statement sa = h2.createStatement();
        sa.execute("CREATE TABLE A(X NUMBER, Y NUMBER)");
        String create = MessageFormat.format("CREATE ALIAS {0} FOR \"{1}.{2}\"",
                "ifn", Functions.class.getName(), "iff");

        sa.execute(create);
        sa.executeQuery("SELECT SUM(ifn(X=0,1,0)) FROM A GROUP BY Y");
        sa.execute("DROP TABLE A");
    }

    @Test
    public void testComplexConstraint() throws Exception {
        Statement sa = h2.createStatement();
        sa.execute("CREATE TABLE A(X INT, Y INT, CONSTRAINT XX CHECK ( X * Y > 0))"); // OK
        sa.execute("CREATE TABLE B(X INT, Y INT CONSTRAINT XX CHECK ( Y * Y > 0))"); // OK
        try {
            sa.execute("CREATE TABLE C(X INT, Y INT CONSTRAINT XX CHECK ( X * Y > 0))"); // FAIL
            //TODO. reflect in the documentation then we do not support these contraints
            Assert.fail();
        } catch (SQLException e) {
        }
        sa.execute("DROP TABLE A");
        sa.execute("DROP TABLE B");
    }

    /**
     * aggregate functions sum can be called on boolean expression.
     *
     * @throws Exception
     */
    @Test
    public void sumBoolean() throws Exception {
        Statement sa = h2.createStatement();
        sa.execute("CREATE TABLE A(X NUMBER, Y CHAR)");
        sa.executeQuery("SELECT SUM(X BETWEEN 10 AND 20) FROM A GROUP BY Y");
        sa.execute("DROP TABLE A");
    }

    @Test
    public void complexSum() throws Exception {
        Statement sa = h2.createStatement();
        sa.execute("CREATE TABLE A(X NUMBER, Y CHAR)");
        sa.executeQuery("SELECT SUM(X BETWEEN 10 AND 20) FROM A GROUP BY Y");
        sa.executeQuery("SELECT SUM(SIN(X)) FROM A GROUP BY Y");
        sa.execute("DROP TABLE A");
    }

    @Test
    public void testCreate() throws Exception {
        Statement sa = h2.createStatement();
        sa.execute("CREATE TABLE A(X CHAR)");
        sa.execute("SELECT X FROM A GROUP BY  SUBSTR(X,1)");
        sa.execute("SELECT X FROM A GROUP BY  SUBSTRING(X,1)");
        sa.execute("CREATE TABLE B(Y INT)");
        sa.execute("SELECT Y FROM B GROUP BY  Y+Y");
        sa.execute("DROP TABLE B");
        sa.execute("DROP TABLE A");
    }

    @Test
    public void testFunction() throws Exception {
        Statement sa = h2.createStatement();
        sa.execute("CREATE TABLE A(X INT)");
        sa.execute("INSERT INTO A VALUES(1.1)");
        ResultSet rs = sa.executeQuery("SELECT * FROM A;");
        rs.next();
        assertEquals(rs.getDouble(1), 1.0);
        sa.execute("DROP TABLE A");
    }

    @Test
    public void testGroupByAlias() throws Exception {
        Statement sa = h2.createStatement();
        sa.execute("CREATE TABLE A(X INT)");
        ResultSet rs = null;
        sa.executeQuery("SELECT 2*X AS Y FROM A GROUP BY Y;");
        sa.execute("DROP TABLE A");
    }

    @Test
    public void testHavingAlias() throws Exception {
        Statement sa = h2.createStatement();
        sa.execute("CREATE TABLE A(X INT)");
        try {
            sa.executeQuery("SELECT 2*X AS Y FROM A HAVING Y>0;");
            Assert.fail();
        } catch (SQLException e) {
        }
        sa.execute("DROP TABLE A");
    }

    @Test
    public void testHavingAvg() throws Exception {
        Statement sa = h2.createStatement();
        sa.execute("CREATE TABLE A(X INT)");
        ResultSet rs = sa.executeQuery("SELECT X FROM A GROUP BY X HAVING X=AVG(X);");
        rs.next();
        sa.execute("DROP TABLE A");
    }

    @Test
    public void testHavingAvgOracle() throws Exception {
        if (TestConfig.ORACLE) {
            Statement sa = oracle.createStatement();
            Utils.drop(sa, "DROP TABLE A");
            sa.execute("CREATE TABLE A(X INT)");
            ResultSet rs = sa.executeQuery("SELECT X FROM A GROUP BY X HAVING X=AVG(X)");
            rs.next();
            sa.execute("DROP TABLE A");
        }
    }

    @Test
    public void testIntersect() throws Exception {
        Statement sa = h2.createStatement();
        sa.execute("CREATE TABLE A(X INT)");
        sa.execute("CREATE TABLE B(X INT)");
        sa.execute("CREATE TABLE C(X INT)");
        ResultSet rs = sa.executeQuery("SELECT * FROM A INTERSECT SELECT * FROM B UNION SELECT * FROM C;");
        rs.next();
        sa.execute("DROP TABLE A");
        sa.execute("DROP TABLE B");
        sa.execute("DROP TABLE C");
    }

    @Test
    public void testLeftJoinH2() throws Exception {
        Statement sa = h2.createStatement();
        drop(sa, "L");
        drop(sa, "R");
        sa.execute("CREATE TABLE L (ID INT, B INT)");
        sa.execute("CREATE TABLE R (ID INT, C INT)");
        sa.execute("INSERT INTO L VALUES (10,20)");
        sa.execute("INSERT INTO R VALUES (10,30)");

        {
            ResultSet rs = sa.executeQuery("select L.* from L LEFT JOIN R on L.id = R.id where L.id = R.id");
            rs.next();
            assertEquals(rs.getInt(1), 10);
        }
        {
            ResultSet rs = sa.executeQuery("select L.* from L LEFT OUTER JOIN R on L.id = R.id where L.id = R.id");
            rs.next();
            assertEquals(rs.getInt(1), 10);
        }
        sa.execute("DROP TABLE L");
        sa.execute("DROP TABLE R");
    }

    @Test
    public void testLeftJoinOracle() throws Exception {
        if (TestConfig.ORACLE) {
            Statement sa = oracle.createStatement();
            drop(sa, "L");
            Utils.drop(sa, "DROP TABLE R");
            sa.execute("CREATE TABLE L (ID INT, B INT)");
            sa.execute("CREATE TABLE R (ID INT, C INT)");
            sa.execute("INSERT INTO L VALUES (10,20)");
            sa.execute("INSERT INTO R VALUES (10,30)");
            {
                ResultSet rs = sa.executeQuery("select L.* from L LEFT OUTER JOIN R on L.id = R.id where L.id = R.id");
                rs.next();
                assertEquals(rs.getInt(1), 10);
            }
            {
                ResultSet rs = sa.executeQuery("select L.* from L LEFT JOIN R on L.id = R.id where L.id = R.id");
                rs.next();
                assertEquals(rs.getInt(1), 10);
            }
            sa.execute("DROP TABLE L");
            sa.execute("DROP TABLE R");
        }
    }

    @Test
    public void testLeftJoinTeradata() throws Exception {
        if (TestConfig.TERADATA) {
            Statement sa = teradata.createStatement();
            drop(sa, "L");
            Utils.drop(sa, "DROP TABLE R");
            sa.execute("CREATE TABLE L (ID INT, B INT)");
            sa.execute("CREATE TABLE R (ID INT, C INT)");
            sa.execute("INSERT INTO L VALUES (10,20)");
            sa.execute("INSERT INTO R VALUES (10,30)");

            {
                ResultSet rs = sa.executeQuery("select L.* from L LEFT OUTER JOIN R on L.id = R.id where L.id = R.id");
                rs.next();
                assertEquals(rs.getInt(1), 10);
            }
            {
                ResultSet rs = sa.executeQuery("select L.* from L LEFT JOIN R on L.id = R.id where L.id = R.id");
                rs.next();
                assertEquals(rs.getInt(1), 10);
            }
            sa.execute("DROP TABLE L");
            sa.execute("DROP TABLE R");
        }
    }

    @Test
    public void testSelectStae() throws Exception {
        Statement sa = h2.createStatement();
        sa.execute("CREATE TABLE A(X NUMBER, Y CHAR)");
        {
            ResultSet rs = sa.executeQuery("SELECT *,SUM(X) FROM A GROUP BY Y");
            ResultSetMetaData meta = rs.getMetaData();
            assertEquals(meta.getColumnCount(), 3);
            assertEquals(meta.getColumnName(1), "X");
            assertEquals(meta.getColumnName(2), "Y");
            assertEquals(meta.getColumnName(3), "SUM(X)");
        }
        {
            ResultSet rs = sa.executeQuery("SELECT X,Y,SUM(X) FROM A GROUP BY Y");
            ResultSetMetaData meta = rs.getMetaData();
            assertEquals(meta.getColumnCount(), 3);
            assertEquals(meta.getColumnName(1), "X");
            assertEquals(meta.getColumnName(2), "Y");
            assertEquals(meta.getColumnName(3), "SUM(X)");
        }

        sa.execute("DROP TABLE A");
    }

    /**
     * priority on NOT operator when used as part of combined expression NOT EXISTS
     */
    @Test
    public void update() throws Exception {
        Statement sa = h2.createStatement();
        Utils.drop(sa, "DROP TABLE ONE");
        Utils.drop(sa, "DROP TABLE TWO");
        sa.execute("CREATE TABLE ONE (VAR INT, ID CHAR (8),  dt DATE)");
        sa.execute("CREATE TABLE TWO (VAR INT, ID CHAR (8))");

        sa.execute(
                "   update one a\n" +
                        "      set VAR=\n" +
                        "      (\n" +
                        "         select max(b.VAR) as max_var\n" +
                        "         from two b\n" +
                        "         where a.id=b.id\n" +
                        "      )\n" +
                        "      where a.id is not NULL and dt=today()");
        sa.execute("DROP TABLE ONE");
        sa.execute("DROP TABLE TWO");
    }

    /**
     * priority on NOT operator when used as part of combined expression NOT EXISTS
     */
    @Test
    public void updateCount() throws Exception {
        if (TestConfig.ORACLE) {
            Statement sa = oracle.createStatement();
            drop(sa, "ONE");
            drop(sa, "TWO");
            sa.execute("CREATE TABLE ONE (X INT)");
            sa.execute("INSERT INTO ONE VALUES (1)");

            sa.execute("CREATE TABLE TWO AS SELECT * FROM ONE");
            int rc = sa.getUpdateCount();
            sa.execute("DROP TABLE ONE");
            sa.execute("DROP TABLE TWO");
        }
    }

    @Test
    public void updateTwoColumns() throws Exception {
        Statement sa = h2.createStatement();
        drop(sa, "DROP TABLE ONE");
        sa.execute("CREATE TABLE ONE (X INT, Y INT)");
        sa.execute("update one a set X=1, Y=2");
        sa.execute("DROP TABLE ONE");
    }

    @Test
    public void unionOrderByConstant() throws Exception {

        Statement statement = h2.createStatement();
        Utils.drop(statement, "drop TABLE tab");
        statement.execute("CREATE TABLE tab (x INTEGER)");
        statement.executeQuery("SELECT x AS xx from tab order by x");
        statement.executeQuery("SELECT x AS xx from tab order by 1");
        statement.executeQuery("SELECT x AS xx from tab union SELECT x AS xx from tab order by 1");

        try {
            statement.executeQuery("SELECT x AS xx from tab order by 2");
            Assert.fail();
        } catch (SQLException e) {
            Assert.assertEquals(e.getErrorCode(), ErrorCode.ORDER_BY_NOT_IN_RESULT);
            Assert.assertEquals(Utils.truncate(e), "Order by expression \"2\" must be in the result list in this case");
        }
        try {
            statement.executeQuery("SELECT x AS xx from tab union SELECT x AS xx from tab order by 2");
            Assert.fail();
        } catch (SQLException e) {
            Assert.assertEquals(e.getErrorCode(), ErrorCode.ORDER_BY_NOT_IN_RESULT);
            Assert.assertEquals(Utils.truncate(e), "Order by expression \"2\" must be in the result list in this case");
        }
    }

    @Test
    public void distinctOrderBy() throws Exception {

        Statement stat = h2.createStatement();
        Utils.drop(stat, "drop TABLE one");

        stat.execute("CREATE TABLE one (x INTEGER, y integer)");
        stat.executeQuery("SELECT X FROM ONE GROUP BY X,Y ORDER BY Y");
        try {
            stat.executeQuery("SELECT DISTINCT X FROM ONE GROUP BY X,Y ORDER BY Y");
            Assert.fail();
        } catch (SQLException e) {
            Assert.assertEquals(e.getErrorCode(), ErrorCode.ORDER_BY_NOT_IN_RESULT);
            Assert.assertEquals(Utils.truncate(e), "Order by expression \"Y\" must be in the result list in this case");
        }

    }


// -------------------------- INNER CLASSES --------------------------

    static class Functions {
        public static double iff(double c, double t, double f) {
            return c == 0 ? t : f;
        }
    }
}
