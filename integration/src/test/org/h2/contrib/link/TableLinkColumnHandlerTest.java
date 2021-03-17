package org.h2.contrib.link;

import org.h2.contrib.external.TestConfig;
import org.h2.contrib.test.Utils;
import org.h2.engine.Session;
import org.h2.engine.SessionInterface;
import org.h2.jdbc.JdbcConnection;
import org.h2.jdbc.JdbcResultSetMetaData;
import org.h2.message.DbException;
import org.h2.table.Column;
import org.h2.value.*;
import org.testng.Assert;
import org.testng.annotations.*;

import java.sql.*;
import java.text.MessageFormat;

/**
 * Miscellaneous test for linked table
 */
public class TableLinkColumnHandlerTest {
    private static final long DAY = (24 * 60 * 60 * 1000);
    // ------------------------------ FIELDS ------------------------------

    private Connection h2;
    private Connection oracle;
    private Statement stat;

// -------------------------- TEST METHODS --------------------------

    @BeforeMethod
    protected void setUp() throws Exception {
        Class.forName("org.h2.Driver");
        h2 = DriverManager.getConnection("jdbc:h2:mem:;MIXED_CASE=true");
        stat = h2.createStatement();
    }

    @AfterMethod
    protected void tearDown() throws Exception {
        h2.close();
    }

    @BeforeClass
    protected void beforeClass() throws Exception {
        oracle = TestConfig.getOracleConnection();
        if (oracle != null) {
            oracle.setAutoCommit(false);
        }
    }

    @AfterClass
    protected void afterClass() throws Exception {
        if (oracle != null) {
            oracle.close();
        }
    }

// -------------------------- OTHER METHODS --------------------------

    @Test
    public void alias() throws Exception {
        stat.execute("CREATE TABLE ONE (First integer, Second integer extension 'aBc')");
        {
            stat.execute("create table two as select one.second extension 'CdE' from one");

            ResultSet rs = stat.executeQuery("select * FROM TWO");
            JdbcResultSetMetaData metaData = rs.getMetaData().unwrap(JdbcResultSetMetaData.class);
            Assert.assertEquals(metaData.getColumnName(1), "Second");
            Assert.assertEquals(metaData.getColumnMetaExtension(1), "aBcCdE");
        }
        {
            stat.execute("create table three as select one.second as SeCoNd extension 'CdE' from one");

            ResultSet rs = stat.executeQuery("select * FROM THREE");
            JdbcResultSetMetaData metaData = rs.getMetaData().unwrap(JdbcResultSetMetaData.class);
            Assert.assertEquals(metaData.getColumnName(1), "SeCoNd");
            Assert.assertEquals(metaData.getColumnMetaExtension(1), "aBcCdE");
        }
    }

    /**
     * confirm that H2 automatically converts datetime to date
     */
    @Test
    public void extension() throws Exception {
        if (TestConfig.ORACLE) {
            Statement ora = oracle.createStatement();
            Utils.drop(ora, "DROP TABLE ONE");
            ora.execute("CREATE TABLE ONE (X NUMBER(10,2), D DATE, F DOUBLE PRECISION )");
            ora.execute("INSERT INTO ONE VALUES(123.45, '07-DEC-2017', 12345.6789)");
            oracle.commit();

            Statement sa = h2.createStatement();
            Utils.drop(sa, "DROP TABLE ONE");

            SessionInterface session = h2.unwrap(JdbcConnection.class).getSession();
            session.setTableLinkColumnHandlerFactory((connection, schema, table) -> new NoDateTableLinkColumnHandler());
            String sql = MessageFormat.format(
                    "CREATE LINKED VIEW W(''oracle.jdbc.driver.OracleDriver'', ''{0}'', ''h2user'', ''h2pass'', ''select * from one'');",
                    TestConfig.ORACLE_URL);
            sa.execute(sql);

            sa.execute("CREATE TABLE TWO AS SELECT * FROM W");

            try (ResultSet rs = sa.executeQuery("select * FROM TWO")) {
                JdbcResultSetMetaData metaData = rs.getMetaData().unwrap(JdbcResultSetMetaData.class);
                Assert.assertEquals("X", metaData.getColumnName(1));
                Assert.assertEquals("abc", metaData.getColumnMetaExtension(1));
                rs.next();
                Assert.assertEquals(rs.getDouble(2), 17507.);
            }
            Utils.drop(ora, "DROP TABLE ONE");
            Utils.drop(sa, "DROP TABLE TWO");
        }
    }

    /**
     * confirm that H2 automatically converts datetime to date
     */
    @Test
    public void insertConversion() throws Exception {
        if (TestConfig.ORACLE) {
            Statement ora = oracle.createStatement();
            Utils.drop(ora, "DROP TABLE ONE");
            ora.execute("CREATE TABLE ONE (F DOUBLE PRECISION , D DATE )");
//        ora.execute("INSERT INTO ONE VALUES(123.45, '07-DEC-2017', 12345.6789)");
            oracle.commit();

            Statement sa = h2.createStatement();
            Utils.drop(sa, "DROP TABLE ONE");

            SessionInterface session = h2.unwrap(JdbcConnection.class).getSession();
            session.setTableLinkColumnHandlerFactory((connection, schema, table) -> new NoDateTableLinkColumnHandler());
            String sql = MessageFormat.format(
                    "CREATE LINKED TABLE T(''oracle.jdbc.driver.OracleDriver'', ''{0}'', ''h2user'', ''h2pass'', ''one'');",
                    TestConfig.ORACLE_URL);
            sa.execute(sql);

            sa.execute("INSERT INTO T VALUES (123,456789)");
            try (ResultSet rs = sa.executeQuery("select * FROM T")) {
                rs.next();
                Assert.assertEquals(rs.getDouble(2), 456787.0);
            }
            Utils.drop(ora, "DROP TABLE ONE");
            Utils.drop(sa, "DROP TABLE TWO");
        }
    }

    @Test
    public void joinWithConversion() throws Exception {
        if (TestConfig.ORACLE) {
            Statement ora = oracle.createStatement();
            Utils.drop(ora, "DROP TABLE ONE");
            Utils.drop(ora, "DROP TABLE TWO");
            ora.execute("CREATE TABLE ONE (D1 DATE , R CHAR(6))");
            ora.execute("INSERT INTO ONE VALUES('07-DEC-2017', 'abc')");
            ora.execute("CREATE TABLE TWO (D2 DATE , S CHAR(6))");
            ora.execute("INSERT INTO TWO VALUES('07-DEC-2017', 'xyz')");
            oracle.commit();

            Statement sa = h2.createStatement();
            Utils.drop(sa, "DROP TABLE T1");
            Utils.drop(sa, "DROP TABLE T2");

            SessionInterface session = h2.unwrap(JdbcConnection.class).getSession();
            session.setTableLinkColumnHandlerFactory((connection, schema, table) -> new NoDateTableLinkColumnHandler());
            sa.execute(MessageFormat.format(
                    "CREATE LINKED TABLE T1(''oracle.jdbc.driver.OracleDriver'', ''{0}'', ''h2user'', ''h2pass'', ''H2USER'', ''ONE'');",
                    TestConfig.ORACLE_URL));
            sa.execute(MessageFormat.format(
                    "CREATE LINKED TABLE T2(''oracle.jdbc.driver.OracleDriver'', ''{0}'', ''h2user'', ''h2pass'', ''H2USER'', ''TWO'');",
                    TestConfig.ORACLE_URL));

            try (ResultSet rs = sa.executeQuery("select * FROM T1 INNER JOIN T2 ON T1.D1=T2.D2")) {
                rs.next();
                Assert.assertEquals(rs.getDouble(1), 17507.);
            }
            Utils.drop(ora, "DROP TABLE ONE");
            Utils.drop(ora, "DROP TABLE TWO");
            Utils.drop(sa, "DROP TABLE T1");
            Utils.drop(sa, "DROP TABLE T2");
        }
    }

    @Test
    public void subquery() throws Exception {
        stat.execute("CREATE TABLE ONE (X NUMBER(10,2) EXTENSION 'AbC' )");
        stat.execute("create table two as select a.x as xx from ( select * from one) a");

        ResultSet rs = stat.executeQuery("select * FROM TWO");
        JdbcResultSetMetaData metaData = rs.getMetaData().unwrap(JdbcResultSetMetaData.class);
        Assert.assertEquals(metaData.getColumnName(1), "xx");
        Assert.assertEquals(metaData.getColumnMetaExtension(1), "AbC");
    }

    @Test
    public void view() throws Exception {
        stat.execute("CREATE TABLE ONE (xX NUMBER(10,2) EXTENSION 'AbC' )");
        {
            stat.execute("create view two as select a.xX as xYz from ( select * from one) a");

            ResultSet rs = stat.executeQuery("select * FROM TWO");
            JdbcResultSetMetaData metaData = rs.getMetaData().unwrap(JdbcResultSetMetaData.class);
            Assert.assertEquals(metaData.getColumnName(1), "xYz");
            Assert.assertEquals(metaData.getColumnMetaExtension(1), "AbC");
        }
        {
            stat.execute("create view three as select * from one");

            ResultSet rs = stat.executeQuery("select * FROM THREE");
            JdbcResultSetMetaData metaData = rs.getMetaData().unwrap(JdbcResultSetMetaData.class);
            Assert.assertEquals(metaData.getColumnName(1), "xX");
            Assert.assertEquals(metaData.getColumnMetaExtension(1), "AbC");
        }
    }

    @Test
    public void view2() throws Exception {
        stat.execute("CREATE TABLE ONE (i integer)");
        {
            stat.execute("create view two as select i extension 'AbC' from one");

            ResultSet rs = stat.executeQuery("select * FROM TWO");
            JdbcResultSetMetaData metaData = rs.getMetaData().unwrap(JdbcResultSetMetaData.class);
            Assert.assertEquals(metaData.getColumnName(1), "i");
            Assert.assertEquals(metaData.getColumnMetaExtension(1), "AbC");
        }
        {
            stat.execute("create view three as select * from two");

            ResultSet rs = stat.executeQuery("select * FROM THREE");
            JdbcResultSetMetaData metaData = rs.getMetaData().unwrap(JdbcResultSetMetaData.class);
            Assert.assertEquals(metaData.getColumnName(1), "i");
            Assert.assertEquals(metaData.getColumnMetaExtension(1), "AbC");
        }
    }

    @Test
    public void view3() throws Exception {
        stat.execute("CREATE TABLE ONE (i integer)");
        {
            stat.execute("create view two as select i as i extension 'AbC' from one");

            ResultSet rs = stat.executeQuery("select * FROM TWO");
            JdbcResultSetMetaData metaData = rs.getMetaData().unwrap(JdbcResultSetMetaData.class);
            Assert.assertEquals(metaData.getColumnName(1), "i");
            Assert.assertEquals(metaData.getColumnMetaExtension(1), "AbC");
        }
        {
            stat.execute("create view three as select two.i as ii from two");

            ResultSet rs = stat.executeQuery("select * FROM THREE");
            JdbcResultSetMetaData metaData = rs.getMetaData().unwrap(JdbcResultSetMetaData.class);
            Assert.assertEquals(metaData.getColumnName(1), "ii");
            Assert.assertEquals(metaData.getColumnMetaExtension(1), "AbC");
        }
    }

    @Test(description = "https://bitbucket.org/dullesopen/h2/issues/3/create-view-column-with-extension-in-group")
    public void viewGroupBy() throws Exception {
        stat.execute("CREATE TABLE ONE (x integer, y integer)");
        stat.execute("create view two as select x extension 'AbC', sum(y) from one group by x");

        ResultSet rs = stat.executeQuery("select * FROM TWO");
        JdbcResultSetMetaData metaData = rs.getMetaData().unwrap(JdbcResultSetMetaData.class);
        Assert.assertEquals(metaData.getColumnName(1), "x");
        Assert.assertEquals(metaData.getColumnMetaExtension(1), "AbC");
    }

    private static class NoDateTableLinkColumnHandler implements TableLinkColumnHandler {
        @SuppressWarnings("SwitchStatementWithTooFewBranches")
        @Override
        public Column createColumn(String name, int sqlType, int type, String typename, long precision, int scale, int displaySize) {
            switch (sqlType) {
                case Types.TIMESTAMP: {
                    Column column = new Column(name, Value.DOUBLE, 20, 20, 20);
                    column.setMetaExtension("date");
                    return column;
                }
                default: {
                    Column column = new Column(name, type, precision, scale, displaySize);
                    column.setMetaExtension("abc");
                    return column;
                }
            }
        }

        @SuppressWarnings("SwitchStatementWithTooFewBranches")
        @Override
        public Value createValue(Session session, ResultSet rs, int columnIndex, int type) {
            try {
                int t = rs.getMetaData().getColumnType(columnIndex);
                switch (t) {
                    case Types.TIMESTAMP: {
                        Date value = rs.getDate(columnIndex);
                        if (value == null) {
                            return ValueNull.INSTANCE;
                        } else {
                            long diff = value.getTime() - Date.valueOf("1970-01-01").getTime();
                            long days = diff / DAY;
                            return ValueDouble.get(days, ValueDate.get(value));
                        }
                    }
                    default:
                        return DataType.readValue(session, rs, columnIndex, type);
                }
            } catch (SQLException e) {
                throw DbException.convert(e);
            }
        }

        @SuppressWarnings("SwitchStatementWithTooFewBranches")
        @Override
        public void bindParameterValue(PreparedStatement prep, Value v, int i, int sqlType, String typeName) throws SQLException {
            switch (sqlType) {
                case Types.TIMESTAMP: {
                    ValueDouble d = (ValueDouble) v;
                    if (d.reference != null) {
                        d.reference.set(prep, i);
                    } else {
                        double days = v.getDouble();
                        ValueDate date = ValueDate.fromMillis((long) (DAY * days));
                        date.set(prep, i);
                    }
                    break;
                }
                default:
                    v.set(prep, i);
            }
        }
    }
}