package other;

import com.dullesopen.h2test.TestConfig;
import com.dullesopen.h2test.Utils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.sql.*;

/**
 * https://bitbucket.org/dullesresearch/h2database/issues/7/group-by-column-number
 */
public class GroupByColumnNumberTest {
// ------------------------------ FIELDS ------------------------------

    private Connection ca;
    private Connection oracle;
    private Connection teradata;

    @BeforeClass
    protected void setUp() throws Exception {
        Class.forName("org.h2.Driver");
        ca = DriverManager.getConnection("jdbc:h2:mem:");
        oracle = TestConfig.getOracleConnection();
        teradata = TestConfig.getTeradataConnection();
    }

    @AfterClass
    protected void tearDown() throws Exception {
        ca.close();
        if (oracle != null) {
            oracle.close();
        }
        if (teradata != null) {
            teradata.close();
        }
    }

    /**
     * Oracle does not support group by column number
     */
    @Test
    public void oracle() throws Exception {
        if (TestConfig.ORACLE) {
            try (Statement statement = oracle.createStatement()) {
                Utils.drop(statement, "DROP TABLE A");
                statement.execute("CREATE TABLE A(X INT , Y INT, Z INT)");
                statement.execute("INSERT INTO A VALUES(1,2,3)");
                try {
                    statement.executeQuery("SELECT X, SUM(Y), Z FROM A GROUP BY 1 , 3");
                } catch (SQLException e) {
                    Assert.assertEquals("ORA-00979: not a GROUP BY expression\n", e.getMessage());
                }
            }
        }
    }

    /**
     * Teradata supports group by column number
     */
    @Test
    public void teradata() throws Exception {
        if (TestConfig.TERADATA) {
            try (Statement statement = teradata.createStatement()) {
                Utils.drop(statement, "DROP TABLE A");
                statement.execute("CREATE TABLE A(X INT , Y INT, Z INT)");
                statement.execute("INSERT INTO A VALUES(1,123,2)");
                statement.execute("INSERT INTO A VALUES(1,456,2)");
                statement.execute("INSERT INTO A VALUES(3,789,4)");
                try (ResultSet rs = statement.executeQuery("SELECT X, SUM(Y), Z FROM A GROUP BY 1 , 3 ORDER BY 1")) {
                    rs.next();
                    Assert.assertEquals(rs.getInt(1), 1);
                    Assert.assertEquals(rs.getInt(2), 579);
                    Assert.assertEquals(rs.getInt(3), 2);
                    rs.next();
                    Assert.assertEquals(rs.getInt(1), 3);
                    Assert.assertEquals(rs.getInt(2), 789);
                    Assert.assertEquals(rs.getInt(3), 4);
                    rs.next();
                    Assert.assertFalse(rs.next());
                }
            }
        }
    }
}