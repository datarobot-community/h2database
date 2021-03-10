package other;

import com.dullesopen.h2test.TestConfig;
import com.dullesopen.h2test.Utils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.sql.*;

/**
 * https://bitbucket.org/dullesresearch/h2database/issues/3/lateral-column-alias
 */
public class LateralAliasTest {
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
     * Oracle does not support lateral column alias
     */
    @Test
    public void oracle() throws Exception {
        if (TestConfig.ORACLE) {
            try (Statement statement = oracle.createStatement()) {
                Utils.drop(statement, "DROP TABLE A");
                statement.execute("CREATE TABLE A(X INT , Y INT)");
                statement.execute("INSERT INTO A VALUES(1,2)");
                try {
                    statement.executeQuery("SELECT X + Y AS Z, 2 * Z FROM A");
                } catch (SQLException e) {
                    Assert.assertEquals("ORA-00904: \"Z\": invalid identifier\n", e.getMessage());
                }
                try {
                    statement.executeQuery("SELECT X + Y AS Z FROM A WHERE Z > 0");
                } catch (SQLException e) {
                    Assert.assertEquals("ORA-00904: \"Z\": invalid identifier\n", e.getMessage());
                }
            }
        }
    }

    /**
     * Teradata supports lateral column alias without requiring any additional keyword
     */
    @Test
    public void teradata() throws Exception {
        if (TestConfig.TERADATA) {
            try (Statement statement = teradata.createStatement()) {
                Utils.drop(statement, "DROP TABLE A");
                statement.execute("CREATE TABLE A(X INT , Y INT)");
                statement.execute("INSERT INTO A VALUES(1,2)");
                try (ResultSet rs = statement.executeQuery("SELECT X + Y AS Z, 2 * Z FROM A")) {
                    rs.next();
                    Assert.assertEquals(rs.getInt(1), 3);
                }
                try (ResultSet rs = statement.executeQuery("SELECT X + Y AS Y, 2 * Y FROM A")) {
                    rs.next();
                    Assert.assertEquals(rs.getInt(1), 3);
                    Assert.assertEquals(rs.getInt(2), 4);
                }

                try (ResultSet rs = statement.executeQuery("SELECT X + Y AS Z FROM A WHERE Z>0")) {
                    rs.next();
                    Assert.assertEquals(rs.getInt(1), 3);
                }
            }
        }
    }
}