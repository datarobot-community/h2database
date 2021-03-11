package other.mode;

import com.dullesopen.h2test.TestConfig;
import com.dullesopen.h2test.Utils;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

public class AggregateIgnoreNanInfiniteTest {

    @Test
    public void sumWithMissingOracleBinaryFloat() throws Exception {
        if (TestConfig.ORACLE) {
            try (Connection ora = TestConfig.getOracleConnection()) {
                try (Statement statement = ora.createStatement()) {
                    Utils.drop(statement, "DROP TABLE FOO");
                    statement.execute("create table FOO as select TO_BINARY_FLOAT(0) X from DUAL");
                    statement.execute("insert into FOO values(1.0)");
                    statement.execute("insert into FOO values(2.718281828459045235)");
                    try (ResultSet rs = statement.executeQuery("select sum(ln(X)) from FOO")) {
                        rs.next();
                        Assert.assertEquals(rs.getDouble(1), Double.NEGATIVE_INFINITY);
                    }
                    statement.executeUpdate("insert into FOO values( to_binary_float('INF'))");
                    try (ResultSet rs = statement.executeQuery("select sum(X) from FOO")) {
                        rs.next();
                        Assert.assertEquals(rs.getDouble(1), Double.POSITIVE_INFINITY);
                    }
                }
            }
        }
    }
}