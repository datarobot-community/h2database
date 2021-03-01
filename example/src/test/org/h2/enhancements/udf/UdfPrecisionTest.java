package org.h2.enhancements.udf;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.MessageFormat;

import static org.testng.Assert.assertEquals;

/**
 * @see <a href="UDF precision">https://bitbucket.org/dullesresearch/h2database/issues/2</a>
 */
public class UdfPrecisionTest {

    private static final String MYFUN = MyFun.class.getName();

    private Connection h2;

    @BeforeMethod
    protected void setUp() throws Exception {
        Class.forName("org.h2.Driver");
        h2 = DriverManager.getConnection("jdbc:h2:mem:");
    }

    @AfterMethod
    protected void tearDown() throws Exception {
        h2.close();
    }

    @Test(description = "https://bitbucket.org/dullesresearch/h2database/issues/2")
    public void precision() throws Exception {
        Statement statement = h2.createStatement();
        statement.execute("CREATE TABLE foo(a CHAR(6))");
        statement.execute("INSERT INTO foo values('abcdef')");

        statement.execute(MessageFormat.format("CREATE ALIAS my FOR \"{0}.{1}\" WITH PRECISION \"{2}.{3}\"",
                MYFUN, "fs", MYFUN, "myprecision"));
        statement.execute("CREATE TABLE bar as select my(a) from foo"); // throws exception here

        String sql = "select * from bar";
        ResultSet rs = statement.executeQuery(sql);

        assertEquals(rs.getMetaData().getPrecision(1), 63);
        statement.close();
    }

}
