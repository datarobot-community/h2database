package org.h2.enhancements.udf;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.MessageFormat;

import static org.testng.Assert.assertTrue;

public class AliasTest {

    private static final String CLASS = AliasTest.class.getName();
    private Connection h2;

// -------------------------- TEST METHODS --------------------------

    @BeforeMethod
    protected void setUp() throws Exception {
        Class.forName("org.h2.Driver");
        h2 = DriverManager.getConnection("jdbc:h2:mem:;mode=Carolina");
    }

    @AfterMethod
    protected void tearDown() throws Exception {
        h2.close();
    }

    @Test
    public void nan() throws Exception {
        {
            Statement statement = h2.createStatement();
            statement.execute(MessageFormat.format("CREATE ALIAS isnan FOR \"java.lang.Double.isNaN\"", CLASS));

            String sql = "select isnan(NULL) ";
            ResultSet rs = statement.executeQuery(sql);
            rs.next();
            assertTrue(rs.getBoolean(1));
            statement.close();
        }
        {
            Statement statement = h2.createStatement();
            statement.execute(MessageFormat.format("CREATE ALIAS nan FOR \"{0}.nan\"", CLASS));

            String sql = "select nan(10) is null";
            ResultSet rs = statement.executeQuery(sql);
            rs.next();
            assertTrue(rs.getBoolean(1));
            statement.close();
        }
    }

    @SuppressWarnings("unused")
    public static double nan(double x) {
        return Double.NaN;
    }

}
