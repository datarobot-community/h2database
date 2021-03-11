package org.h2.mode;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.MessageFormat;

import static org.testng.Assert.assertTrue;

/**
 * Mode.doubleNanSameAsNullTest
 */
public class DoubleNanSameAsNullTest {

    private static final String CLASS = DoubleNanSameAsNullTest.class.getName();

    @BeforeClass
    protected void setUp() throws Exception {
        Class.forName("org.h2.Driver");
    }

    @Test
    public void nan() throws Exception {
        try (Connection h2 = DriverManager.getConnection("jdbc:h2:mem:;mode=Carolina")) {
            {
                try (Statement statement = h2.createStatement()) {
                    statement.execute(MessageFormat.format("CREATE ALIAS isnan FOR \"java.lang.Double.isNaN\"", CLASS));

                    String sql = "select isnan(NULL) ";
                    try (ResultSet rs = statement.executeQuery(sql)) {
                        rs.next();
                        assertTrue(rs.getBoolean(1));
                    }
                }
            }
            try (Statement statement = h2.createStatement()) {
                statement.execute(MessageFormat.format("CREATE ALIAS nan FOR \"{0}.nan\"", CLASS));

                String sql = "select nan(10) is null";
                try (ResultSet rs = statement.executeQuery(sql)) {
                    rs.next();
                    assertTrue(rs.getBoolean(1));
                }
            }
        }
    }

    @SuppressWarnings("unused")
    public static double nan(double x) {
        return Double.NaN;
    }

}
