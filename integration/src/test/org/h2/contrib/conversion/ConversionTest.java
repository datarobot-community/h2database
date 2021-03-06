package org.h2.contrib.conversion;

import org.h2.api.ErrorCode;
import org.h2.contrib.test.Utils;
import org.h2.value.*;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.sql.*;
import java.text.MessageFormat;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;


public class ConversionTest {
// ------------------------------ FIELDS ------------------------------

    private Connection h2;

    private static String CLASS = ConversionTest.class.getName();

    // -------------------------- TEST METHODS --------------------------
    @BeforeSuite
    static public void beforeSuite() {
        System.getProperties().put("h2.doubleNanSameAsNull", "true");
    }


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
    public void conversion() throws Exception {
        try (Statement sa = h2.createStatement()) {
            sa.execute("CREATE TABLE A (A INT);");
            try {
                sa.execute("INSERT INTO A VALUES ('10K');");
                fail();
            } catch (SQLException e) {
                assertEquals(e.getErrorCode(), ErrorCode.DATA_CONVERSION_ERROR_1);
                Assert.assertEquals(Utils.truncate(e), "Data conversion error converting \"'10K' (A: A INT)\"");
            }
            // customize database
            String sql = MessageFormat.format(
                    "CREATE CONVERSION CHARACTER VARYING, INT,  ''{0}$My'';",
                    CLASS);
            sa.execute(sql);
            // now it runs ok
            sa.execute("INSERT INTO A VALUES ('10K');");
            try (ResultSet rs = sa.executeQuery("SELECT * FROM A")) {
                rs.next();
                assertEquals(rs.getInt(1), 10000);
            }
        }
    }

    @Test
    public void doubleNanToString() throws Exception {
        try (Statement sa = h2.createStatement()) {
            sa.execute("CREATE TABLE tab (abc DOUBLE);");
            sa.execute("INSERT INTO tab VALUES (NULL);");
            sa.execute("INSERT INTO tab VALUES (123.456);");
            String conversion = MessageFormat.format("CREATE CONVERSION DOUBLE , CHARACTER VARYING,  ''{0}$DoubleToDot'';", CLASS);
            sa.execute(conversion);
            String alias = MessageFormat.format("CREATE ALIAS cats FOR \"{0}.cats\"", CLASS);
            sa.execute(alias);
            ResultSet a = sa.executeQuery("SELECT cats('klm',abc,'xyz') from tab");
            a.next();
            assertEquals(a.getString(1), "klm.xyz");
            a.next();
            assertEquals(a.getString(1), "klm123.456xyz");
        }
    }


// -------------------------- INNER CLASSES --------------------------

    public static class My implements Value.Convert {
        public Value convertTo(Value from, int to) {
            if (from.getType() != Value.STRING)
                throw new IllegalArgumentException("unexpected type");
            String s = from.getString();
            if (s.endsWith("K")) {
                int i = Integer.parseInt(s.substring(0, s.length() - 1)) * 1000;
                return ValueInt.get(i);
            } else {
                int i = Integer.parseInt(s);
                return ValueInt.get(i);
            }
        }
    }

    @SuppressWarnings("unused")
    public static class DoubleToDot implements Value.Convert {
        public Value convertTo(Value from, int to) {
            if (to != Value.STRING) {
                throw new IllegalArgumentException("unexpected type");
            } else if (from == ValueNull.INSTANCE) {
                return ValueString.get(".");
            } else if (from.getType() == Value.DOUBLE) {
                double d = from.getDouble();
                return ValueString.get(Double.toString(d));
            } else {
                return ValueString.get(from.getString());
            }
        }
    }

    @SuppressWarnings("unused")
    public static String cats(String... args) {
        StringBuilder buf = new StringBuilder();
        for (String arg : args) {
            buf.append(arg);
        }
        return buf.toString();
    }

}
