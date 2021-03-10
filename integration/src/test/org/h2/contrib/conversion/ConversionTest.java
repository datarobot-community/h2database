package org.h2.contrib.conversion;

import org.h2.contrib.UdfArgumentConverter;
import org.h2.jdbc.JdbcConnection;
import org.h2.value.Value;
import org.h2.value.ValueNull;
import org.h2.value.ValueString;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.MessageFormat;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;


public class ConversionTest {
// ------------------------------ FIELDS ------------------------------

    private JdbcConnection h2;

    private static String CLASS = ConversionTest.class.getName();

    // -------------------------- TEST METHODS --------------------------
    @BeforeSuite
    static public void beforeSuite() {
        System.getProperties().put("h2.doubleNanSameAsNull", "true");
    }


    @BeforeMethod
    protected void setUp() throws Exception {
        Class.forName("org.h2.Driver");
        h2 = (JdbcConnection) DriverManager.getConnection("jdbc:h2:mem:");
        h2.setUdfArgumentConverter(new DoubleToDot());
    }

    @AfterMethod
    protected void tearDown() throws Exception {
        h2.close();
    }

// -------------------------- OTHER METHODS --------------------------

    @Test
    public void doubleNanToString() throws Exception {
        try (Statement sa = h2.createStatement()) {
            sa.execute("CREATE TABLE tab (abc DOUBLE);");
            sa.execute("INSERT INTO tab VALUES (NULL);");
            sa.execute("INSERT INTO tab VALUES (123.);");
            sa.execute("INSERT INTO tab VALUES (123.456);");
            String alias = MessageFormat.format("CREATE ALIAS cats FOR \"{0}.cats\"", CLASS);
            sa.execute(alias);
            ResultSet a = sa.executeQuery("SELECT cats('klm',abc,'xyz') from tab");
            a.next();
            assertEquals(a.getString(1), "klm.xyz");
            a.next();
            assertEquals(a.getString(1), "klm123xyz");
            a.next();
            assertEquals(a.getString(1), "klm123.456xyz");
        }
    }


    /**
     * string concatenation is not using User defined conversiona
     *
     * @throws Exception
     */
    @Test
    public void concatenate() throws Exception {
        try (Statement sa = h2.createStatement()) {
            sa.execute("CREATE TABLE tab (abc DOUBLE);");
            sa.execute("INSERT INTO tab VALUES (NULL);");
            sa.execute("INSERT INTO tab VALUES (123);");
            sa.execute("INSERT INTO tab VALUES (123.456);");
            ResultSet a = sa.executeQuery("SELECT 'klm' || abc || 'xyz' from tab");
            a.next();
            assertNull(a.getString(1));
            a.next();
            assertEquals(a.getString(1), "klm123.0xyz");
            a.next();
            assertEquals(a.getString(1), "klm123.456xyz");
        }
    }

// -------------------------- INNER CLASSES --------------------------

    @SuppressWarnings("unused")
    public static class DoubleToDot implements UdfArgumentConverter {
        public Value convertTo(Value value, int fromType, int toType) {
            if (toType != Value.STRING) {
                return null;
            } else if (value == ValueNull.INSTANCE) {
                return ValueString.get(".");
            } else if (value.getType() == Value.DOUBLE) {
                double d = value.getDouble();
                long n = (long) d;
                return ValueString.get(n == d ? Long.toString(n) : Double.toString(d));
            } else {
                return ValueString.get(value.getString());
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
