package org.h2.contrib.conversion;

import org.h2.contrib.UserDefinedConversion;
import org.h2.store.fs.FileUtils;
import org.h2.value.Value;
import org.h2.value.ValueNull;
import org.h2.value.ValueString;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.MessageFormat;

public class ConcatTest {
// ------------------------------ FIELDS ------------------------------

    private Connection h2;

// -------------------------- TEST METHODS --------------------------

    @BeforeMethod
    protected void setUp() throws Exception {
        Class.forName("org.h2.Driver");
        FileUtils.deleteRecursive("./target/db", false);
        h2 = DriverManager.getConnection("jdbc:h2:./target/db/concat;MODE=Carolina");

        Statement stat = h2.createStatement();
        stat.execute("CREATE TABLE one ( abc double, def double)");
        stat.execute("INSERT INTO one(abc,def) VALUES(10.,20.)");
    }

    @AfterMethod
    protected void tearDown() throws Exception {
        h2.close();
    }

// -------------------------- OTHER METHODS --------------------------

    /**
     * No user defined conversion for concat operation
     */
    @Test
    public void dot() throws Exception {
        Statement stat = h2.createStatement();
        String sql = MessageFormat.format(
                "CREATE CONVERSION DOUBLE , CHARACTER VARYING,  ''{0}$Best'';"
                , ConcatTest.class.getName());
        stat.execute(sql);
        ResultSet rs = stat.executeQuery("SELECT concat(abc,'/',def) cat FROM one");
        int prec = rs.getMetaData().getPrecision(1);
        rs.next();
        Assert.assertEquals(rs.getString("cat"), "10/20");
        Assert.assertEquals(prec, 200);
    }

    public static class Best implements UserDefinedConversion {
        public Value convertTo(Value from, int to) {
            if (to != Value.STRING) {
                throw new IllegalArgumentException("unexpected type");
            } else if (from == ValueNull.INSTANCE) {
                return ValueString.get(".");
            } else if (from.getType() == Value.DOUBLE) {
                double d = from.getDouble();
                if (d == (long) d)
                    return ValueString.get(Long.toString((long) d));
                else
                    return ValueString.get(Double.toString(d));
            } else {
                return ValueString.get(from.getString());
            }
        }
    }

}
