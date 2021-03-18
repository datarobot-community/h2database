package org.h2.mode;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.testng.Assert.assertEquals;

/**
 * Mode.decimalConstantAsDouble
 */

public class DecimalConstantAsDoubleTest {

    @BeforeClass
    protected void setUp() throws Exception {
        Class.forName("org.h2.Driver");
    }

    @Test
    public void decimal() throws Exception {
        try (Connection h2 = DriverManager.getConnection("jdbc:h2:mem:;MODE=Carolina")) {
            try (Statement s1 = h2.createStatement()) {
                s1.execute("CREATE TABLE FOO (I INTEGER)");
                s1.execute("INSERT INTO FOO VALUES(7)");

                s1.execute("CREATE TABLE BAR AS SELECT I/3.25 FROM FOO");

                try (ResultSet rs = s1.executeQuery("SELECT * FROM BAR")) {
                    rs.next();
                    assertEquals(2.1538461538461537, rs.getDouble(1));
                }
            }
        }
    }
}
