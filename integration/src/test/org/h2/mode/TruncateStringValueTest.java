package org.h2.mode;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.testng.Assert.assertEquals;

/**
 * Mode.truncateStringValue
 */

public class TruncateStringValueTest {

    @BeforeClass
    protected void setUp() throws Exception {
        Class.forName("org.h2.Driver");
    }

    @Test
    public void truncate() throws Exception {

        try (Connection h2 = DriverManager.getConnection("jdbc:h2:mem:;mode=Carolina")) {
            try (Statement s1 = h2.createStatement()) {
                s1.execute("CREATE TABLE MYTAB (S VARCHAR(5))");
                s1.execute("INSERT INTO MYTAB VALUES('abcdefg')");

                try (ResultSet rs = s1.executeQuery("SELECT S FROM MYTAB")) {
                    rs.next();
                    assertEquals("abcde", rs.getString("S"));
                }
            }
        }
    }
}
