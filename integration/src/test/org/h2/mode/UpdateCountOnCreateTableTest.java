package org.h2.mode;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

/**
 * Mode.updateCountOnCreateTable
 */

public class UpdateCountOnCreateTableTest {

    @BeforeClass
    protected void setUp() throws Exception {
        Class.forName("org.h2.Driver");
    }

    @Test
    public void create() throws Exception {
        try (Connection h2 = DriverManager.getConnection("jdbc:h2:mem:;MODE=Carolina")) {
            try (Statement stat = h2.createStatement()) {

                stat.execute("CREATE TABLE FOO (A INTEGER)");
                stat.execute("INSERT INTO FOO VALUES(10)");
                stat.execute("INSERT INTO FOO VALUES(20)");
                stat.execute("INSERT INTO FOO VALUES(30)");

                stat.execute("CREATE TABLE BAR AS SELECT * FROM FOO");
                Assert.assertEquals(stat.getUpdateCount(), 3);
            }
        }
    }
}
