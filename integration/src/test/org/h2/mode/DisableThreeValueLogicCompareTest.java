package org.h2.mode;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;


/**
 * Mode.disableThreeValueLogic
 */

public class DisableThreeValueLogicCompareTest {

    @BeforeClass
    protected void setUp() throws Exception {
        Class.forName("org.h2.Driver");
    }

    @Test
    public void compare() throws Exception {
        try (Connection h2 = DriverManager.getConnection("jdbc:h2:mem:;mode=Carolina")) {
            try (Statement sa = h2.createStatement()) {

                sa.execute("CREATE TABLE A(X DOUBLE, Y DOUBLE)");
                sa.execute("INSERT INTO A VALUES (NULL,1)");
                sa.execute("CREATE TABLE B(X DOUBLE, Y DOUBLE)");
                sa.execute("INSERT INTO B VALUES (NULL,NULL)");

                try (ResultSet rs = sa.executeQuery("SELECT COUNT(*) FROM A WHERE X < Y")) {
                    rs.next();
                    Assert.assertEquals(1, rs.getInt(1));
                }

                try (ResultSet rs = sa.executeQuery("SELECT COUNT(*) FROM A WHERE Y < X")) {
                    rs.next();
                    Assert.assertEquals(0, rs.getInt(1));
                }

                try (ResultSet rs = sa.executeQuery("SELECT COUNT(*) FROM A WHERE Y > X")) {
                    rs.next();
                    Assert.assertEquals(1, rs.getInt(1));
                }

                try (ResultSet rs = sa.executeQuery("SELECT COUNT(*) FROM A WHERE X > Y")) {
                    rs.next();
                    Assert.assertEquals(0, rs.getInt(1));
                }

                try (ResultSet rs = sa.executeQuery("SELECT COUNT(*) FROM A WHERE X <= Y")) {
                    rs.next();
                    Assert.assertEquals(1, rs.getInt(1));
                }

                try (ResultSet rs = sa.executeQuery("SELECT COUNT(*) FROM A WHERE Y >= X")) {
                    rs.next();
                    Assert.assertEquals(1, rs.getInt(1));
                }

                try (ResultSet rs = sa.executeQuery("SELECT COUNT(*) FROM A WHERE X <> Y")) {
                    rs.next();
                    Assert.assertEquals(1, rs.getInt(1));
                }

                try (ResultSet rs = sa.executeQuery("SELECT COUNT(*) FROM A WHERE X = Y")) {
                    rs.next();
                    Assert.assertEquals(0, rs.getInt(1));
                }

                try (ResultSet rs = sa.executeQuery("SELECT COUNT(*) FROM B WHERE X < Y")) {
                    rs.next();
                    Assert.assertEquals(0, rs.getInt(1));
                }

                try (ResultSet rs = sa.executeQuery("SELECT COUNT(*) FROM B WHERE Y < X")) {
                    rs.next();
                    Assert.assertEquals(0, rs.getInt(1));
                }

                try (ResultSet rs = sa.executeQuery("SELECT COUNT(*) FROM B WHERE Y > X")) {
                    rs.next();
                    Assert.assertEquals(0, rs.getInt(1));
                }

                try (ResultSet rs = sa.executeQuery("SELECT COUNT(*) FROM B WHERE X > Y")) {
                    rs.next();
                    Assert.assertEquals(0, rs.getInt(1));
                }

                try (ResultSet rs = sa.executeQuery("SELECT COUNT(*) FROM B WHERE X <= Y")) {
                    rs.next();
                    Assert.assertEquals(1, rs.getInt(1));
                }

                try (ResultSet rs = sa.executeQuery("SELECT COUNT(*) FROM B WHERE Y >= X")) {
                    rs.next();
                    Assert.assertEquals(1, rs.getInt(1));
                }

                try (ResultSet rs = sa.executeQuery("SELECT COUNT(*) FROM B WHERE X <> Y")) {
                    rs.next();
                    Assert.assertEquals(0, rs.getInt(1));
                }

                try (ResultSet rs = sa.executeQuery("SELECT COUNT(*) FROM B WHERE X = Y")) {
                    rs.next();
                    Assert.assertEquals(1, rs.getInt(1));
                }
            }
        }   }
}