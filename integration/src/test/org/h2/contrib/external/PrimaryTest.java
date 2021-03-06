package org.h2.contrib.external;

import org.h2.api.ErrorCode;
import org.h2.contrib.test.Utils;
import org.h2.store.fs.FileUtils;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.sql.*;

public class PrimaryTest {
// ------------------------------ FIELDS ------------------------------

    public static final String DIR = "target/primary";

    private Connection h2;

// -------------------------- TEST METHODS --------------------------

    @BeforeMethod
    protected void setUp() throws Exception {
        FileUtils.deleteRecursive(DIR, false);
        new File(DIR).mkdirs();
        Class.forName("org.h2.Driver");
        h2 = DriverManager.getConnection("jdbc:h2:mem:");
    }

    @AfterMethod
    protected void tearDown() throws Exception {
        h2.close();
        FileUtils.deleteRecursive(DIR, false);
        Assert.assertFalse(new File(DIR).exists());
    }

// -------------------------- OTHER METHODS --------------------------

    /**
     * Insert value into table without primary key.
     * Afterwards create primary key on already created data.
     * we need to update the primary key.
     *
     * @throws SQLException
     */


    @Test
    public void alter() throws SQLException {
        Statement stat;
        stat = h2.createStatement();
        stat.execute(Init.schema(DIR));

        stat.execute("CREATE TABLE S.T (ID INT NOT NULL, X REAL NOT NULL, Y REAL)");
        stat.execute("INSERT INTO S.T VALUES( 10, 11., 12. )");
        stat.execute("INSERT INTO S.T VALUES( 20, 21., 22. )");
        stat.execute("INSERT INTO S.T VALUES( 30, 31., 32. )");

        stat.execute("ALTER TABLE S.T ADD CONSTRAINT PK PRIMARY KEY (ID, X);");
        {
            ResultSet rs = stat.executeQuery("select Y from s.t where id=20");
            rs.next();
            Assert.assertEquals(22., rs.getDouble(1));
        }
        {
            ResultSet rs = stat.executeQuery("select Y from s.t where id=20 and X=21");
            rs.next();
            Assert.assertEquals(22., rs.getDouble(1));
        }
    }

    @Test
    public void duplicate() throws SQLException {
        Statement stat;
        stat = h2.createStatement();
        stat.execute(Init.schema(DIR));

        stat.execute("CREATE TABLE S.T (ID INT NOT NULL, X REAL NOT NULL, Y REAL)");
        stat.execute("INSERT INTO S.T VALUES( 10, 11., 12. )");
        stat.execute("INSERT INTO S.T VALUES( 10, 11., 22. )");

        try {
            stat.execute("ALTER TABLE S.T ADD CONSTRAINT PK PRIMARY KEY (ID, X);");
            Assert.fail();
        } catch (SQLException e) {
            int code = ErrorCode.DUPLICATE_KEY_1;
            Assert.assertEquals(e.getErrorCode(), code);
            Assert.assertEquals(Utils.truncate(e), "Unique index or primary key violation: \"T$PRIMARY_KEY_5 ON S.T(ID, X) VALUES (10, 11.0, 1)\"");
        }
    }

    /**
     * Insert value into table with primary key already created, so on each insert
     * we need to update the primary key.
     *
     * @throws SQLException
     */

    @Test
    public void insert() throws SQLException {
        Statement stat;
        stat = h2.createStatement();
        stat.execute(Init.schema(DIR));

        stat.execute("CREATE TABLE S.T (ID INT , X REAL, Y REAL, CONSTRAINT PK PRIMARY KEY(ID, X) )");
        stat.execute("INSERT INTO S.T VALUES( 10, 11., 12. )");
        stat.execute("INSERT INTO S.T VALUES( 20, 21., 22. )");
        stat.execute("INSERT INTO S.T VALUES( 30, 31., 32. )");

        {
            ResultSet rs = stat.executeQuery("select Y from s.t where id=20");
            rs.next();
            Assert.assertEquals(22., rs.getDouble(1));
        }
        {
            ResultSet rs = stat.executeQuery("select Y from s.t where id=20 and X=21");
            rs.next();
            Assert.assertEquals(22., rs.getDouble(1));
        }
    }

    @Test
    public void foreignKey() throws SQLException {
        {
            Statement stat;
            stat = h2.createStatement();
            stat.execute(Init.schema(DIR));

            stat.execute("CREATE TABLE S.T (name varchar(12) , X REAL, Y REAL, CONSTRAINT PK PRIMARY KEY(name) )");
            h2.commit();
            h2.close();
        }
        {
            Statement stat;
            h2 = DriverManager.getConnection("jdbc:h2:mem:");
            stat = h2.createStatement();
            stat.execute(Init.schema(DIR));
            stat.execute("CREATE TABLE S.R (ID INT , name varchar(12), constraint c2 foreign key(name) references S.T)");
        }
    }
}
