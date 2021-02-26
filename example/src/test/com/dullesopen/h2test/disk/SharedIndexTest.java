package com.dullesopen.h2test.disk;


import com.dullesopen.h2.external.DiskSchemaFactory;
import org.h2.store.fs.FileUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.MessageFormat;


public class SharedIndexTest {
// ------------------------------ FIELDS ------------------------------

    static final String DIR = "target/external";
    static final String URL = "jdbc:h2:mem:";

// -------------------------- OTHER METHODS --------------------------

    @BeforeMethod
    public void setUpConnection() throws Exception {
        Class.forName("org.h2.Driver");
        File dir = new File(DIR);
        if (dir.exists())
            FileUtils.deleteRecursive(DIR, false);
        dir.mkdirs();

        Connection connection = DriverManager.getConnection(URL);
        connection.setAutoCommit(false);
        final java.sql.Statement statement = connection.createStatement();
        schemas(statement, true);
        statement.execute("CREATE TABLE mylib.one (X INTEGER)");

        statement.execute("INSERT INTO mylib.one (X) VALUES (1)");
        statement.execute("CREATE INDEX I ON MYLIB.ONE(X)");
        statement.close();
        connection.close();
    }

    private void schemas(Statement statement, boolean append) throws SQLException {
        String schema = MessageFormat.format("CREATE SCHEMA MYLIB EXTERNAL (''{0}'',''dir={1};append={2}'')",
                DiskSchemaFactory.class.getName(), DIR, append);
        statement.execute(schema);
    }

    @AfterMethod
    public void tearDownConnection() throws Exception {
        FileUtils.deleteRecursive(DIR, false);
    }

    @Test
    public void connectTwice() throws Exception {
        Connection c1 = DriverManager.getConnection(URL);
        Connection c2 = DriverManager.getConnection(URL);
        {
            Statement statement = c1.createStatement();
            schemas(statement, true);
            statement.executeQuery("SELECT * from  mylib.one ");
            statement.close();
        }
        {
            Statement statement = c2.createStatement();
            schemas(statement, true);
            statement.executeQuery("SELECT * from  mylib.one ");
            statement.close();
        }
        c1.close();
        c2.close();
    }
}