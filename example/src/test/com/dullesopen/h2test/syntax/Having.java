package com.dullesopen.h2test.syntax;

import com.dullesopen.h2test.Utils;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 */
public class Having {

    /**
     * this test demonstrates that we can not use alias in having statement
     *
     * @throws Exception
     */

    @Test
    public void alias() throws Exception {
        Class.forName("org.h2.Driver");
        Connection ca = DriverManager.getConnection("jdbc:h2:mem:having");

        Statement sa = ca.createStatement();
        sa.execute("CREATE TABLE A(B FLOAT, C FLOAT)");
        try {
            sa.execute("SELECT B+C AS D FROM A HAVING D>0");
            Assert.fail();
        } catch (SQLException e) {
            // H2 does not support using alias in having clause
            Assert.assertEquals(e.getErrorCode(), 90016);
            Assert.assertEquals(Utils.truncate(e), "Column \"B\" must be in the GROUP BY list");
        }

    }
}

