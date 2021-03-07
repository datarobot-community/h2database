/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: PG
 */
package org.h2.command.ddl;

import org.h2.api.ErrorCode;
import org.h2.command.CommandInterface;
import org.h2.engine.Session;
import org.h2.engine.Database;
import org.h2.contrib.UserDefinedConversion;
import org.h2.message.DbException;

public class CreateConversion extends DefineCommand {

    private String javaClassMethod;
    private int from;
    private int to;

    public CreateConversion(Session session) {
        super(session);
    }

    public int update() {
        session.commit(true);
        session.getUser().checkAdmin();
        Database db = session.getDatabase();
        Class javaClass;
        UserDefinedConversion convert;
        try {
            javaClass = Class.forName(javaClassMethod);
            convert = (UserDefinedConversion) javaClass.newInstance();
        } catch (IllegalAccessException | InstantiationException | ClassCastException | ClassNotFoundException e) {
            throw DbException.get(ErrorCode.CLASS_NOT_FOUND_1, e, javaClassMethod);
        }
        db.addConversion(from, to, convert);
        return 0;
    }

    public void setJavaClass(String string) {
        javaClassMethod = string;

    }

    public void setFromDataType(int type) {
        from = type;
    }

    public void setToDataType(int type) {
        to = type;
    }

    @Override
    public int getType() {
        return CommandInterface.CONVERSION;
    }


}
