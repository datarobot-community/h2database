/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import java.util.ArrayList;
import org.h2.api.ErrorCode;
import org.h2.command.CommandInterface;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.engine.User;
import org.h2.message.DbException;
import org.h2.schema.Schema;
import org.h2.schema.SchemaFactory;
import org.h2.schema.SchemaLink;

/**
 * This class represents the statement
 * CREATE SCHEMA
 */
public class CreateSchema extends DefineCommand {

    private String schemaName;
    private String authorization;
    private boolean ifNotExists;
    private ArrayList<String> tableEngineParams;
    private String external;
    private String externalConnectionName;
    private boolean linked;
    private String original;
    private String externalParameters;

    public CreateSchema(Session session, boolean force) {
        super(session);
    }

    public void setIfNotExists(boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }

    @Override
    public int update() {
        session.getUser().checkSchemaAdmin();
        session.commit(true);
        Database db = session.getDatabase();
        User user = db.getUser(authorization);
        // during DB startup, the Right/Role records have not yet been loaded
        if (!db.isStarting()) {
            user.checkSchemaAdmin();
        }
        if (db.findSchema(schemaName) != null) {
            if (ifNotExists) {
                return 0;
            }
            throw DbException.get(ErrorCode.SCHEMA_ALREADY_EXISTS_1, schemaName);
        }
        int id = getObjectId();
        Schema schema;
        if (external != null) {
            // when bootstrap database it is ok to fail to recreate external schema, in such case the schema will be
            // silently dropped and all objects from this schema will not be available
            // also will need to drop all derived objects
            schema = createExternalSchema(db, id, user);
        } else if (linked) {
            schema = new SchemaLink(db, id, schemaName, user, externalConnectionName, original);
        } else
            schema = new Schema(db, id, schemaName, user, false);
        schema.setTableEngineParams(tableEngineParams);
        db.addDatabaseObject(session, schema);
        return 0;
    }

    public void setSchemaName(String name) {
        this.schemaName = name;
    }

    public void setAuthorization(String userName) {
        this.authorization = userName;
    }

    public void setTableEngineParams(ArrayList<String> tableEngineParams) {
        this.tableEngineParams = tableEngineParams;
    }

    @Override
    public int getType() {
        return CommandInterface.CREATE_SCHEMA;
    }

    private Schema createExternalSchema(Database db, int id, User user) {
        SchemaFactory sf;
        try {
            sf = (SchemaFactory) Class.forName(external).newInstance();
        } catch (ClassNotFoundException e) {
            throw DbException.convert(e);
        } catch (IllegalAccessException e) {
            throw DbException.convert(e);
        } catch (InstantiationException e) {
            throw DbException.convert(e);
        }
        return sf.create(db, id, schemaName, user, externalParameters);
    }

    public void setExternal(String external) {
        this.external = external;
    }

    public void setLinked(String externalConnectionName, String original) {
        this.original = original;

        linked = true;
        this.externalConnectionName = externalConnectionName;
    }

    public void setExternalParameters(String externalParameters) {
        this.externalParameters = externalParameters;

    }

}
