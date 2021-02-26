/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: PG
 */

package org.h2.schema;

import org.h2.api.ErrorCode;
import org.h2.command.ddl.CreateTableData;
import org.h2.engine.Database;
import org.h2.engine.DbObject;
import org.h2.engine.Session;
import org.h2.engine.User;
import org.h2.message.DbException;
import org.h2.table.Table;
import org.h2.table.TableLink;
import org.h2.util.JdbcUtils;
import org.h2.util.StringUtils;

import java.sql.*;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;

public class SchemaLink extends Schema {

    private final String externalConnectionName;
    private final String original;

    private Connection connection;

    protected final Map<String, Table> linked = new HashMap<String, Table>();

    public SchemaLink(Database database, int id, String schemaName, User owner,
                      String externalConnectionName, String original) {
        super(database, id, schemaName, owner, false);
        this.externalConnectionName = externalConnectionName;
        this.original = StringUtils.isNullOrEmpty(original) ? null : original;

        connection = database.getExternalConnection(externalConnectionName);

    }

    public String getCreateSQL() {
        StringBuffer buff = new StringBuffer();
        buff.append("CREATE FORCE SCHEMA ");
        buff.append(getSQL());
        buff.append(" AUTHORIZATION ");
        buff.append(getOwner().getSQL());
        buff.append(" LINKED ");
        buff.append("(");
        buff.append("'").append(externalConnectionName).append("',");
        buff.append("'").append(original == null ? "" : original).append("')");
        return buff.toString();
    }

    public Table findTableOrView(Session session, String name) {
        if (connection == null) {
            throw DbException.get(ErrorCode.EXTERNAL_CONNECTION_NOT_FOUND, externalConnectionName);
        } else {
            ResultSet rs = null;
            try {
                DatabaseMetaData metaData = connection.getMetaData();
                rs = metaData.getTables(connection.getCatalog(), original, name, null);
                while (rs.next()) {
                    Table table = getTableOrView(session, name);
                    return table;
                }
                return null;
            } catch (SQLException e) {
                throw DbException.convert(e);
            } finally {
                JdbcUtils.closeSilently(rs);
            }
        }
    }

    public Table getTableOrView(Session session, String name) {

        if (connection == null) {
            throw DbException.get(ErrorCode.EXTERNAL_CONNECTION_NOT_FOUND, externalConnectionName);
        } else {
            Table table = tablesAndViews.get(name);
            if (table == null && session != null) {
                table = session.findLocalTempTable(name);
            }
            if (table == null) {
                table = linked.get(name);
            }
            if (table == null) {
                int id = session.getDatabase().allocateObjectId();

                table = new TableLink(this, id, name, null, null, null, null, externalConnectionName, original, name,
                        false, false, false, database.columnExtensionFactory);
                linked.put(name, table);

            }
            return table;
        }
    }

    public void remove(Session session, SchemaObject obj) {

        switch (obj.getType()) {
            case DbObject.TABLE_OR_VIEW:
                if (connection != null) {
                    Statement stat = null;
                    try {
                        stat = connection.createStatement();
                        String sql = MessageFormat.format("DROP {0} {1}{2}",
                                ((TableLink) obj).isView() ? "VIEW" : "TABLE", original == null ? "" : original + ".", obj.getName());
                        stat.execute(sql);
                    } catch (SQLException e) {
                        throw DbException.convert(e);
                    } finally {
                        JdbcUtils.closeSilently(stat);
                    }
                }
                break;
            // TODO throw proper exception here for unsupported operations
            default:
                throw DbException.throwInternalError("unsupported delete type=" + obj.getType());
        }

    }


    public Table createTable(CreateTableData data) {
        throw new UnsupportedOperationException("Linked schemas do not allow to create table;");
    }

    public void close(Session session) {
        super.close(session);
    }

    @Override
    public void flush(Session session) {
        super.flush(session);
        for (Table table : linked.values()) {
            table.close(session);
        }
        linked.clear();
    }

    @Override
    public void removeChildrenAndResources(Session session) {
        flush(session);
        super.removeChildrenAndResources(session);
    }
}
