package org.h2.contrib.link;

import org.h2.api.ErrorCode;
import org.h2.index.Index;
import org.h2.message.DbException;
import org.h2.table.BaseTableLinkConnection;
import org.h2.table.TableLink;

import java.sql.Connection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class TableLinkSupport {

    /**
     * List of externally provided connections
     */
    private Map<String, Connection> connections = new LinkedHashMap<>();

    /**
     * Hook to provide additional index information for Hadoop
     */
    @Deprecated
    private Map<String, LinkedIndexResolver> indexResolvers = new HashMap<>();

    private LinkedQueryExecutionReporter reporter;

    public TableLinkColumnHandlerFactory tableLinkColumnHandlerFactory;

    public BaseTableLinkConnection getConnection(String connectionName) {
        final Connection c = connections.get(connectionName);
        if (c == null)
            throw DbException.get(ErrorCode.EXTERNAL_CONNECTION_NOT_FOUND, connectionName);
        return new BaseTableLinkConnection() {
            @Override
            public void close(boolean force) {
                //noop
            }

            @Override
            public Connection getConnection() {
                return c;
            }
        };
    }

    public void addLinkedConnection(String connectionName, Connection connection) {
        connections.put(connectionName, connection);
    }

    public Connection getLinkedConnection(String connectionName) {
        return connections.get(connectionName);
    }

    public void removeLinkedConnection(String connectionName) {
        connections.remove(connectionName);
    }

    public void addLinkedIndexResolver(String connectionName, LinkedIndexResolver indexResolver) {
        indexResolvers.put(connectionName, indexResolver);
    }

    public void reportLinkedQueryExecution(LinkedQueryExecutionReporter.Action action,
                                           String schema,
                                           String sql,
                                           Connection connection) {
        if (reporter != null)
            reporter.report(action, schema, sql, connection);
    }

    public void setLinkedQueryExecutionReporter(LinkedQueryExecutionReporter linkedQueryExecutionReporter) {
        this.reporter = linkedQueryExecutionReporter;
    }

    public List<Index> getIndexes(String connectionName, TableLink tableLink) {
        if (connectionName != null) {
            LinkedIndexResolver indexResolver = indexResolvers.get(connectionName);
            if (indexResolver != null) {
                return indexResolver.getIndexes(tableLink);
            }
        }
        return null;
    }
}
